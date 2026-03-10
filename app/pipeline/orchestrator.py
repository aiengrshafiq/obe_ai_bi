import pandas as pd
import numpy as np
import math
import re
import time
import traceback
import asyncio
from datetime import datetime
from typing import Dict, Any, List
import sqlglot

# Import Agents & Infrastructure
from app.pipeline.agents.intent import IntentAgent
from app.pipeline.agents.sql import SQLAgent
from app.pipeline.agents.visualization import VisualizationAgent
from app.pipeline.guardrails.sql_policy import SQLGuard, SQLPolicyException
from app.services.vanna_wrapper import vn
from app.db.app_models import ChatLog, SessionLocal
from app.services.date_resolver import DateResolver
from app.pipeline.prompts.sql_prompt import get_sql_system_prompt
from app.pipeline.agents.context_resolver import ContextResolver
from app.pipeline.agents.suggestion import SuggestionAgent


class Orchestrator:
    """
    The Master Controller.
    Flow: Context -> Intent -> SQL Gen -> Safety Check -> Execution -> Visualization
    """
    
    def __init__(self, user: str):
        self.user = user
        self.db = SessionLocal() 

    def _json_safe(self, obj):
        """Global safety net."""
        if obj is None or obj is pd.NaT: return None
        if isinstance(obj, (pd.Timestamp, datetime)): return obj.isoformat()
        if isinstance(obj, np.generic): return self._json_safe(obj.item())
        if isinstance(obj, float): return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, (int, bool, str)): return obj
        if isinstance(obj, dict): return {str(k): self._json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)): return [self._json_safe(v) for v in obj]
        return str(obj)

    # FIX: Updated signature to accept List (raw_history) instead of String
    async def run_pipeline(self, user_msg: str, raw_history: List[Dict[str, str]]) -> Dict[str, Any]:
        """Executes the full Intelligence Pipeline with Self-Correction."""
        start_time = time.time()
        
        # ⚠️ FIX 1: Move the master try/finally to the VERY TOP to guarantee DB closure
        try:
            # --- STEP 0: CONTEXT RESOLUTION (Structured & Safe) ---
            final_msg = user_msg
            context_metadata = {}

            if raw_history:
                # ⚠️ FIX 5: Protect ContextResolver from crashing the pipeline
                try:
                    resolution = await asyncio.to_thread(ContextResolver.resolve, user_msg, raw_history)
                    if resolution.get("confidence", 0) > 0.6:
                        final_msg = resolution["rewritten_query"]
                        context_metadata = resolution
                        print(f"🔄 Context Rewritten: '{user_msg}' -> '{final_msg}'")
                    else:
                        print(f"⚠️ Context skipped (Low Confidence): Using original message.")
                except Exception as e:
                    print(f"⚠️ ContextResolver Error: {e}. Falling back to original message.")
            # -----------------------------------------------

            # 1. Build Legacy String Context
            history_context = ""
            if raw_history:
                 recent = raw_history[-4:]
                 history_context = "PREVIOUS CONVERSATION:\n" + "\n".join(
                     [f"{msg.get('role', 'unknown').upper()}: {str(msg.get('content',''))[:200]}" for msg in recent]
                 )

            # Initialize Log Entry
            log_entry = ChatLog(
                username=self.user, 
                user_question=final_msg, 
                context_provided=history_context,
                correction_attempts=0
            )
            self.db.add(log_entry)
            self.db.commit()

            # --- STEP 1: INTENT & SHORT CONFIRMATIONS ---
            # ⚠️ FIX 3: Prevent "Yes/Ok" from triggering the general greeting if mid-conversation
            short_confirmations = {"yes", "ok", "sure", "do it", "go ahead", "yep", "yeah", "continue"}
            is_follow_up = raw_history and user_msg.strip().lower() in short_confirmations

            intent_result = await IntentAgent.classify(final_msg)
            
            if not is_follow_up:
                if intent_result.get("intent_type") == "general_chat":
                    return self._finalize_safe(log_entry, "text", message="Hello! I am your Data Analyst. Ask me about Users, Volume, Deposits, or Risk.")
                
                if intent_result.get("intent_type") == "ambiguous":
                    clarification = intent_result.get("clarification_question") or "Could you please clarify which metrics or dates you are interested in?"
                    return self._finalize_safe(log_entry, "text", message=clarification)

            # --- STEP 2: PREPARE CONTEXT (DateResolver) ---
            date_context = await DateResolver.get_date_context()
            log_entry.resolved_latest_ds = date_context['latest_ds']

            # --- STEP 3: REASONING LOOP (Self-Healing) ---
            max_retries = 2
            attempts = 0
            last_error = None
            
            # ⚠️ FIX 2: Store the original massive prompt so we don't lose it on retries
            base_prompt = self._build_prompt(final_msg, history_context, intent_result, date_context)
            current_prompt = base_prompt
            
            while attempts <= max_retries:
                clean_sql = ""
                try:
                    # A. Generate SQL
                    generated_sql = await SQLAgent.generate(current_prompt)
                    
                    if "intermediate_sql" in generated_sql.lower():
                        raise ValueError("LLM tried to inspect data (intermediate_sql detected). Retrying with stricter instruction.")
                    
                    clean_sql = re.sub(r'```sql|```', '', generated_sql, flags=re.IGNORECASE).strip()
                    clean_sql = clean_sql.replace("CLARIFICATION:", "").strip()

                    if not re.match(r'^\s*(SELECT|WITH)\b', clean_sql, re.IGNORECASE):
                        return self._finalize_safe(log_entry, "text", message=clean_sql)

                    # B. Safety Gate
                    safe_sql = SQLGuard.validate_and_fix(clean_sql)
                    
                    # Extract Tables
                    tables = re.findall(r'(?:FROM|JOIN)\s+(?:public\.)?([a-zA-Z0-9_]+)', safe_sql, re.IGNORECASE)
                    log_entry.tables_used = ", ".join(set(tables))
                    
                    # C. Apply Date Replacements
                    final_sql = self._apply_replacements(safe_sql, date_context)

                    # D. Execution
                    df = await vn.run_sql_async(final_sql)
                    
                    log_entry.generated_sql = final_sql
                    log_entry.correction_attempts = attempts
                    self.db.commit()

                    if df is None or df.empty:
                        log_entry.execution_ms = int((time.time() - start_time) * 1000)
                        return self._finalize_safe(log_entry, "success", sql=final_sql, message="No data found.", data=[])

                    # E. Visualization & Suggestions
                    df = self._sanitize_dataframe(df)
                    log_entry.row_count = len(df)
                    
                    viz_result = await VisualizationAgent.determine_format(df, final_sql, final_msg, intent_result)
                    visual_type = viz_result.get("visual_type", "table")
                    log_entry.visual_type = visual_type

                    #suggestions = SuggestionAgent.generate(df, final_msg)
                    suggestions = [] # Deprecated in favor of AST Explore Bar
                    
                    df_safe = df.where(pd.notnull(df), None)
                    safe_data = df_safe.head(5000 if visual_type == "plotly" else 100).to_dict(orient='records')

                    log_entry.execution_ms = int((time.time() - start_time) * 1000)

                    result = self._finalize(
                        log_entry, "success", sql=final_sql, data=safe_data,
                        visual_type=visual_type, plotly_code=viz_result.get("plotly_code"), 
                        suggestions=suggestions,
                        thought=f"Pipeline: Intent={intent_result.get('intent_type')} -> SQL -> {viz_result.get('thought')}"
                    )
                    return self._json_safe(result)

                except SQLPolicyException as pe:
                    return self._finalize_safe(log_entry, "error", message=f"Security Block: {str(pe)}")
                
                except Exception as e:
                    last_error = str(e)
                    attempts += 1
                    print(f"⚠️ SQL Fail (Attempt {attempts}): {last_error}")
                    
                    if attempts <= max_retries:
                        sql_context = clean_sql if clean_sql else "NO_SQL_GENERATED"
                        
                        # ⚠️ FIX 2: Append the error to the BASE prompt, maintaining all system rules
                        current_prompt = (
                            base_prompt + 
                            f"\n\n======================================\n"
                            f"⚠️ PREVIOUS ATTEMPT FAILED\n"
                            f"You must correct the SQL based on the error below while strictly adhering to all rules above.\n\n"
                            f"FAILED SQL:\n{sql_context}\n\n"
                            f"DATABASE ERROR:\n{last_error}\n"
                            f"======================================\n"
                            f"TASK: Output ONLY the corrected SQL."
                        )
                    else:
                        break # Exit loop

            # Failure after retries
            log_entry.correction_attempts = attempts
            log_entry.execution_ms = int((time.time() - start_time) * 1000)
            return self._finalize_safe(log_entry, "error", message=f"I tried to run the query, but it kept failing: {last_error}")

        except Exception as e:
            print(f"Orchestrator Fatal Error: {e}")
            traceback.print_exc()
            return self._finalize_safe(log_entry if 'log_entry' in locals() else None, "error", message="An internal system error occurred.")
        
        finally:
            # ⚠️ FIX 1: This is now guaranteed to run, closing the DB session no matter what crashes
            self.db.close()


    async def explore_action(self, log_id: int, action_type: str, key: str, agg: str = None) -> Dict[str, Any]:
        """Phase 2 & 3: Production-Grade Deterministic AST SQL Transformation."""
        import sqlglot.expressions as exp
        start_time = time.time()
        
        try:
            log_entry = self.db.query(ChatLog).filter(ChatLog.id == log_id).first()
            if not log_entry or not log_entry.generated_sql:
                return self._json_safe({"type": "error", "message": "Original query not found."})

            base_sql = log_entry.generated_sql
            
            try:
                ast = sqlglot.parse_one(base_sql, read="postgres")
                
                if action_type == "dimension":
                    existing_cols = [e.alias_or_name.lower() for e in ast.selects]
                    if key.lower() not in existing_cols:
                        ast = ast.select(key, append=True).group_by(key, append=True)
                        new_sql = ast.sql(dialect="postgres")
                    else:
                        new_sql = base_sql 

                elif action_type == "measure" and agg:
                    replaced = False
                    new_alias = f"{key}_metric"
                    old_alias_name = None
                    
                    if agg == "COUNT_DISTINCT":
                        expr_str = f"COUNT(DISTINCT {key}) AS {new_alias}"
                    else:
                        expr_str = f"{agg}({key}) AS {new_alias}"
                        
                    for i, expr in enumerate(ast.expressions):
                        if expr.find(exp.AggFunc):
                            old_alias_name = expr.alias_or_name
                            new_expr = sqlglot.parse_one(expr_str)
                            ast.expressions[i] = new_expr
                            replaced = True
                            break 
                            
                    if replaced:
                        # 4. FIXED: Safe ORDER BY Handling (ignores literals like ORDER BY 1)
                        if ast.args.get("order"):
                            for ord_expr in ast.args["order"].expressions:
                                # Safely check if it's a column/identifier, not a literal number
                                if hasattr(ord_expr.this, "name") and ord_expr.this.name and old_alias_name:
                                    if ord_expr.this.name.lower() == old_alias_name.lower():
                                        ord_expr.this.replace(exp.column(new_alias))
                        new_sql = ast.sql(dialect="postgres")
                    else:
                        new_sql = base_sql

                elif action_type == "time":
                    date_ctx = await DateResolver.get_date_context()
                    new_date_val = date_ctx.get(key)
                    
                    if not new_date_val:
                        return self._json_safe({"type": "error", "message": "Invalid time window selected."})

                    where_node = ast.args.get("where")
                    if where_node:
                        # 5. FIXED: Safely use find_all() instead of walk()
                        ds_nodes = [n for n in where_node.find_all(exp.Column) if n.name.lower() == 'ds']
                        
                        if len(ds_nodes) != 1:
                            return self._json_safe({"type": "error", "message": "Time switching not supported for complex query shapes."})
                            
                        # Exactly 1 ds node found, safe to replace
                        target_node = ds_nodes[0]
                        parent = target_node.parent
                        
                        if isinstance(parent, (exp.GTE, exp.GT, exp.EQ)):
                            if key == "latest_ds" and not isinstance(parent, exp.EQ):
                                parent.replace(exp.EQ(this=target_node, expression=exp.Literal.string(new_date_val)))
                            elif key != "latest_ds" and not isinstance(parent, exp.GTE):
                                parent.replace(exp.GTE(this=target_node, expression=exp.Literal.string(new_date_val)))
                            else:
                                parent.set("expression", exp.Literal.string(new_date_val))
                        elif isinstance(parent, exp.Between):
                            if key == "latest_ds":
                                parent.replace(exp.EQ(this=target_node, expression=exp.Literal.string(new_date_val)))
                            else:
                                parent.replace(exp.GTE(this=target_node, expression=exp.Literal.string(new_date_val)))
                        else:
                            return self._json_safe({"type": "error", "message": "Unsupported time filter operator."})
                            
                    new_sql = ast.sql(dialect="postgres")

                else:
                    return self._json_safe({"type": "error", "message": "Invalid explore action."})
                    
            except Exception as e:
                print(f"AST Parsing Error: {e}")
                return self._json_safe({"type": "error", "message": "Cannot safely transform this specific query format."})

            action_text = f"Sliced by {key}" if action_type == "dimension" else f"Swapped metric to {key}" if action_type == "measure" else f"Changed timeframe to {key}"
            new_log = ChatLog(username=self.user, user_question=action_text, generated_sql=new_sql, tables_used=log_entry.tables_used, execution_success=True)
            self.db.add(new_log)
            self.db.commit()

            df = await vn.run_sql_async(new_sql)
            if df is None or df.empty:
                return self._json_safe({"type": "success", "sql": new_sql, "message": "No data found for this view.", "data": []})

            df = self._sanitize_dataframe(df)
            dummy_msg = f"Show data broken down by {key}" if action_type == "dimension" else f"Show {key}"
            intent_result = await IntentAgent.classify(dummy_msg)
            
            viz_result = await VisualizationAgent.determine_format(df, new_sql, dummy_msg, intent_result)
            visual_type = viz_result.get("visual_type", "table")
            
            df_safe = df.where(pd.notnull(df), None)
            safe_data = df_safe.head(5000 if visual_type == "plotly" else 100).to_dict(orient='records')

            result = {
                "type": "success",
                "sql": new_sql,
                "data": safe_data,
                "visual_type": visual_type,
                "plotly_code": viz_result.get("plotly_code"),
                "thought": f"AST Deterministic Transformation: {action_text}."
            }
            
            # Pass base_sql to ensure we apply the multi-metric guardrail on the NEW query too
            explore_meta = self._get_explore_metadata(new_log.tables_used, new_sql)
            current_ast = sqlglot.parse_one(new_sql, read="postgres")
            current_cols = [e.alias_or_name.lower() for e in current_ast.selects]
            
            available_dims = [d for d in explore_meta.get("dimensions", []) if d["key"].lower() not in current_cols]
            available_measures = [m for m in explore_meta.get("measures", []) if m["key"].lower() != key.lower()]
            available_time = [tc for tc in explore_meta.get("time_controls", []) if tc["key"] != key]
            
            if available_dims or available_measures or available_time:
                result["explore"] = {
                    "log_id": new_log.id,
                    "dimensions": available_dims,
                    "measures": available_measures,
                    "time_controls": available_time
                }

            return self._json_safe(result)

        except Exception as e:
            traceback.print_exc()
            return self._json_safe({"type": "error", "message": f"Explore failed: {str(e)}"})
        finally:
            self.db.close()

            
    # --- HELPERS ---
    def _build_prompt(self, msg, history, intent, date_ctx):
        return get_sql_system_prompt(
            history=history,
            intent_type=intent.get('intent_type'),
            entities=intent.get('entities', []),
            date_ctx=date_ctx,
            user_msg=msg
        )
        

    
    def _apply_replacements(self, sql, date_ctx):
        # DYNAMIC SAFETY NET:
        # If the LLM mimics a training example and outputs a literal string 
        # like "{start_last_month_dash}", this safely replaces it with the real date.
        for key, value in date_ctx.items():
            sql = sql.replace(f"{{{key}}}", str(value))
        return sql
        
    def _sanitize_dataframe(self, df):
        try: df.replace([np.inf, -np.inf], np.nan, inplace=True)
        except: pass
        return df

    def _get_explore_metadata(self, tables_string: str, base_sql: str = None) -> dict:
        """Fetches safe dimensions, measures, and time pills for the UI."""
        import sqlglot
        import sqlglot.expressions as exp
        
        # 1. FIXED: Use CubeRegistry instead of hardcoded dictionary
        try:
            from app.core.cube_registry import CubeRegistry
            has_registry = True
        except ImportError:
            has_registry = False

        if not tables_string:
            return {"dimensions": [], "measures": [], "time_controls": []}

        tables = [t.strip() for t in tables_string.split(",")]
        all_dims = set()
        all_measures = set()

        cube_dims = {
            "user_profile_360": [{"key": "country", "label": "Country"}, {"key": "user_segment", "label": "User Segment"}, {"key": "kyc_status_desc", "label": "KYC Status"}, {"key": "lifecycle_stage", "label": "Lifecycle Stage"}, {"key": "trading_profile", "label": "Trading Profile"}],
            "dws_user_deposit_withdraw_detail_di": [{"key": "type", "label": "Transaction Type"}, {"key": "coin", "label": "Coin/Token"}, {"key": "chain", "label": "Network Chain"}, {"key": "audit_status_desc", "label": "Audit Status"}],
            "dws_all_trades_di": [{"key": "market_type", "label": "Market (Spot/Futures)"}, {"key": "order_side_desc", "label": "Order Side (Buy/Sell)"}, {"key": "margin_mode_desc", "label": "Margin Mode"}, {"key": "alias", "label": "Trading Pair"}],
            "risk_campaign_blacklist": [{"key": "reason", "label": "Ban Reason"}, {"key": "owner", "label": "Blocked By"}],
            "ads_total_root_referral_volume_di": [{"key": "root_country", "label": "Partner Country"}, {"key": "root_user_type", "label": "Partner Type"}],
            "ads_total_root_referral_volume_df": [{"key": "root_country", "label": "Partner Country"}, {"key": "root_user_type", "label": "Partner Type"}],
            "dwd_activity_t_points_user_task_di": [{"key": "rule_code", "label": "Activity Type"}, {"key": "state", "label": "Completion State"}],
            "dwd_user_device_log_di": [{"key": "operation", "label": "Operation Type"}, {"key": "os_name", "label": "Operating System"}, {"key": "browser_name", "label": "Browser"}, {"key": "country", "label": "Device Country"}],
            "dwd_login_history_log_di": [{"key": "type", "label": "Event Type"}, {"key": "business", "label": "Business Unit"}, {"key": "os", "label": "Operating System"}]
        }

        cube_measures = {
            "user_profile_360": [{"key": "user_code", "label": "Total Users", "agg": "COUNT"}, {"key": "total_trade_volume", "label": "Lifetime Trade Volume", "agg": "SUM"}, {"key": "total_deposit_volume", "label": "Lifetime Deposit Volume", "agg": "SUM"}, {"key": "total_net_fees", "label": "Lifetime Net Fees", "agg": "SUM"}, {"key": "total_available_balance", "label": "Total Available Balance", "agg": "SUM"}],
            "dws_user_deposit_withdraw_detail_di": [{"key": "user_code", "label": "Unique Users", "agg": "COUNT_DISTINCT"}, {"key": "real_amount", "label": "Total Amount", "agg": "SUM"}, {"key": "fee_amount", "label": "Total Fees", "agg": "SUM"}],
            "dws_all_trades_di": [{"key": "user_code", "label": "Unique Traders", "agg": "COUNT_DISTINCT"}, {"key": "deal_amount", "label": "Trading Volume", "agg": "SUM"}, {"key": "net_fee", "label": "Trading Fees", "agg": "SUM"}, {"key": "order_id", "label": "Trade Count", "agg": "COUNT"}],
            "ads_total_root_referral_volume_di": [{"key": "daily_referrals", "label": "New Referrals", "agg": "SUM"}, {"key": "daily_referral_volume", "label": "Referral Volume", "agg": "SUM"}, {"key": "daily_deposit_amount", "label": "Referral Deposits", "agg": "SUM"}, {"key": "daily_referral_pnl", "label": "Referral PnL", "agg": "SUM"}],
            "ads_total_root_referral_volume_df": [{"key": "total_referrals", "label": "Lifetime Referrals", "agg": "SUM"}, {"key": "total_referral_volume", "label": "Lifetime Referral Volume", "agg": "SUM"}],
            "dwd_activity_t_points_user_task_di": [{"key": "user_code", "label": "Unique Users", "agg": "COUNT_DISTINCT"}, {"key": "earned_points", "label": "Total Points Earned", "agg": "SUM"}]
        }

        for t in tables:
            if t in cube_dims:
                for dim in cube_dims[t]:
                    all_dims.add(tuple(dim.items()))
            if t in cube_measures:
                for meas in cube_measures[t]:
                    all_measures.add(tuple(meas.items()))

        unique_dims = sorted([dict(t) for t in all_dims], key=lambda x: x["label"])
        unique_measures = sorted([dict(t) for t in all_measures], key=lambda x: x["label"])

        # 2. FIXED: Multi-Metric Guardrail
        # If the query has multiple metrics (or zero), hide the measure swap pills to prevent bad UX
        if base_sql and unique_measures:
            try:
                ast = sqlglot.parse_one(base_sql, read="postgres")
                agg_count = sum(1 for e in ast.selects if e.find(exp.AggFunc))
                if agg_count != 1:
                    unique_measures = [] # Hide pills
            except Exception:
                pass

        # 3. FIXED: strict KIND mapping via CubeRegistry (with fallback)
        time_controls = []
        if len(tables) == 1:
            is_di = False
            if has_registry:
                cube = CubeRegistry.get_cube(tables[0])
                if cube and getattr(cube, "kind", None) == "di":
                    is_di = True
            elif tables[0].endswith("_di"): # Safe fallback if Registry isn't fully wired yet
                is_di = True
                
            if is_di:
                time_controls = [
                    {"key": "latest_ds", "label": "Today (Latest)"},
                    {"key": "start_7d", "label": "Last 7 Days"},
                    {"key": "start_30d", "label": "Last 30 Days"},
                    {"key": "start_this_month", "label": "This Month"}
                ]

        return {
            "dimensions": unique_dims,
            "measures": unique_measures,
            "time_controls": time_controls
        }


    # def _finalize(self, log_entry, status_type, **kwargs):
    #     if status_type == "success": log_entry.execution_success = True
    #     elif status_type == "error": log_entry.error_message = kwargs.get("message", "Unknown Error")
    #     self.db.commit()
    #     return {"type": status_type, **kwargs}

    # def _finalize(self, log_entry, status_type, **kwargs):
    #     if status_type == "success": 
    #         log_entry.execution_success = True
    #     elif status_type == "error": 
    #         log_entry.error_message = kwargs.get("message", "Unknown Error")
            
    #     # Commit generates the log_entry.id
    #     self.db.commit() 
        
    #     result = {"type": status_type, **kwargs}
        
    #     # --- NEW: Phase 1 Explore Payload Injection ---
    #     if status_type == "success" and log_entry.tables_used:
    #        # explore_meta = self._get_explore_metadata(log_entry.tables_used)
    #         explore_meta = self._get_explore_metadata(log_entry.tables_used, final_sql)
            
    #         # Only append explore if we actually found valid dimensions
    #         if explore_meta["dimensions"]:
    #             result["explore"] = {
    #                 "log_id": log_entry.id,
    #                 "dimensions": explore_meta["dimensions"],
    #                 "measures": explore_meta["measures"]
    #             }
    #     # ----------------------------------------------
        
    #     return result

    def _finalize(self, log_entry, status_type, **kwargs):
        if status_type == "success": 
            log_entry.execution_success = True
        elif status_type == "error": 
            log_entry.error_message = kwargs.get("message", "Unknown Error")
            
        # Commit generates the log_entry.id
        self.db.commit() 
        
        result = {"type": status_type, **kwargs}
        
        # --- Phase 1 & 2 & 3 Explore Payload Injection ---
        if status_type == "success" and log_entry.tables_used:
            # FIX: Extract the SQL string safely from kwargs
            executed_sql = kwargs.get("sql")
            
            explore_meta = self._get_explore_metadata(log_entry.tables_used, executed_sql)
            
            if explore_meta.get("dimensions") or explore_meta.get("measures") or explore_meta.get("time_controls"):
                result["explore"] = {
                    "log_id": log_entry.id,
                    "dimensions": explore_meta.get("dimensions", []),
                    "measures": explore_meta.get("measures", []),
                    "time_controls": explore_meta.get("time_controls", [])
                }
        # ----------------------------------------------
        
        return result

    def _finalize_safe(self, log_entry, status_type, **kwargs):
        return self._json_safe(self._finalize(log_entry, status_type, **kwargs))