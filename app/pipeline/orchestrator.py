import pandas as pd
import numpy as np
import math
import re
import time
import traceback
import asyncio
from datetime import datetime
from typing import Dict, Any, List

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
        
        # --- STEP 0: CONTEXT RESOLUTION (Structured) ---
        final_msg = user_msg
        context_metadata = {}

        if raw_history:
            # Run the resolver in a thread to be safe
            resolution = await asyncio.to_thread(ContextResolver.resolve, user_msg, raw_history)
            
            # Logic: If high confidence rewrite, use it.
            if resolution.get("confidence", 0) > 0.6:
                final_msg = resolution["rewritten_query"]
                context_metadata = resolution
                print(f"🔄 Context Rewritten: '{user_msg}' -> '{final_msg}'")
            else:
                print(f"⚠️ Context skipped (Low Confidence): Using original message.")
        # -----------------------------------------------

        # 1. Build Legacy String Context (For the SQL Prompt)
        # We still pass this to SQLAgent so it sees the conversation flow
        history_context = ""
        if raw_history:
             recent = raw_history[-4:]
             history_context = "PREVIOUS CONVERSATION:\n" + "\n".join(
                 [f"{msg.get('role', 'unknown').upper()}: {str(msg.get('content',''))[:200]}..." for msg in recent]
             )

        log_entry = ChatLog(
            username=self.user, 
            user_question=final_msg, # Log the RESOLVED question
            context_provided=history_context,
            correction_attempts=0
        )
        self.db.add(log_entry)
        self.db.commit()

        try:
            # --- STEP 1: INTENT ---
            intent_result = await IntentAgent.classify(final_msg)
            
            if intent_result.get("intent_type") == "general_chat":
                return self._finalize_safe(log_entry, "text", message="Hello! I am your Data Analyst. Ask me about Users, Volume, Deposits, or Risk.")
            
            if intent_result.get("intent_type") == "ambiguous":
                clarification = intent_result.get("clarification_question") or "Could you please clarify which metrics or dates you are interested in?"
                return self._finalize_safe(log_entry, "text", message=clarification)

            # --- STEP 2: PREPARE CONTEXT (DateResolver) ---
            date_context = await DateResolver.get_date_context()
            
            # LOGGING: Save the resolved date
            log_entry.resolved_latest_ds = date_context['latest_ds']

            # --- STEP 3: REASONING LOOP (Self-Healing) ---
            max_retries = 2
            attempts = 0
            last_error = None
            
            # Initial Prompt (Uses FINAL_MSG)
            current_prompt = self._build_prompt(final_msg, history_context, intent_result, date_context)
            
            while attempts <= max_retries:
                # Initialize variables to avoid UnboundLocalError
                clean_sql = ""
                
                try:
                    # A. Generate SQL
                    generated_sql = await SQLAgent.generate(current_prompt)
                    
                    # --- NEW FIX: Detect and Reject Intermediate SQL ---
                    if "intermediate_sql" in generated_sql.lower():
                        raise ValueError("LLM tried to inspect data (intermediate_sql detected). Retrying with stricter instruction.")
                    # ---------------------------------------------------
                    
                    # Clean the output
                    clean_sql = re.sub(r'```sql|```', '', generated_sql, flags=re.IGNORECASE).strip()
                    clean_sql = clean_sql.replace("CLARIFICATION:", "").strip()

                    # --- NEW FIX: Handle "I don't know" text responses ---
                    # If it doesn't start with SELECT/WITH, it's not SQL. Don't parse it.
                    if not re.match(r'^\s*(SELECT|WITH)\b', clean_sql, re.IGNORECASE):
                        return self._finalize_safe(log_entry, "text", message=clean_sql)
                    # -----------------------------------------------------

                    # B. Safety Gate
                    safe_sql = SQLGuard.validate_and_fix(clean_sql)
                    
                    # LOGGING: Extract Tables
                    tables = re.findall(r'(?:FROM|JOIN)\s+(?:public\.)?([a-zA-Z0-9_]+)', safe_sql, re.IGNORECASE)
                    log_entry.tables_used = ", ".join(set(tables))
                    
                    # C. Apply Date Replacements
                    final_sql = self._apply_replacements(safe_sql, date_context)

                    # D. Execution
                    df = await vn.run_sql_async(final_sql)
                    
                    # Log Success & Update attempts
                    log_entry.generated_sql = final_sql
                    log_entry.correction_attempts = attempts
                    self.db.commit()

                    if df is None or df.empty:
                        # Calculate timing before returning
                        log_entry.execution_ms = int((time.time() - start_time) * 1000)
                        return self._finalize_safe(log_entry, "success", sql=final_sql, message="No data found.", data=[])

                    # E. Visualization
                    df = self._sanitize_dataframe(df)
                    
                    # LOGGING: Row Count
                    log_entry.row_count = len(df)
                    
                    viz_result = await VisualizationAgent.determine_format(df, final_sql, final_msg, intent_result) # <--- Passed final_msg
                    
                    # LOGGING: Visual Type
                    visual_type = viz_result.get("visual_type", "table")
                    log_entry.visual_type = visual_type

                    # --- NEW: Generate Suggestions ---
                    suggestions = SuggestionAgent.generate(df, final_msg)
                    # ---------------------------------
                    
                    df_safe = df.where(pd.notnull(df), None)
                    safe_data = df_safe.head(5000 if visual_type == "plotly" else 100).to_dict(orient='records')

                    # LOGGING: Final Execution Time
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
                        current_prompt = (
                            f"The previous SQL you generated failed.\n"
                            f"FAILED SQL: {sql_context}\n"
                            f"ERROR MESSAGE: {last_error}\n"
                            f"TASK: Fix the SQL logic. Return ONLY the valid SQL."
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
            return self._finalize_safe(log_entry, "error", message="An internal system error occurred.")
        finally:
            self.db.close()

    # --- HELPERS ---
   def _build_prompt(self, msg, history, intent, date_ctx):
        return get_sql_system_prompt(
            history=history,
            intent_type=intent.get('intent_type'),
            entities=intent.get('entities', []),
            latest_ds=date_ctx['latest_ds'],
            latest_ds_iso=date_ctx['latest_ds_dash'],
            today_iso=date_ctx['today_iso'],
            start_7d=date_ctx['start_7d'],              # Passed to original slot
            start_7d_dash=date_ctx['start_7d_dash'],    # <--- CRITICAL FIX (Match prompt sig)
            start_this_month=date_ctx['start_this_month_dash'], 
            start_last_month=date_ctx['start_last_month_dash'], 
            end_last_month=date_ctx['end_last_month_dash'],     
            user_msg=msg
        )

    def _apply_replacements(self, sql, date_ctx):
        sql = sql.replace("{latest_ds}", date_ctx['latest_ds'])
        sql = sql.replace("{latest_ds_dash}", date_ctx['latest_ds_dash'])
        sql = sql.replace("{today_iso}", date_ctx['today_iso'])
        sql = sql.replace("{start_30d}", date_ctx['start_30d'])
        return sql
        
    def _sanitize_dataframe(self, df):
        try: df.replace([np.inf, -np.inf], np.nan, inplace=True)
        except: pass
        return df

    def _finalize(self, log_entry, status_type, **kwargs):
        if status_type == "success": log_entry.execution_success = True
        elif status_type == "error": log_entry.error_message = kwargs.get("message", "Unknown Error")
        self.db.commit()
        return {"type": status_type, **kwargs}

    def _finalize_safe(self, log_entry, status_type, **kwargs):
        return self._json_safe(self._finalize(log_entry, status_type, **kwargs))