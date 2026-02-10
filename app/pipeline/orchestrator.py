import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any
import re

# Import Agents & Infrastructure
from app.pipeline.agents.intent import IntentAgent
from app.pipeline.agents.sql import SQLAgent
from app.pipeline.agents.visualization import VisualizationAgent
from app.pipeline.guardrails.sql_policy import SQLGuard, SQLPolicyException
from app.services.vanna_wrapper import vn
from app.db.app_models import ChatLog, SessionLocal
from app.services.cache import cache

class Orchestrator:
    """
    The Master Controller.
    Flow: Intent -> SQL Gen -> Safety Check -> Execution -> Visualization
    """
    
    def __init__(self, user: str):
        self.user = user
        self.db = SessionLocal() # For logging

    async def run_pipeline(self, user_msg: str, history_context: str) -> Dict[str, Any]:
        """
        Executes the full Intelligence Pipeline.
        """
        # 1. Log Request
        log_entry = ChatLog(username=self.user, user_question=user_msg, context_provided=history_context)
        self.db.add(log_entry)
        self.db.commit()

        try:
            # --- STEP 1: INTENT CLASSIFICATION ---
            intent_result = await IntentAgent.classify(user_msg)
            
            # A. General Chat
            if intent_result.get("intent_type") == "general_chat":
                return self._finalize(log_entry, "text", message="Hello! I am your Data Analyst. Ask me about Users, Volume, Deposits, or Risk.")
            
            # B. Ambiguous
            if intent_result.get("intent_type") == "ambiguous":
                clarification = intent_result.get("clarification_question") or "Could you please clarify which metrics or dates you are interested in?"
                return self._finalize(log_entry, "text", message=clarification)

            # --- STEP 2: SQL GENERATION ---
            full_prompt = self._build_prompt(user_msg, history_context, intent_result)
            generated_sql = await SQLAgent.generate(full_prompt)
            
            # --- ROBUST SQL DETECTION ---
            clean_sql = re.sub(r'```sql|```', '', generated_sql, flags=re.IGNORECASE).strip()
            is_sql = re.match(r'^(SELECT|WITH)\b', clean_sql, re.IGNORECASE)
            
            if not is_sql or generated_sql.strip().upper().startswith("CLARIFICATION"):
                clean_msg = generated_sql.replace("CLARIFICATION:", "").strip()
                return self._finalize(log_entry, "text", message=clean_msg)

            # --- STEP 3: SAFETY GATE ---
            try:
                safe_sql = SQLGuard.validate_and_fix(clean_sql)
            except SQLPolicyException as e:
                return self._finalize(log_entry, "error", message=f"Security Block: {str(e)}")

            # --- STEP 4: EXECUTION ---
            final_sql = self._apply_replacements(safe_sql)
            log_entry.generated_sql = final_sql
            self.db.commit()

            df = await vn.run_sql_async(final_sql)
            
            if df is None or df.empty:
                return self._finalize(log_entry, "success", sql=final_sql, message="No data found.", data=[])

            # Smarter Sanitization (No fillna(0)!)
            df = self._sanitize_dataframe(df)

            # --- STEP 5: VISUALIZATION INTELLIGENCE ---
            viz_result = await VisualizationAgent.determine_format(df, final_sql, user_msg, intent_result)
            
            # --- FINAL PACKAGING (Fixed Contract) ---
            # Smart Data Limiting: Tables get 100 rows, Charts get full data (up to 5000)
            visual_type = viz_result.get("visual_type", "table")
            if visual_type == "plotly":
                safe_data = df.head(5000).to_dict(orient='records')
            else:
                safe_data = df.head(100).to_dict(orient='records')

            return self._finalize(
                log_entry, 
                "success",
                sql=final_sql,
                data=safe_data,
                visual_type=visual_type,
                plotly_code=viz_result.get("plotly_code"), 
                thought=f"Pipeline: Intent={intent_result.get('intent_type')} -> SQL -> {viz_result.get('thought')}"
            )

        except Exception as e:
            print(f"Orchestrator Error: {e}")
            import traceback
            traceback.print_exc()
            return self._finalize(log_entry, "error", message="An internal error occurred.")
        finally:
            self.db.close()

    # --- HELPERS (Unchanged logic) ---
    def _build_prompt(self, msg, history, intent):
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        latest_ds = cache.get("latest_ds") or yesterday.strftime("%Y%m%d")
        latest_ds_iso = f"{latest_ds[:4]}-{latest_ds[4:6]}-{latest_ds[6:]}"
        today_iso = today.strftime("%Y-%m-%d")
        start_7d = (datetime.strptime(latest_ds, "%Y%m%d") - timedelta(days=6)).strftime("%Y%m%d")
        
        return f"""
        {history}
        CURRENT CONTEXT:
        - **System:** Alibaba Dataworks / Hologres Architecture.
        - **Anchor Date (Latest Data):** {latest_ds_iso} (Partition: ds='{latest_ds}').
        - **Real Time:** {today_iso} (Do NOT use this).
        INTENT: {intent.get('intent_type')}
        ENTITIES: {intent.get('entities', [])}
        CRITICAL PARTITIONING RULES:
        1. **INCREMENTAL (_di):** `dws_all_trades_di`, `dws_user_deposit...`.
           - Trend? Use RANGE: `WHERE ds BETWEEN '{start_7d}' AND '{latest_ds}'`
           - Snapshot? `WHERE ds = '{latest_ds}'`
        2. **SNAPSHOT (_df):** `user_profile_360`, `ads_total_root...`.
           - Current State? Use `WHERE ds = '{latest_ds}'`
        3. **TIME:** NEVER use `NOW()`. Use `ds`.
        CRITICAL SQL RULES:
        1. **Funnels:** Use `UNION ALL`.
        2. **Formatting:** `user_code` is STRING.
        3. **Query Pattern:** `SELECT DATE_TRUNC('hour', [time_col]), COUNT(*) ...`
        NEW QUESTION: {msg}
        """

    def _apply_replacements(self, sql):
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        latest_ds = cache.get("latest_ds") or yesterday.strftime("%Y%m%d")
        latest_ds_dash = f"{latest_ds[:4]}-{latest_ds[4:6]}-{latest_ds[6:]}"
        today_iso = today.strftime("%Y-%m-%d")
        sql = sql.replace("{latest_ds}", latest_ds)
        sql = sql.replace("{latest_ds_dash}", latest_ds_dash)
        sql = sql.replace("{today_iso}", today_iso)
        return sql

    def _sanitize_dataframe(self, df):
        try:
            df.replace([float('inf'), float('-inf')], 0, inplace=True)
        except:
            pass
        return df

    def _finalize(self, log_entry, status_type, **kwargs):
        if status_type == "success":
            log_entry.execution_success = True
        elif status_type == "error":
            log_entry.error_message = kwargs.get("message", "Unknown Error")
        self.db.commit()
        return {"type": status_type, **kwargs}