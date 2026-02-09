import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any

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
            
            if intent_result.get("intent_type") == "general_chat":
                # Skip SQL, just chat
                response_text = await vn.generate_summary(question=user_msg, df=None)
                return self._finalize(log_entry, "text", message=response_text)
            
            if intent_result.get("intent_type") == "ambiguous":
                return self._finalize(log_entry, "text", message="Could you please clarify which metrics or dates you are interested in?")

            # --- STEP 2: SQL GENERATION (Context-Aware) ---
            full_prompt = self._build_prompt(user_msg, history_context, intent_result)
            
            generated_sql = await SQLAgent.generate(full_prompt)
            
            # Check for textual refusal ("CLARIFICATION: ...")
            if "SELECT" not in generated_sql.upper() or generated_sql.strip().startswith("CLARIFICATION"):
                clean_msg = generated_sql.replace("CLARIFICATION:", "").strip()
                return self._finalize(log_entry, "text", message=clean_msg)

            # --- STEP 3: SAFETY GATE (AST Validation) ---
            try:
                # This enforces LIMIT 100 and checks for DROP/DELETE
                safe_sql = SQLGuard.validate_and_fix(generated_sql)
            except SQLPolicyException as e:
                return self._finalize(log_entry, "error", message=f"Security Block: {str(e)}")

            # --- STEP 4: EXECUTION (Async) ---
            # Deterministic Replacements (Date placeholders)
            final_sql = self._apply_replacements(safe_sql)
            
            # Log the final SQL
            log_entry.generated_sql = final_sql
            self.db.commit()

            df = await vn.run_sql_async(final_sql)
            
            if df is None or df.empty:
                return self._finalize(log_entry, "success", sql=final_sql, message="No data found.", data=[])

            # Sanitize Data (NaN -> 0)
            df = self._sanitize_dataframe(df)

            # --- STEP 5: VISUALIZATION INTELLIGENCE ---
            viz_result = await VisualizationAgent.determine_format(df, final_sql, user_msg)
            
            # --- FINAL PACKAGING ---
            return self._finalize(
                log_entry, 
                "success",
                sql=final_sql,
                data=df.head(100).to_dict(orient='records'),
                visual_type=viz_result['type'],
                # FIX: Use .get('data') because VisualizationAgent now returns JSON data, not python code
                plotly_code=viz_result.get('data'), 
                thought=f"Pipeline: Intent={intent_result['intent_type']} -> SQL -> {viz_result['thought']}"
            )

        except Exception as e:
            print(f"Orchestrator Error: {e}")
            return self._finalize(log_entry, "error", message="An internal error occurred.")
        finally:
            self.db.close()

    # --- HELPERS ---

    def _build_prompt(self, msg, history, intent):
        # Dynamic Dates
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        # Get Latest Partition (or fallback to yesterday)
        latest_ds = cache.get("latest_ds")
        if not latest_ds:
            latest_ds = yesterday.strftime("%Y%m%d")
            
        
        # Formats: 20260208 and 2026-02-08
        latest_ds_iso = f"{latest_ds[:4]}-{latest_ds[4:6]}-{latest_ds[6:]}"
        today_iso = today.strftime("%Y-%m-%d")
        
        return f"""
        {history}
        
        CURRENT CONTEXT:
        - You are the Data Analyst for **OneBullEx (OBE)**.
        - **SYSTEM TIME:** The database is updated via Daily Batch. 
        - **LATEST AVAILABLE DATA:** {latest_ds_iso} (Partition: ds='{latest_ds}').
        - **REAL TIME:** {today_iso} (Do NOT use this for data filtering).
        
        INTENT: {intent.get('intent_type')}
        ENTITIES: {intent.get('entities', [])}
        
        CRITICAL SQL RULES:
        1. **Snapshots:** For "Current Status" (e.g. "total users", "balance"), YOU MUST filter by `ds='{latest_ds}'`.
        2. **History/Trends:** If user asks for "Trend", "History", "Last Month", or "MAU", you MAY filter by a date range (e.g. `ds BETWEEN ...`).
        3. **Funnels:** Use `UNION ALL` to stack stages vertically.
        4. **Formatting:** `user_code` is STRING (use quotes).
        
        CRITICAL TIME HANDLING RULES (ANCHOR SHIFTING):
        1. **Daily Batch Rule:** The data ends at {latest_ds_iso} 23:59:59. 
           - If user asks for "Last 12 hours" or "Recent", they mean **the last 12 hours of available data**.
           - **NEVER** use `NOW()` or `CURRENT_TIMESTAMP`. It will return 0 rows.
           
        2. **Column Selection:**
           - Users/Registration -> use `registration_date`
           - Trades -> use `trade_datetime`
           - Deposits/Withdrawals -> use `create_at`
           - Risk -> use `start_date`
           - Login/Device -> use `create_at`
           - **Referrals** (`ads_total_root_...`) -> NO time column. Only Daily Trends allowed.
           
        3. **Hourly Query Pattern:**
           To show hourly trend for the latest day:
           `SELECT DATE_TRUNC('hour', [time_col]) as hour, COUNT(*) FROM [table] WHERE ds='{latest_ds}' GROUP BY 1 ORDER BY 1`

        4. **General Rules:**
           - Always filter `ds='{latest_ds}'` for snapshots.
          

        NEW QUESTION: {msg}
        """

    def _apply_replacements(self, sql):
        # Calculate fresh dates
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        latest_ds = cache.get("latest_ds")
        if not latest_ds:
            latest_ds = yesterday.strftime("%Y%m%d")

        latest_ds_dash = f"{latest_ds[:4]}-{latest_ds[4:6]}-{latest_ds[6:]}"
        today_iso = today.strftime("%Y-%m-%d")

        # Perform string replacements
        sql = sql.replace("{latest_ds}", latest_ds)
        sql = sql.replace("{latest_ds_dash}", latest_ds_dash)
        sql = sql.replace("{today_iso}", today_iso)
        return sql

    def _sanitize_dataframe(self, df):
        try:
            df.replace([float('inf'), float('-inf')], 0, inplace=True)
            df.fillna(0, inplace=True)
        except:
            pass
        return df

    def _finalize(self, log_entry, status_type, **kwargs):
        """Helper to update log and return dict"""
        if status_type == "success":
            log_entry.execution_success = True
        elif status_type == "error":
            log_entry.error_message = kwargs.get("message", "Unknown Error")
        
        self.db.commit()
        
        return {
            "type": status_type,
            **kwargs
        }