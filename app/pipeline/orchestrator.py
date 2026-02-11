import pandas as pd
import numpy as np
import math
from datetime import datetime, date, timedelta
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

# --- NEW IMPORT ---
from app.pipeline.prompts.sql_prompt import get_sql_system_prompt

class Orchestrator:
    """
    The Master Controller.
    Flow: Intent -> SQL Gen -> Safety Check -> Execution -> Visualization
    """
    
    def __init__(self, user: str):
        self.user = user
        self.db = SessionLocal() 

    def _json_safe(self, obj):
        """Global safety net. Ensures NO NaN, Inf, or weird types escape."""
        if obj is None or obj is pd.NaT: return None
        if isinstance(obj, (pd.Timestamp, datetime, date)): return obj.isoformat()
        if isinstance(obj, np.generic): return self._json_safe(obj.item())
        if isinstance(obj, float): return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, (int, bool, str)): return obj
        if isinstance(obj, dict): return {str(k): self._json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)): return [self._json_safe(v) for v in obj]
        return str(obj)

    async def run_pipeline(self, user_msg: str, history_context: str) -> Dict[str, Any]:
        """Executes the full Intelligence Pipeline."""
        log_entry = ChatLog(username=self.user, user_question=user_msg, context_provided=history_context)
        self.db.add(log_entry)
        self.db.commit()

        try:
            # --- STEP 1: INTENT ---
            intent_result = await IntentAgent.classify(user_msg)
            
            if intent_result.get("intent_type") == "general_chat":
                return self._finalize_safe(log_entry, "text", message="Hello! I am your Data Analyst. Ask me about Users, Volume, Deposits, or Risk.")
            
            if intent_result.get("intent_type") == "ambiguous":
                clarification = intent_result.get("clarification_question") or "Could you please clarify which metrics or dates you are interested in?"
                return self._finalize_safe(log_entry, "text", message=clarification)

            # --- STEP 2: SQL GENERATION (Refactored) ---
            full_prompt = self._build_prompt(user_msg, history_context, intent_result)
            generated_sql = await SQLAgent.generate(full_prompt)
            
            clean_sql = re.sub(r'```sql|```', '', generated_sql, flags=re.IGNORECASE).strip()
            if not re.match(r'^(SELECT|WITH)\b', clean_sql, re.IGNORECASE):
                clean_msg = generated_sql.replace("CLARIFICATION:", "").strip()
                return self._finalize_safe(log_entry, "text", message=clean_msg)

            # --- STEP 3: SAFETY GATE ---
            try:
                safe_sql = SQLGuard.validate_and_fix(clean_sql)
            except SQLPolicyException as e:
                return self._finalize_safe(log_entry, "error", message=f"Security Block: {str(e)}")

            # --- STEP 4: EXECUTION ---
            final_sql = self._apply_replacements(safe_sql)
            log_entry.generated_sql = final_sql
            self.db.commit()

            df = await vn.run_sql_async(final_sql)
            
            if df is None or df.empty:
                return self._finalize_safe(log_entry, "success", sql=final_sql, message="No data found.", data=[])

            # Smarter Sanitization (No fillna(0))
            df = self._sanitize_dataframe(df)

            # --- STEP 5: VISUALIZATION ---
            viz_result = await VisualizationAgent.determine_format(df, final_sql, user_msg, intent_result)
            
            # Smart Data Limiting
            df_safe = df.where(pd.notnull(df), None) # Clean for JSON
            visual_type = viz_result.get("visual_type", "table")
            safe_data = df_safe.head(5000 if visual_type == "plotly" else 100).to_dict(orient='records')

            result = self._finalize(
                log_entry, "success", sql=final_sql, data=safe_data,
                visual_type=visual_type, plotly_code=viz_result.get("plotly_code"), 
                thought=f"Pipeline: Intent={intent_result.get('intent_type')} -> SQL -> {viz_result.get('thought')}"
            )
            
            return self._json_safe(result)

        except Exception as e:
            print(f"Orchestrator Error: {e}")
            import traceback; traceback.print_exc()
            return self._finalize_safe(log_entry, "error", message="An internal error occurred.")
        finally:
            self.db.close()

    # --- HELPERS ---
    def _build_prompt(self, msg, history, intent):
        # 1. Calculate Logic
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        latest_ds = cache.get("latest_ds") or yesterday.strftime("%Y%m%d")
        
        # 2. Format Strings
        latest_ds_iso = f"{latest_ds[:4]}-{latest_ds[4:6]}-{latest_ds[6:]}"
        today_iso = today.strftime("%Y-%m-%d")
        start_7d = (datetime.strptime(latest_ds, "%Y%m%d") - timedelta(days=6)).strftime("%Y%m%d")
        
        # 3. Call External Builder
        return get_sql_system_prompt(
            history=history,
            intent_type=intent.get('intent_type'),
            entities=intent.get('entities', []),
            latest_ds=latest_ds,
            latest_ds_iso=latest_ds_iso,
            today_iso=today_iso,
            start_7d=start_7d,
            user_msg=msg
        )

    def _apply_replacements(self, sql):
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        latest_ds = cache.get("latest_ds") or yesterday.strftime("%Y%m%d")
        latest_ds_dash = f"{latest_ds[:4]}-{latest_ds[4:6]}-{latest_ds[6:]}"
        today_iso = today.strftime("%Y-%m-%d")
        return sql.replace("{latest_ds}", latest_ds).replace("{latest_ds_dash}", latest_ds_dash).replace("{today_iso}", today_iso)

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