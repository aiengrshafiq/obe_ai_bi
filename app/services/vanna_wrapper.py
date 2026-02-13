import asyncio
import os
import pandas as pd
from vanna.openai import OpenAI_Chat
from vanna.chromadb import ChromaDB_VectorStore
from app.core.config import settings
from app.db.safe_sql_runner import runner
from openai import OpenAI # <--- Re-add this import just in case we need to force it

class OneBullexVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self):
        # 1. Vector Store
        storage_path = os.path.join(os.getcwd(), 'vanna_storage')
        ChromaDB_VectorStore.__init__(self, config={'path': storage_path})
        
        # 2. LLM Config (FIXED)
        # We MUST pass 'base_url' here so Vanna knows to talk to Alibaba
        OpenAI_Chat.__init__(self, config={
            'api_key': settings.DASHSCOPE_API_KEY,
            'model': settings.AI_MODEL_NAME,
            'base_url': settings.AI_BASE_URL, # <--- RESTORED THIS LINE
        })
        
        # 3. Connect the Safe Runner
        self.run_sql = self._custom_run_sql
        self.run_sql_is_set = True

    def _custom_run_sql(self, sql: str) -> pd.DataFrame:
        """Delegates to our SafeSQLRunner."""
        try:
            return runner.execute(sql)
        except Exception as e:
            print(f"Vanna Execution Failed: {e}")
            return None

    # --- Async Helpers ---
    async def generate_sql_async(self, question: str, allow_llm_to_see_data=False):
        """Async wrapper for the blocking generate_sql method."""
        return await asyncio.to_thread(
            self.generate_sql, 
            question=question, 
            allow_llm_to_see_data=allow_llm_to_see_data
        )

    async def run_sql_async(self, sql: str):
        return await asyncio.to_thread(self._custom_run_sql, sql)

    async def generate_plotly_code_async(self, question: str, sql: str, df):
        return await asyncio.to_thread(
            self.generate_plotly_code, 
            question=question, 
            sql=sql, 
            df=df
        )

vn = OneBullexVanna()