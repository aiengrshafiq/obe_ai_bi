import asyncio
import os
from vanna.openai import OpenAI_Chat
from vanna.chromadb import ChromaDB_VectorStore
from app.core.config import settings
from openai import OpenAI  # <--- Added this import

# We inherit from the LOCAL classes to avoid Vanna Cloud
class OneBullexVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self):
        # 1. Local Vector Storage
        storage_path = os.path.join(os.getcwd(), 'vanna_storage')
        ChromaDB_VectorStore.__init__(self, config={'path': storage_path})
        
        # 2. Initialize Vanna Parent
        # We pass the config, but Vanna might ignore 'base_url' internally
        OpenAI_Chat.__init__(self, config={
            'api_key': settings.DASHSCOPE_API_KEY,
            'model': settings.AI_MODEL_NAME, 
        })

        # --- CRITICAL FIX: FORCE ALIBABA CONNECTION ---
        # We manually overwrite the client to ensure it uses the correct Base URL.
        # This guarantees traffic goes to DashScope, not OpenAI.
        self.client = OpenAI(
            api_key=settings.DASHSCOPE_API_KEY,
            base_url=settings.AI_BASE_URL,
        )
        print(f"✅ Vanna Client Forced to URL: {settings.AI_BASE_URL}")
        # -----------------------------------------------

    # Helper to run Vanna methods without blocking the server
    async def generate_sql_async(self, question: str, allow_llm_to_see_data=False):
        return await asyncio.to_thread(
            self.generate_sql, 
            question=question, 
            allow_llm_to_see_data=allow_llm_to_see_data
        )

    async def run_sql_async(self, sql: str):
        return await asyncio.to_thread(self.run_sql, sql=sql)

    # Added helper for charts
    async def generate_plotly_code_async(self, question: str, sql: str, df):
        return await asyncio.to_thread(
            self.generate_plotly_code, 
            question=question, 
            sql=sql, 
            df=df
        )

# Initialize Singleton
vn = OneBullexVanna()