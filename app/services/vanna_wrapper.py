import asyncio
import os
from vanna.openai import OpenAI_Chat
from vanna.chromadb import ChromaDB_VectorStore
from app.core.config import settings

class OneBullexVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self):
        # 1. Local Vector Storage (Saves to /app/vanna_storage on the server)
        storage_path = os.path.join(os.getcwd(), 'vanna_storage')
        ChromaDB_VectorStore.__init__(self, config={'path': storage_path})
        
        # 2. Your Existing Qwen Brain
        OpenAI_Chat.__init__(self, config={
            'api_key': settings.DASHSCOPE_API_KEY,
            'model': settings.AI_MODEL_NAME, 
            'base_url': settings.AI_BASE_URL,
        })

    # Helper to run Vanna methods without blocking the server
    async def generate_sql_async(self, question: str, allow_llm_to_see_data=False):
        return await asyncio.to_thread(
            self.generate_sql, 
            question=question, 
            allow_llm_to_see_data=allow_llm_to_see_data
        )

    async def run_sql_async(self, sql: str):
        return await asyncio.to_thread(self.run_sql, sql=sql)

# Initialize Singleton
vn = OneBullexVanna()