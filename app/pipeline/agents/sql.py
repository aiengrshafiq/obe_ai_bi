from app.services.vanna_wrapper import vn

class SQLAgent:
    """
    Agent responsible for generating SQL from Natural Language.
    """
    
    @staticmethod
    async def generate(prompt: str) -> str:
        """
        Generates SQL using the RAG model.
        """
        sql = await vn.generate_sql_async(question=prompt, allow_llm_to_see_data=True)
        return sql