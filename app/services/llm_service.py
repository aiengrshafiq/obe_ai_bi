import json
from openai import OpenAI
from app.core.config import settings
from app.llm.schemas import SQLQueryPlan
from app.llm.prompts import SYSTEM_PROMPT_TEMPLATE
from datetime import datetime

class LLMService:
    def __init__(self):
        # This setup replicates your: -H "Authorization: Bearer YOUR_KEY"
        self.client = OpenAI(
            api_key=settings.DASHSCOPE_API_KEY,
            base_url=settings.AI_BASE_URL
        )

    def generate_sql(self, user_question: str, ddl_context: str, history: list = []) -> SQLQueryPlan:
        
        # 1. Format History into a readable string
        history_block = "No previous context."
        if history:
            # We take the last 2 turns (User + AI) to keep it focused
            recent_history = history[-2:] 
            formatted = []
            for msg in recent_history:
                role = "User" if msg['role'] == 'user' else "Last Generated SQL"
                formatted.append(f"{role}: {msg['content']}")
            history_block = "\n".join(formatted)

        # Build the prompt
        prompt = SYSTEM_PROMPT_TEMPLATE.format(
            ddl_context=ddl_context,
            history_block=history_block,
            current_date=datetime.now().strftime("%Y-%m-%d"),
            user_question=user_question
        )

        try:
            print(f"DEBUG: Connecting to {settings.AI_BASE_URL} with model {settings.AI_MODEL_NAME}...")

            # This call replicates your Curl structure exactly
            response = self.client.chat.completions.create(
                model=settings.AI_MODEL_NAME, # "qwen3-coder-plus"
                messages=[
                    {'role': 'system', 'content': 'You are a SQL generator. Output JSON only. No markdown.'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.2, # Matching your curl temperature
            )

            # Get content
            content = response.choices[0].message.content
            
            # Clean up (Just in case the model adds ```json)
            content = content.replace("```json", "").replace("```", "").strip()
            
            print(f"DEBUG: AI Response:\n{content}")

            # Parse JSON
            data = json.loads(content)
            return SQLQueryPlan(**data)

        except Exception as e:
            print(f"❌ AI ERROR: {str(e)}")
            return SQLQueryPlan(
                thought_process=f"Error: {str(e)}",
                sql_query="",
                visualization_type="table",
                is_safe=False
            )

    def generate_insight(self, user_question: str, data: list) -> str:
        """
        Takes the actual data results and generates a 1-sentence business insight.
        """
        if not data:
            return "No data available to analyze."

        # Safety: Only send a sample of data to avoid token limits
        data_sample = data[:10] 
        
        prompt = f"""
        You are a Data Analyst.
        User Question: "{user_question}"
        
        Actual Data Results (Top 10 rows):
        {json.dumps(data_sample, default=str)}
        
        Task: Write ONE single sentence explaining the key trend, winner, or outlier in this data. 
        Do not describe the table structure. Start directly with the insight.
        Example: "Bitcoin dominates the volume with 60% share, followed by ETH."
        """

        try:
            response = self.client.chat.completions.create(
                model=settings.AI_MODEL_NAME, 
                messages=[{'role': 'user', 'content': prompt}],
                temperature=0.5, # Slightly creative
                max_tokens=100
            )
            return response.choices[0].message.content.strip()
        except Exception:
            return "" # Fail silently if insight generation breaks