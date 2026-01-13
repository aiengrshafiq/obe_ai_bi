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

    def generate_sql(self, user_question: str, ddl_context: str) -> SQLQueryPlan:
        # Build the prompt
        prompt = SYSTEM_PROMPT_TEMPLATE.format(
            ddl_context=ddl_context,
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