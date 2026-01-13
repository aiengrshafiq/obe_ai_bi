import json
from http import HTTPStatus
import dashscope
from app.core.config import settings
from app.llm.schemas import SQLQueryPlan
from app.llm.prompts import SYSTEM_PROMPT_TEMPLATE
from datetime import datetime

# Use Qwen-2.5-Coder-32B for best SQL performance
MODEL_NAME = "qwen2.5-coder-32b-instruct"

class LLMService:
    def __init__(self):
        dashscope.api_key = settings.DASHSCOPE_API_KEY

    def generate_sql(self, user_question: str, ddl_context: str) -> SQLQueryPlan:
        prompt = SYSTEM_PROMPT_TEMPLATE.format(
            ddl_context=ddl_context,
            current_date=datetime.now().strftime("%Y-%m-%d"),
            user_question=user_question
        )

        try:
            # Call Qwen API
            response = dashscope.Generation.call(
                model=MODEL_NAME,
                messages=[{'role': 'system', 'content': 'You are a SQL generator. Output JSON only.'},
                          {'role': 'user', 'content': prompt}],
                result_format='message',  # Returns full message object
            )

            if response.status_code == HTTPStatus.OK:
                content = response.output.choices[0].message.content
                
                # Clean up response (sometimes LLMs add ```json ... ``` wrappers)
                content = content.replace("```json", "").replace("```", "").strip()
                
                # Parse JSON into our strict Pydantic model
                data = json.loads(content)
                return SQLQueryPlan(**data)
            else:
                raise Exception(f"Qwen API Error: {response.message}")
        
        except Exception as e:
            # Fail safe: Return a safe error plan
            return SQLQueryPlan(
                thought_process=f"Error generating query: {str(e)}",
                sql_query="",
                visualization_type="table",
                is_safe=False
            )