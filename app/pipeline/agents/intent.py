import json
from pydantic import BaseModel, Field
from typing import Optional, List
# Reuse your existing wrapper or create a lightweight one
from app.services.vanna_wrapper import vn 

class IntentResponse(BaseModel):
    intent_type: str = Field(..., description="One of: 'data_query', 'general_chat', 'ambiguous'")
    entities: List[str] = Field(default=[], description="List of tables/metrics mentioned e.g. ['users', 'volume']")
    time_range: Optional[str] = Field(None, description="Time range mentioned e.g. 'last_7_days', 'yesterday'")
    needs_clarification: bool = False
    clarification_question: Optional[str] = None

class IntentAgent:
    """
    Classifies user message BEFORE generating SQL.
    Decides if we should query the DB or just chat.
    """
    
    SYSTEM_PROMPT = """
    You are the Intent Classifier for OneBullEx (OBE) Data Platform.
    Your job is to categorize the user's input into JSON.
    
    CATEGORIES:
    1. 'data_query': User asks for stats, numbers, lists, trends, or charts.
    2. 'general_chat': User says "Hi", "Thanks", "Who are you?", or non-data questions.
    3. 'ambiguous': User asks a vague question like "Show me data" without specifying what data.
    
    OUTPUT FORMAT:
    Strict JSON only. No markdown.
    """

    @staticmethod
    async def classify(user_msg: str) -> dict:
        # We use a lightweight prompt to Qwen/OpenAI
        prompt = f"""
        {IntentAgent.SYSTEM_PROMPT}
        
        USER MESSAGE: "{user_msg}"
        
        Return JSON:
        """
        
        try:
            # We assume vn.generate_response returns text. 
            # In production, use a lower-temperature call.
            # Here we reuse the existing connection for simplicity.
            response_text = await vn.generate_summary(question=prompt, df=None) # Using summary method as generic LLM call
            
            # Clean JSON
            json_text = response_text.replace("```json", "").replace("```", "").strip()
            return json.loads(json_text)
            
        except Exception as e:
            # Fallback: Assume it's a data query if it fails
            print(f"Intent Error: {e}")
            return {"intent_type": "data_query", "needs_clarification": False}