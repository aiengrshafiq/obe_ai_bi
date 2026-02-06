import json
import pandas as pd # <--- NEW IMPORT
from pydantic import BaseModel, Field
from typing import Optional, List
from app.services.vanna_wrapper import vn 

class IntentResponse(BaseModel):
    intent_type: str = Field(..., description="One of: 'data_query', 'general_chat', 'ambiguous'")
    entities: List[str] = Field(default=[], description="List of tables/metrics mentioned")
    time_range: Optional[str] = Field(None, description="Time range mentioned")
    needs_clarification: bool = False
    clarification_question: Optional[str] = None

class IntentAgent:
    """
    Classifies user message BEFORE generating SQL.
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
        prompt = f"""
        {IntentAgent.SYSTEM_PROMPT}
        USER MESSAGE: "{user_msg}"
        Return JSON:
        """
        
        try:
            # FIX: Pass an empty DataFrame instead of None to prevent 'NoneType' error
            empty_df = pd.DataFrame() 
            response_text = await vn.generate_summary(question=prompt, df=empty_df)
            
            # Clean JSON
            json_text = response_text.replace("```json", "").replace("```", "").strip()
            return json.loads(json_text)
            
        except Exception as e:
            print(f"Intent Error: {e}")
            # Fallback
            return {"intent_type": "data_query"}