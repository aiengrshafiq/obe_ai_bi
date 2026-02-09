import json
import re
import pandas as pd
from app.services.vanna_wrapper import vn 

class IntentAgent:
    """
    Classifies user message BEFORE generating SQL.
    """
    
    SYSTEM_PROMPT = """
    You are the Intent Classifier.
    CATEGORIES:
    1. 'data_query': asking for data, metrics, charts.
    2. 'general_chat': greetings, non-data talk.
    3. 'ambiguous': vague questions.
    
    OUTPUT: Valid JSON only. Example: {"intent_type": "data_query", "entities": []}
    """

    @staticmethod
    async def classify(user_msg: str) -> dict:
        prompt = f"{IntentAgent.SYSTEM_PROMPT}\nUSER MESSAGE: \"{user_msg}\""
        
        try:
            empty_df = pd.DataFrame()
            # Generate response
            response_text = vn.generate_summary(question=prompt, df=empty_df)
            
            # Sanity check
            if not response_text:
                return {"intent_type": "data_query"}

            # Regex to find the JSON object { ... } inside the text
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            
            if json_match:
                clean_json = json_match.group(0)
                return json.loads(clean_json)
            else:
                # If no JSON found, assume data_query (Safe Fallback)
                return {"intent_type": "data_query"}
            
        except Exception:
            # Suppress error printing to keep logs clean, since fallback works.
            return {"intent_type": "data_query"}