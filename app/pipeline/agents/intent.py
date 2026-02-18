import json
import re
import asyncio
import pandas as pd
from app.services.vanna_wrapper import vn 

class IntentAgent:
    """
    Classifies user message BEFORE generating SQL.
    Acts as the 'Router' to decide if we need SQL, Chat, or Clarification.
    """
    
    SYSTEM_PROMPT = """
    You are the Intent Classifier for a BI Copilot.
    
    CATEGORIES:
    1. 'data_query': User is asking for data, metrics, charts, lists, or specific business info.
    2. 'general_chat': Greetings, compliments, "thank you", or questions about the AI itself.
    3. 'ambiguous': The question is gibberish, incomplete, or impossible to understand.
    
    INPUT: A fully resolved user question (context has already been added).
    
    OUTPUT: Valid JSON only. Example: {"intent_type": "data_query", "entities": []}
    """

    @staticmethod
    async def classify(user_msg: str) -> dict:
        prompt = f"{IntentAgent.SYSTEM_PROMPT}\nUSER MESSAGE: \"{user_msg}\""
        
        try:
            # FIX: Run blocking LLM call in a thread to keep server responsive
            # We pass an empty DF because generate_summary expects it
            response_text = await asyncio.to_thread(
                vn.generate_summary, 
                question=prompt, 
                df=pd.DataFrame()
            )
            
            # Sanity check
            if not response_text:
                return {"intent_type": "data_query"}

            # Regex to find the JSON object { ... } inside the text
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            
            if json_match:
                clean_json = json_match.group(0)
                return json.loads(clean_json)
            else:
                # Fallback: If it looks like a question, assume data query
                return {"intent_type": "data_query"}
            
        except Exception as e:
            print(f"⚠️ Intent Classification Failed: {e}")
            # Safe Fallback: Assume they want data
            return {"intent_type": "data_query"}