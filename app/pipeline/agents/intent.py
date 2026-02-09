import json
import re
from app.services.vanna_wrapper import vn 

class IntentAgent:
    """
    Classifies user message BEFORE generating SQL.
    Returns structured intent data to the Orchestrator.
    """
    
    SYSTEM_PROMPT = """
    You are the Intent Classifier for OneBullEx (OBE) Data Platform.
    Your job is to categorize the user's input into JSON format.
    
    CATEGORIES:
    1. 'data_query': User asks for stats, numbers, lists, trends, charts, or specific business metrics (e.g. "volume", "users", "risk").
    2. 'general_chat': User says "Hi", "Thanks", "Who are you?", or non-data questions.
    3. 'ambiguous': User asks a vague question like "Show me data" or "How is performance" without specifying time or metric.
    
    OUTPUT SCHEMA (JSON Only):
    {
        "intent_type": "data_query" | "general_chat" | "ambiguous",
        "entities": ["list", "of", "metrics", "or", "tables"],
        "clarification_question": "If ambiguous, ask a specific question like 'By volume or count?'" (or null)
    }
    """

    @staticmethod
    async def classify(user_msg: str) -> dict:
        prompt = f"""
        {IntentAgent.SYSTEM_PROMPT}
        USER MESSAGE: "{user_msg}"
        Return strictly JSON. No markdown formatting.
        """
        
        try:
            # Use generate_text for raw LLM prompts (generate_summary requires a DF)
            # Depending on your Vanna version/wrapper, ensure this is the non-plotting text method.
            # If vn.generate_text is sync, we wrap it or just run it (since this func is async def).
            response_text = vn.generate_text(prompt)
            
            # --- GUARDRAIL: Clean JSON ---
            # Remove markdown code fences if the LLM adds them
            clean_json = re.sub(r'```json|```', '', response_text, flags=re.IGNORECASE).strip()
            
            # Parse
            result = json.loads(clean_json)
            
            # Ensure keys exist
            if "intent_type" not in result:
                result["intent_type"] = "data_query"
                
            return result
            
        except Exception as e:
            print(f"Intent Classification Error: {e}")
            # Safe Fallback to ensure pipeline continues
            return {
                "intent_type": "data_query", 
                "entities": [], 
                "clarification_question": None
            }