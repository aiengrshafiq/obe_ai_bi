import json
from app.services.vanna_wrapper import vn

class ContextResolver:
    """
    Intelligent Context Engine.
    Converts vague follow-ups into precise, standalone instructions using JSON reasoning.
    """

    SYSTEM_PROMPT = """
    You are a Context Resolution Engine for a SQL BI Copilot.
    
    INPUT:
    1. Conversation History (User questions + AI answers)
    2. Current User Message (often vague, e.g., "and their volume", "yes", "make it daily")
    
    TASK:
    Analyze the history to find "Anchor Entities" (e.g., specific Partner IDs, Date Ranges, User Segments) and "Active Topics".
    Then, rewrite the Current User Message into a fully standalone, precise instruction.
    
    OUTPUT FORMAT (JSON ONLY):
    {
        "is_followup": boolean, // Is this a continuation or a new topic?
        "anchor_entities": ["Partner 10000047", "Last 7 Days"], // What context is being carried over?
        "rewritten_query": "The fully resolved standalone question",
        "confidence": float // 0.0 to 1.0
    }
    
    EXAMPLES:
    
    [History]: User: "Show users for partner 100" -> AI: (Table of users)
    [Current]: "and their trading volume"
    [Output]: {
        "is_followup": true,
        "anchor_entities": ["Partner 100"],
        "rewritten_query": "Show trading volume for users belonging to Partner 100",
        "confidence": 0.95
    }
    
    [History]: AI: "I cannot do 12h. Would you like daily trend?"
    [Current]: "Yes"
    [Output]: {
        "is_followup": true,
        "anchor_entities": [],
        "rewritten_query": "Show me the daily trend",
        "confidence": 0.98
    }

    [History]: User: "Show top users"
    [Current]: "Show risk blacklist"
    [Output]: {
        "is_followup": false,
        "anchor_entities": [],
        "rewritten_query": "Show risk blacklist",
        "confidence": 1.0
    }
    """

    @staticmethod
    def resolve(user_msg: str, history: list) -> dict:
        """
        Returns a dict with 'rewritten_query' and metadata.
        """
        # 1. Fast Path: No history = No context needed
        if not history:
            return {"rewritten_query": user_msg, "confidence": 1.0, "is_followup": False}

        # 2. Build History String (Compact)
        conversation_text = ""
        for msg in history[-3:]: # Last 3 turns are usually enough
            role = "User" if msg.get("role") == "user" else "AI"
            content = str(msg.get("content", ""))
            
            # Truncate large data/SQL to save tokens, but keep the "gist"
            if "```" in content or "{" in content or "[" in content:
                # Try to keep the text part of the AI response if possible
                lines = content.split('\n')
                text_lines = [l for l in lines if not l.strip().startswith(('[', '{', '`', 'SELECT'))]
                content = " ".join(text_lines[:2]) + " [Data/SQL Output]"
                
            conversation_text += f"[{role}]: {content}\n"

        user_prompt = f"""
        HISTORY:
        {conversation_text}
        
        CURRENT MESSAGE: "{user_msg}"
        
        RESPONSE (JSON):
        """

        try:
            # Low temp for strict JSON adherence
            response = vn.client.chat.completions.create(
                model=vn.config['model'],
                messages=[
                    {"role": "system", "content": ContextResolver.SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                max_tokens=250
            )
            
            raw_content = response.choices[0].message.content.strip()
            # Clean markdown code blocks if present
            raw_content = raw_content.replace("```json", "").replace("```", "").strip()
            
            result = json.loads(raw_content)
            return result

        except Exception as e:
            print(f"⚠️ Context Resolution Error: {e}")
            # Fallback: Treat as new query
            return {"rewritten_query": user_msg, "confidence": 0.0, "is_followup": False}