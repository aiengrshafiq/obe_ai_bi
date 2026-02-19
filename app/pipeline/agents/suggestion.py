# app/pipeline/agents/suggestion.py
import pandas as pd

class SuggestionAgent:
    """
    Generates context-aware "Next Action" chips.
    Uses a Semantic Catalog to ensure suggestions are valid for the current data.
    """
    
    # Domain Mapping: If we see these columns, we suggest these breakdowns
    SEMANTIC_CATALOG = {
        'trading': {
            'triggers': ['deal_amount', 'trade_volume', 'fee', 'symbol'],
            'dimensions': [
                {'label': 'By Symbol', 'prompt': 'Break down by symbol'},
                {'label': 'By Side', 'prompt': 'Break down by buy/sell side'},
                {'label': 'By Market', 'prompt': 'Break down by market type'}
            ]
        },
        'user': {
            'triggers': ['user_code', 'registration_date', 'kyc_status', 'country'],
            'dimensions': [
                {'label': 'By Country', 'prompt': 'Break down by country'},
                {'label': 'By KYC', 'prompt': 'Break down by KYC status'},
                {'label': 'By Segment', 'prompt': 'Break down by user segment'}
            ]
        },
        'risk': {
            'triggers': ['risk_score', 'reason', 'blocked'],
            'dimensions': [
                {'label': 'By Reason', 'prompt': 'Break down by risk reason'},
                {'label': 'Top Risky', 'prompt': 'Show top 10 risky users'}
            ]
        }
    }

    @staticmethod
    def generate(df: pd.DataFrame, user_msg: str) -> list:
        if df is None or df.empty:
            return []

        suggestions = []
        cols = [c.lower() for c in df.columns]
        
        # 1. Detect Domain based on columns present
        active_dimensions = []
        for domain, config in SuggestionAgent.SEMANTIC_CATALOG.items():
            # If any trigger column exists in the result
            if any(trigger in cols for trigger in config['triggers']):
                active_dimensions.extend(config['dimensions'])

        # 2. Time-Based Suggestions (Universal)
        has_time = any(x in cols for x in ['ds', 'date', 'day', 'month', 'time'])
        
        if has_time:
            # If it's a trend, suggest zooming or aggregating
            suggestions.append({'label': 'Last 30 Days', 'prompt': 'Filter for the last 30 days'})
            suggestions.append({'label': 'Weekly View', 'prompt': 'Group by week'})
        else:
            # If it's a snapshot, suggest trending
            suggestions.append({'label': 'Daily Trend', 'prompt': 'Show the daily trend for this'})

        # 3. Add Relevant Dimensions (Deduplicated)
        # We only suggest a dimension if it's NOT already in the columns
        count = 0
        for dim in active_dimensions:
            # Check if the prompt keywords are already in the column list to avoid redundancy
            # e.g. Don't suggest "By Country" if "country" is already a column
            keyword = dim['label'].replace("By ", "").lower()
            if keyword not in cols:
                suggestions.append(dim)
                count += 1
            if count >= 3: break # Limit dimension chips

        # 4. Limit Total Chips (Max 4)
        return suggestions[:4]