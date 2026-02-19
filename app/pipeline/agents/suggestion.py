# app/pipeline/agents/suggestion.py
import pandas as pd

class SuggestionAgent:
    """
    Intelligence Engine for "Next Action" Chips.
    
    It analyzes the result dataframe to detect:
    1. The 'Business Domain' (User, Trade, Risk, etc.)
    2. The 'Current Granularity' (Daily, Total, etc.)
    
    Then it generates context-aware drill-down chips.
    """
    
    # --- SEMANTIC CATALOG (The Brain) ---
    # Maps specific columns to the best next questions.
    CATALOG = {
        # 1. USER PROFILE
        'user': {
            'triggers': ['user_code', 'registration_date', 'kyc_status', 'register_country', 'inviter_user_code'],
            'dimensions': [
                {'label': 'By Country', 'prompt': 'Break down by country'},
                {'label': 'By KYC', 'prompt': 'Break down by KYC status'},
                {'label': 'By Terminal', 'prompt': 'Break down by terminal type'},
                {'label': 'Show VIPs', 'prompt': 'Filter for VIP users only'}
            ]
        },
        # 2. TRADES
        'trade': {
            'triggers': ['deal_amount', 'trade_volume', 'fee', 'symbol', 'side', 'order_id'],
            'dimensions': [
                {'label': 'By Symbol', 'prompt': 'Break down by symbol'},
                {'label': 'By Side', 'prompt': 'Break down by buy/sell side'},
                {'label': 'By Market', 'prompt': 'Break down by market type (spot/futures)'},
                {'label': 'Top Traders', 'prompt': 'Show top 10 users by volume'}
            ]
        },
        # 3. DEPOSITS
        'deposit': {
            'triggers': ['deposit_amount', 'tx_hash', 'chain', 'token', 'address'],
            'dimensions': [
                {'label': 'By Token', 'prompt': 'Break down by token'},
                {'label': 'By Chain', 'prompt': 'Break down by chain'},
                {'label': 'Large Only', 'prompt': 'Show deposits > 10000 USD'}
            ]
        },
        # 4. WITHDRAWALS
        'withdraw': {
            'triggers': ['withdraw_amount', 'dest_address', 'txid'],
            'dimensions': [
                {'label': 'By Token', 'prompt': 'Break down by token'},
                {'label': 'By Status', 'prompt': 'Break down by status'},
                {'label': 'High Value', 'prompt': 'Show withdrawals > 5000 USD'}
            ]
        },
        # 5. RISK / FRAUD
        'risk': {
            'triggers': ['risk_score', 'reason', 'blocked', 'rule_id'],
            'dimensions': [
                {'label': 'By Reason', 'prompt': 'Break down by risk reason'},
                {'label': 'Top Risky', 'prompt': 'Show top 10 users by risk score'},
                {'label': 'Blocked Only', 'prompt': 'Show only blocked users'}
            ]
        },
        # 6. LOGIN
        'login': {
            'triggers': ['login_ip', 'device_id', 'os', 'browser'],
            'dimensions': [
                {'label': 'By OS', 'prompt': 'Break down by OS'},
                {'label': 'By Browser', 'prompt': 'Break down by browser'},
                {'label': 'Success Rate', 'prompt': 'Show login success rate'}
            ]
        },
        # 7. INVITE / REFERRAL
        'invite': {
            'triggers': ['invite_code', 'commission', 'rebate'],
            'dimensions': [
                {'label': 'Top Referrers', 'prompt': 'Show top 10 referrers by volume'},
                {'label': 'By Level', 'prompt': 'Break down by rebate level'}
            ]
        },
        # 8. BALANCE / ASSETS
        'balance': {
            'triggers': ['balance', 'available', 'frozen', 'asset', 'wallet'],
            'dimensions': [
                {'label': 'By Asset', 'prompt': 'Break down by asset'},
                {'label': 'Whales', 'prompt': 'Show top 10 users by total balance'}
            ]
        }
    }

    @staticmethod
    def generate(df: pd.DataFrame, user_msg: str) -> list:
        if df is None or df.empty:
            return []

        suggestions = []
        cols = [str(c).lower() for c in df.columns]
        
        # --- 1. DETECT ACTIVE DOMAIN ---
        active_dimensions = []
        for domain, config in SuggestionAgent.CATALOG.items():
            # If ANY trigger column is present, we are in this domain
            if any(trigger in cols for trigger in config['triggers']):
                active_dimensions.extend(config['dimensions'])

        # --- 2. TIME INTELLIGENCE ---
        # Detect if we are ALREADY looking at a time trend
        time_cols = ['ds', 'date', 'day', 'month', 'time', 'hour', 'minute', 'registration_date_only', 'trade_date']
        is_already_trend = any(t in cols for t in time_cols)
        
        if is_already_trend:
            # If already daily, suggest aggregations or forecasts
            suggestions.append({'label': 'Weekly View', 'prompt': 'Group by week'})
            suggestions.append({'label': 'Cumulative', 'prompt': 'Show cumulative total'})
            if len(df) > 10:
                 suggestions.append({'label': 'Last 7 Days', 'prompt': 'Filter for last 7 days'})
        else:
            # If it's a snapshot (Total Volume), suggest Trending
            suggestions.append({'label': 'Daily Trend', 'prompt': 'Show the daily trend'})
            suggestions.append({'label': 'Last 30 Days', 'prompt': 'Show trend for last 30 days'})

        # --- 3. DIMENSION INTELLIGENCE ---
        # Suggest breakdowns, but ONLY if that column isn't already there
        # e.g. If columns=['country', 'count'], DO NOT suggest "By Country"
        
        added_count = 0
        for dim in active_dimensions:
            # Extract key word (e.g. "Country" from "By Country")
            keyword = dim['label'].replace("By ", "").lower()
            
            # Special handling for "Side" (Buy/Sell)
            if keyword == "side" and "side" in cols: continue
            
            # General check: is this dimension likely already in the result?
            if keyword not in cols and f"{keyword}_code" not in cols and f"{keyword}_name" not in cols:
                # Deduplicate: Don't add if we already have this label
                if not any(s['label'] == dim['label'] for s in suggestions):
                    suggestions.append(dim)
                    added_count += 1
            
            if added_count >= 3: break 

        # --- 4. DATA SHAPE RULES ---
        # If result is huge (>100 rows), suggest "Top 10"
        if len(df) > 50 and not any("top" in s['label'].lower() for s in suggestions):
            suggestions.append({'label': 'Top 10', 'prompt': 'Keep only the top 10'})

        # Limit to 4 chips max to keep UI clean
        return suggestions[:4]