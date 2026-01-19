"""
metadata.py
Contains the Business Logic, Semantic Rules, and Gold Standard Examples.
This is the 'Brain' that guides the AI.
"""

# The "Truth" about your columns (Updated from BI Documentation)
COLUMN_DEFINITIONS = {
    # 1. Volume & Finance
    "volume": "total_trade_volume (Lifetime total)",
    "deposit": "total_deposit_volume (Lifetime total)",
    "revenue": "total_net_fees (Fees after rebates)",
    "wallet_balance": "total_wallet_balance (Available + Frozen)",
    
    # 2. User Status & Definitions
    "active_user": "is_active_user_7d = 1 (Logged in last 7 days)",
    "active_trader": "is_active_trader_7d = 1 (Traded last 7 days)",
    "kyc": "kyc_status (0=None, 1=Basic, 2=Advanced)",
    "risk": "max_risk_score (0.0 to 1.0)",
    "good_user": "is_good_user = 1 (Clean user, low risk)",
    
    # 3. Segments (Use these exact strings in WHERE clauses)
    "segments": [
        "VIP", "High Value", "Medium Value", "Low Value", 
        "Depositor Only", "Registered Only"
    ],
    "lifecycle": [
        "Acquisition", "Onboarding", "Active", "At Risk", "Churned"
    ],
    "trading_profile": [
        "Futures Focused", "Spot Focused", "Mixed Trader", "Non Trader"
    ]
}

# 5 "Gold Standard" Examples. 
# These teach the AI how to query the Cube correctly (filtering by DS!).
FEW_SHOT_EXAMPLES = """
**Example 1: Active Users vs Active Traders (Current State)**
User: "How many active traders do we have?"
SQL: 
SELECT count(user_code) 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND is_active_trader_7d = 1;

**Example 2: Segmentation Analysis**
User: "Show me the breakdown of users by lifecycle stage."
SQL: 
SELECT lifecycle_stage, COUNT(user_code) as user_count
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
GROUP BY lifecycle_stage
ORDER BY user_count DESC;

**Example 3: High Value Query**
User: "List the top 5 'High Value' users by total volume."
SQL: 
SELECT user_code, email, country, total_trade_volume 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND user_segment = 'High Value'
ORDER BY total_trade_volume DESC 
LIMIT 5;

**Example 4: Risk Analysis**
User: "Who are the risky users with high balance?"
SQL: 
SELECT user_code, max_risk_score, total_wallet_balance
FROM public.user_profile_360
WHERE ds = '{yesterday_ds}'
AND is_good_user = 0
AND total_wallet_balance > 1000
ORDER BY max_risk_score DESC;

**Example 5: Referral Performance**
User: "Which users have referred the most people?"
SQL: 
SELECT user_code, total_direct_referrals, total_referral_commission
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND is_referrer = 1
ORDER BY total_direct_referrals DESC 
LIMIT 10;
"""

def get_metadata_context(yesterday_ds: str) -> str:
    """
    Returns the formatted block to inject into the Prompt.
    """
    return f"""
    **BUSINESS LOGIC & RULES:**
    1. **The 'Yesterday' Rule:** This data is a DAILY SNAPSHOT. You MUST filter by `ds = '{yesterday_ds}'` for ANY question about "current" status (e.g. current balance, total users).
    2. **Active Definition:** - If user asks for "Active Users", use `is_active_user_7d`.
       - If user asks for "Active Traders", use `is_active_trader_7d`.
    3. **Segments:** Use the exact string values provided below for `user_segment` or `lifecycle_stage`.
    
    **KEY COLUMN DEFINITIONS:**
    {COLUMN_DEFINITIONS}
    
    **CORRECT SQL EXAMPLES (Study these patterns):**
    {FEW_SHOT_EXAMPLES.format(yesterday_ds=yesterday_ds)}
    """