"""
metadata.py
Contains the Business Logic, Semantic Rules, and Gold Standard Examples.
This is the 'Brain' that guides the AI.
"""

# The "Truth" about your columns
COLUMN_DEFINITIONS = {
    "volume": "total_trade_volume (Lifetime total)",
    "deposit": "total_deposit_volume (Lifetime total)",
    "active_user": "is_active = 1 (Account status active)",
    "kyc": "kyc_status (1=Basic, 2=Advanced)",
    "revenue": "total_net_fees (Fees after rebates)",
    "risk": "max_risk_score (0.0 to 1.0)",
    "wallet_balance": "total_wallet_balance (Available + Frozen)"
}

# 5 "Gold Standard" Examples. 
# These teach the AI how to query the Cube correctly (filtering by DS!).
FEW_SHOT_EXAMPLES = """
**Example 1: Simple Count (Current State)**
User: "How many active users do we have?"
SQL: 
SELECT count(user_code) 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND is_active = 1;

**Example 2: Top N Ranking (Current State)**
User: "Show me the top 5 users by total trading volume."
SQL: 
SELECT user_code, email, country, total_trade_volume 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
ORDER BY total_trade_volume DESC 
LIMIT 5;

**Example 3: Aggregation by Category (Current State)**
User: "What is the total deposit volume by country?"
SQL: 
SELECT country, SUM(total_deposit_volume) as total_dep 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
GROUP BY country 
ORDER BY total_dep DESC;

**Example 4: Historical Trend (Time Series)**
User: "Show me the trend of new registrations over the last 7 days."
SQL: 
SELECT ds, COUNT(user_code) as new_users
FROM public.user_profile_360
WHERE ds >= TO_CHAR(CURRENT_DATE - INTERVAL '7 days', 'YYYYMMDD')
GROUP BY ds
ORDER BY ds ASC;

**Example 5: Complex Filtering**
User: "List VIP users (High Value) who registered this month."
SQL: 
SELECT user_code, email, registration_date, user_segment 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND user_segment LIKE '%High Value%'
AND registration_date >= DATE_TRUNC('month', CURRENT_DATE);
"""

def get_metadata_context(yesterday_ds: str) -> str:
    """
    Returns the formatted block to inject into the Prompt.
    """
    return f"""
    **BUSINESS LOGIC & RULES:**
    1. **The 'Yesterday' Rule:** This data is a DAILY SNAPSHOT. You MUST filter by `ds = '{yesterday_ds}'` for ANY question about "current" status (e.g. current balance, total users).
    2. **History Exception:** Only remove the `ds` filter if the user explicitly asks for "history", "trend", or "over time".
    3. **Active Users:** Use column `is_active` = 1.
    4. **Volume:** Use `total_trade_volume`.
    5. **Deposits:** Use `total_deposit_volume`.
    
    **KEY COLUMN DEFINITIONS:**
    {COLUMN_DEFINITIONS}
    
    **CORRECT SQL EXAMPLES (Study these patterns):**
    {FEW_SHOT_EXAMPLES.format(yesterday_ds=yesterday_ds)}
    """