"""
metadata.py
Contains the Business Logic, Semantic Rules, and Gold Standard Examples.
This is the 'Brain' that guides the AI.
"""

# The "Truth" about your columns
COLUMN_DEFINITIONS = {
    # 1. Finance & Volume
    "volume": "total_trade_volume",
    "deposit": "total_deposit_volume",
    "revenue": "total_net_fees",
    "wallet_balance": "total_wallet_balance",
    
    # 2. User Definitions
    "active_user": "is_active_user_7d = 1 (Logged in last 7 days)",
    "active_trader": "is_active_trader_7d = 1 (Traded last 7 days)",
    "depositor": "first_deposit_date IS NOT NULL (Has made at least one deposit)",
    "referral_user": "inviter_user_code IS NOT NULL AND inviter_user_code != 0",
    
    # 3. Acronyms (CRITICAL FOR BI)
    "NRU": "New Registration Users (Filter: registration_date_only = Target Date)",
    "FTD": "First Time Depositors (Filter: DATE(first_deposit_date) = Target Date)",
    
    # 4. Grouping Preferences
    "kyc_status": "Use 'kyc_status_desc' for grouping/display (not the numeric code)",
    
    # 5. Segments
    "segments": ["VIP", "High Value", "Medium Value", "Low Value", "Depositor Only"],
}

# "Gold Standard" Examples to teach the Logic Patterns
FEW_SHOT_EXAMPLES = """
**Example 1: Snapshot Aggregation (The "Yesterday Rule")**
User: "Show me KYC status breakdown."
SQL: 
SELECT kyc_status_desc, COUNT(user_code) 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
GROUP BY kyc_status_desc;

**Example 2: Historical Events in Snapshot (The "Date Filter" Pattern)**
User: "How many new registrations (NRU) did we have yesterday?"
SQL: 
SELECT COUNT(user_code) 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND registration_date_only = '{yesterday_dash}';

**Example 3: Date Range Analysis**
User: "Show me the trend of daily registrations for December 2025."
SQL: 
SELECT registration_date_only, COUNT(user_code) 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND registration_date_only BETWEEN '2025-12-01' AND '2025-12-31'
GROUP BY registration_date_only
ORDER BY registration_date_only;

**Example 4: Referral Funnel (No Deposit)**
User: "How many users joined via referral but never deposited?"
SQL: 
SELECT COUNT(user_code) 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND inviter_user_code IS NOT NULL 
AND inviter_user_code != 0 
AND first_deposit_date IS NULL;

**Example 5: First Time Deposits (FTD)**
User: "Count FTD users for Jan 1st 2026."
SQL: 
SELECT COUNT(user_code) 
FROM public.user_profile_360 
WHERE ds = '{yesterday_ds}' 
AND DATE(first_deposit_date) = '2026-01-01';
"""

def get_metadata_context(yesterday_ds: str) -> str:
    # Helper to create YYYY-MM-DD version of the DS for date matching
    try:
        y_dash = f"{yesterday_ds[:4]}-{yesterday_ds[4:6]}-{yesterday_ds[6:]}"
    except:
        y_dash = "2026-01-01" # Fallback

    return f"""
    **BUSINESS LOGIC & RULES:**
    1. **The 'Yesterday' Rule:** ALWAYS filter by `ds = '{yesterday_ds}'` unless specifically asked for history comparison.
    2. **Event vs Snapshot:** To find "New Users" or "Events" on a specific day, do NOT change the `ds`. Keep `ds = '{yesterday_ds}'` and filter the specific column (e.g. `registration_date` or `first_deposit_date`).
    3. **Acronyms:** NRU = New Registrations. FTD = First Time Deposits.
    
    **KEY COLUMN DEFINITIONS:**
    {COLUMN_DEFINITIONS}
    
    **CORRECT SQL EXAMPLES:**
    {FEW_SHOT_EXAMPLES.format(yesterday_ds=yesterday_ds, yesterday_dash=y_dash)}
    """