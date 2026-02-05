# app/cubes/user_profile.py

# 1. Identity
NAME = "User Profile Cube"
DESCRIPTION = "Contains daily snapshots of user profiles, lifetime stats, risk scores, and current balances."

# 2. DDL (The Structure)
# Note: Copied exactly from your validated schema.
DDL = """
CREATE TABLE public.user_profile_360 (
    -- IDENTITY
    user_code BIGINT,           -- Unique User ID
    email TEXT,
    country TEXT,
    
    -- DATES
    registration_date TIMESTAMPTZ,
    registration_date_only DATE, -- Use for daily registration counts (NRU)
    first_deposit_date TIMESTAMPTZ, -- FTD Date. If NOT NULL, user is a depositor.
    
    -- STATUS FLAGS (1=Yes, 0=No)
    is_active_user_7d BIGINT,   -- Logged in last 7 days
    is_active_trader_7d BIGINT, -- Traded last 7 days
    is_good_user BIGINT,        -- Low risk
    kyc_status_desc TEXT,       -- e.g. 'Basic', 'Advanced'
    
    -- SEGMENTS
    user_segment TEXT,          -- 'VIP', 'High Value', 'Medium Value', 'Low Value'
    lifecycle_stage TEXT,       -- 'Acquisition', 'Active', 'Churned'
    
    -- LIFETIME METRICS (Accumulated totals as of 'ds')
    total_trade_volume DECIMAL,    -- All-time Volume
    total_deposit_volume DECIMAL,  -- All-time Deposit
    total_withdraw_volume DECIMAL, -- All-time Withdrawals
    total_net_fees DECIMAL,        -- All-time Revenue
    total_wallet_balance DECIMAL,  -- Current Balance (Available + Frozen)
    
    -- REFERRALS
    inviter_user_code BIGINT,      -- Who invited them (NULL if none)
    total_direct_referrals BIGINT, -- How many people they invited
    
    -- PARTITION
    ds TEXT                        -- Date Partition 'YYYYMMDD' (e.g. '20260118')
);
"""

# 3. Documentation (The Semantics)
# This replaces 'COLUMN_DEFINITIONS' and adds the "Critical Rules".
DOCS = """
**Table Purpose:**
This table is a DAILY SNAPSHOT. Each row represents a user's state on a specific day (`ds`).

**Critical SQL Rules:**
1. **The 'Yesterday' Rule (Partitioning):** - You MUST filter by `ds = '{latest_ds}'` for ANY question about "current" status (e.g., "total users", "current balance").
   - Do NOT scan all partitions unless the user explicitly asks for "History" or "Trend".
2. **Forbidden Tables:** Do NOT hallucinate. You can ONLY query `public.user_profile_360`.

**Column Definitions:**
- **Volume**: Use `total_trade_volume`.
- **Deposit**: Use `total_deposit_volume`.
- **Revenue**: Use `total_net_fees`.
- **Active User**: Use `is_active_user_7d = 1` (People who logged in).
- **Active Trader**: Use `is_active_trader_7d = 1` (People who actually traded).
- **Depositor**: `first_deposit_date IS NOT NULL`.
- **Referral User**: `inviter_user_code IS NOT NULL`.

**Acronyms & Business Terms:**
- **NRU (New Registration Users)**: Count of users where `registration_date_only` equals the target date.
- **FTD (First Time Depositors)**: Count of users where `DATE(first_deposit_date)` equals the target date.
- **KYC Status**: Always group by `kyc_status_desc` (e.g., 'Basic'), never by numeric codes.

**Segments (Exact Spelling):**
- `user_segment`: 'VIP', 'High Value', 'Medium Value', 'Low Value', 'Depositor Only'
"""

# 4. Training Examples (The Patterns)
# Vanna uses these to learn "How to write SQL" for this specific schema.
# Note: We use {latest_ds} placeholder which the System Prompt will fill in at runtime.
EXAMPLES = [
    {
        "question": "Show me KYC status breakdown.",
        "sql": "SELECT kyc_status_desc, COUNT(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' GROUP BY kyc_status_desc;"
    },
    {
        "question": "How many new registrations (NRU) did we have yesterday?",
        "sql": "SELECT COUNT(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND registration_date_only = '{latest_ds_dash}';"
    },
    {
        "question": "Show me the trend of daily registrations for December 2025.",
        "sql": "SELECT registration_date_only, COUNT(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND registration_date_only BETWEEN '2025-12-01' AND '2025-12-31' GROUP BY registration_date_only ORDER BY registration_date_only;"
    },
    {
        "question": "How many active traders do we have?",
        "sql": "SELECT count(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND is_active_trader_7d = 1;"
    },
    {
        "question": "List top 5 VIP users by volume.",
        "sql": "SELECT user_code, email, total_trade_volume FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND user_segment = 'VIP' ORDER BY total_trade_volume DESC LIMIT 5;"
    },
    {
        "question": "Count FTD users for Jan 1st 2026.",
        "sql": "SELECT COUNT(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND DATE(first_deposit_date) = '2026-01-01';"
    },
    # CASE 2: Population Ratios (Single Row)
    {
        "question": "How many users we have? Show ratio of deposit, trade and withdraw.",
        "sql": """
        SELECT 
            COUNT(user_code) as total_users,
            ROUND(COUNT(CASE WHEN first_deposit_date IS NOT NULL THEN 1 END) * 100.0 / COUNT(user_code), 2) as deposit_ratio,
            ROUND(COUNT(CASE WHEN is_active_trader_7d = 1 THEN 1 END) * 100.0 / COUNT(user_code), 2) as active_trader_ratio,
            ROUND(SUM(CASE WHEN total_withdraw_volume > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(user_code), 2) as withdraw_ratio
        FROM public.user_profile_360 
        WHERE ds = '{latest_ds}';
        """
    },

    # CASE 3: DAU (Yesterday) & MAU (Monthly)
    # Strategy: Scan the whole month for MAU to get true unique counts
    {
        "question": "What is DAU for yesterday and MAU for January 2026?",
        "sql": """
        SELECT 
            (SELECT COUNT(DISTINCT user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND is_active_user_7d = 1) as DAU_Yesterday,
            (SELECT COUNT(DISTINCT user_code) FROM public.user_profile_360 WHERE ds BETWEEN '20260101' AND '20260131' AND is_active_user_7d = 1) as MAU_January
        """
    },

    # CASE 4: User Acquisition Funnel (Long Format for Charting)
    # Strategy: Use UNION ALL to create a format Plotly understands easily
    {
        "question": "Generate a user acquisition funnel for the past week.",
        "sql": """
        SELECT '1_Browsing' as stage, COUNT(DISTINCT user_code) as user_count FROM public.dwd_user_device_log_di WHERE ds >= '{latest_ds}'::DATE - INTERVAL '7 days'
        UNION ALL
        SELECT '2_Registration', COUNT(DISTINCT user_code) FROM public.user_profile_360 WHERE registration_date >= NOW() - INTERVAL '7 days' AND ds = '{latest_ds}'
        UNION ALL
        SELECT '3_Login', COUNT(DISTINCT user_code) FROM public.user_profile_360 WHERE is_active_user_7d = 1 AND ds = '{latest_ds}'
        UNION ALL
        SELECT '4_Deposit', COUNT(DISTINCT user_code) FROM public.user_profile_360 WHERE first_deposit_date >= NOW() - INTERVAL '7 days' AND ds = '{latest_ds}'
        UNION ALL
        SELECT '5_Trading', COUNT(DISTINCT user_code) FROM public.user_profile_360 WHERE is_active_trader_7d = 1 AND ds = '{latest_ds}'
        ORDER BY stage;
        """
    }
]