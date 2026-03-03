# app/cubes/user_profile.py

# 1. Identity
NAME = "User Profile Cube"
DESCRIPTION = "Contains daily snapshots of user profiles, lifetime stats, risk scores, and current balances."
HAS_TIME_FIELD = True
TIME_COLUMN = "registration_date_only"
KIND = "df"
# 2. DDL (The Structure)
# Note: Copied exactly from your validated schema.
# 1. Update this block in app/cubes/user_profile.py
DDL = """
CREATE TABLE public.user_profile_360 (
    -- IDENTITY & LOCATION
    user_code BIGINT, email TEXT, country TEXT, city TEXT,
    parent_code BIGINT, inviter_user_code BIGINT, root_user_code BIGINT,
    
    -- DATES & LIFECYCLE
    registration_date TIMESTAMPTZ, registration_date_only DATE, days_since_registration BIGINT,
    first_login_date TIMESTAMPTZ, last_login_date TIMESTAMPTZ, days_since_last_login BIGINT,
    lifecycle_stage TEXT, user_segment TEXT, trading_profile TEXT,
    
    -- KYC & COMPLIANCE
    kyc_status BIGINT, kyc_status_desc TEXT, kyc_audit_status BIGINT, 
    kyc_audit_status_desc TEXT, kyc_audit_date TIMESTAMPTZ,
    
    -- RISK & DEVICE (Fraud Detection)
    is_active BIGINT, withdraw_enabled BIGINT, fund_enabled BIGINT,
    last_device_model TEXT, last_os TEXT, last_browser TEXT,
    first_device_model TEXT, first_os TEXT, first_browser TEXT,
    has_used_vpn BIGINT, has_used_proxy BIGINT, has_bot_detected BIGINT, 
    has_emulator_detected BIGINT, has_rooted_device BIGINT, has_browser_tampering BIGINT, 
    max_risk_score DOUBLE, is_good_user BIGINT,
    
    -- DEPOSITS & WITHDRAWALS
    first_deposit_date TIMESTAMPTZ, first_deposit_amount DOUBLE, first_deposit_coin TEXT,
    days_to_first_deposit BIGINT, has_ftd BIGINT, is_eftd BIGINT, eftd_date TIMESTAMPTZ, eftd_amount DOUBLE,
    lifetime_deposit_count BIGINT, lifetime_deposit_amount DOUBLE, last_deposit_date TIMESTAMPTZ,
    last_deposit_datetime TIMESTAMPTZ, days_since_last_deposit BIGINT,
    total_deposit_volume DOUBLE, total_deposit_txns BIGINT, total_withdraw_volume DOUBLE, 
    total_withdraw_txns BIGINT, total_withdraw_fees DOUBLE, net_deposit_amount DOUBLE, 
    crypto_deposit_volume DOUBLE, crypto_withdraw_volume DOUBLE, net_deposit_ratio DOUBLE,
    total_withdraw_attempts BIGINT, successful_withdrawals BIGINT, rejected_withdrawals BIGINT, 
    approved_withdraw_volume DOUBLE, total_withdraw_fee_paid DOUBLE, crypto_withdraw_count BIGINT, 
    fiat_withdraw_count BIGINT, withdraw_success_rate DOUBLE, last_successful_withdraw_date TIMESTAMPTZ,
    last_withdraw_datetime TIMESTAMPTZ, days_since_last_withdraw BIGINT,
    
    -- TRADING & VOLUMES (Spot vs Futures)
    first_trade_date TIMESTAMPTZ, first_trade_market TEXT, first_trade_symbol TEXT, days_to_first_trade BIGINT, has_ftt BIGINT,
    first_futures_trade_date TIMESTAMPTZ, first_futures_symbol TEXT, has_futures_ftt BIGINT,
    first_spot_trade_date TIMESTAMPTZ, first_spot_symbol TEXT, has_spot_ftt BIGINT,
    milestone_trade_count BIGINT, milestone_futures_count BIGINT, milestone_spot_count BIGINT, 
    milestone_total_volume DOUBLE, milestone_last_trade_date TIMESTAMPTZ,
    total_trade_volume DOUBLE, total_trade_count BIGINT, total_trading_fees DOUBLE, total_net_fees DOUBLE, 
    total_rebates DOUBLE, futures_trade_volume DOUBLE, futures_trade_count BIGINT, futures_fees DOUBLE, 
    spot_trade_volume DOUBLE, spot_trade_count BIGINT, spot_fees DOUBLE, most_traded_symbol TEXT, 
    avg_trade_size DOUBLE, avg_futures_trade_size DOUBLE, avg_spot_trade_size DOUBLE,
    last_trade_datetime TIMESTAMPTZ, last_futures_trade_datetime TIMESTAMPTZ, last_spot_trade_datetime TIMESTAMPTZ,
    days_since_last_trade BIGINT, is_active_trader_7d BIGINT, is_active_trader_30d BIGINT, 
    is_active_user_7d BIGINT, is_active_user_30d BIGINT, avg_trades_per_day DOUBLE,
    
    -- BALANCES & PNL
    total_available_balance DOUBLE, total_frozen DOUBLE, total_manually_frozen DOUBLE, 
    total_wallet_balance DOUBLE, fund_account_balance DOUBLE, spot_account_balance DOUBLE, 
    bot_account_balance DOUBLE, contract_account_balance DOUBLE, active_currency_count BIGINT, 
    open_position_count BIGINT, long_position_count BIGINT, short_position_count BIGINT, 
    total_position_value DOUBLE, total_init_margin DOUBLE, total_maintain_margin DOUBLE, 
    total_extra_margin DOUBLE, total_realized_pnl DOUBLE, avg_leverage DOUBLE, max_leverage DOUBLE,
    estimated_ltv DOUBLE, return_on_equity DOUBLE, wallet_retention_ratio DOUBLE,
    
    -- REBATES & NETWORK
    rebate_from_own_trading DOUBLE, own_trading_rebate_count BIGINT, total_rebate_earned DOUBLE, 
    rebate_distributed DOUBLE, rebate_pending DOUBLE, last_rebate_distribution_date TIMESTAMPTZ, 
    total_rebate_count BIGINT, total_network_size BIGINT, total_direct_referrals BIGINT, 
    rebate_earning_direct_refs_l1 DOUBLE, rebate_earning_direct_refs_l2 DOUBLE, rebate_earning_direct_refs_total DOUBLE, 
    unique_rebate_earning_direct_refs BIGINT, rebate_from_level_1 DOUBLE, rebate_from_level_2 DOUBLE, 
    rebate_from_referrals_total DOUBLE, referral_rebate_count BIGINT, is_referrer BIGINT, was_referred BIGINT, 
    last_referral_rebate_date TIMESTAMPTZ,
    
    -- PARTITION
    ds TEXT
);
"""

# 3. Documentation (The Semantics)
# This replaces 'COLUMN_DEFINITIONS' and adds the "Critical Rules".
DOCS = """
**Table Purpose:**
This table is a DAILY SNAPSHOT. Each row represents a user's state on a specific day (`ds`).

** ⛔ SCOPE & LIMITATIONS (CRITICAL):**
1. **NO BLACKLIST DATA:** This table does NOT contain banned/blocked/blacklisted users. 
   - For "Blacklist", "Banned", or "Risk" queries, you **MUST** use the `risk_campaign_blacklist` table.
2. **NO TRANSACTIONS:** This table only has *aggregate totals*. For raw deposit/trade lists, use the `dws_` transaction tables.

** ⚡ PERFORMANCE & JOINS (CRITICAL):**
- **PRE-CALCULATED TOTALS:** This table ALREADY contains lifetime volumes (`total_trade_volume`, `total_deposit_volume`).
  - **Rule:** If user asks for "Total Volume" or "Lifetime Volume", use `total_trade_volume`. **DO NOT JOIN** the trades table (`dws_all_trades_di`) unless the user explicitly asks for a specific date range (e.g., "Volume last 7 days").

**Critical SQL Rules:**
1. **The 'Yesterday' Rule (Partitioning):** - You MUST filter by `ds = '{latest_ds}'` for ANY question about "current" status (e.g., "total users", "current balance").
   - **The Default:** If the user does NOT specify a timeframe, ALWAYS default to `ds = '{latest_ds}'` (this represents the latest available data).
   - Do NOT scan all partitions unless the user explicitly asks for "History" or "Trend".

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

**New Acronyms & Flags (1=True, 0=False):**
- **FTT (First Time Trader)**: `has_ftt = 1`
- **EFTD (Effective First Time Depositor)**: High-quality depositor `is_eftd = 1`.
- **Risk/Fraud Flags**: `has_used_vpn`, `has_used_proxy`, `has_bot_detected`, `has_emulator_detected`, `has_rooted_device`. Use these directly to find suspicious users.
- **LTV**: `estimated_ltv` (Lifetime Value).
- **ROE**: `return_on_equity`.

**New Granularity:**
- Trading and volume are now split into **Spot** (`spot_trade_volume`) and **Futures** (`futures_trade_volume`). 
- **Balances** are split across `fund`, `spot`, `bot`, and `contract` accounts.
- **Active Users**: Can now be queried for 7 days (`is_active_trader_7d`) or 30 days (`is_active_trader_30d`).

**Snapshot Trend Exception:**
- Normally, you only query `ds = '{latest_ds}'`.
- HOWEVER, if the user explicitly asks for the "daily trend" of a snapshot state (e.g., "trend of active traders over time"), you MAY query a range of `ds` partitions.
- **CRITICAL:** If you do this, you MUST aggregate the data using `GROUP BY ds` and `COUNT(CASE WHEN...)` or `SUM()`. NEVER select raw user rows across multiple days.
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
        "question": "How many new registrations (NRU) did we have?",
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
    {
        "question": "What is the DAU, WAU, and MAU?",
        "sql": """
        SELECT 
            COUNT(CASE WHEN days_since_last_login = 0 THEN 1 END) as DAU,
            COUNT(CASE WHEN is_active_user_7d = 1 THEN 1 END) as WAU,
            COUNT(CASE WHEN is_active_user_30d = 1 THEN 1 END) as MAU
        FROM public.user_profile_360 
        WHERE ds = '{latest_ds}';
        """
    },
    {
        "question": "Generate a user acquisition funnel for the past week.",
        "sql": """
        SELECT '1_Browsing' as stage, COUNT(DISTINCT user_code) as user_count FROM public.dwd_user_device_log_di WHERE ds >= '{start_7d}'
        UNION ALL
        SELECT '2_Registration', COUNT(DISTINCT user_code) FROM public.user_profile_360 WHERE registration_date_only >= '{start_7d_dash}' AND ds = '{latest_ds}'
        UNION ALL
        SELECT '3_Login', COUNT(DISTINCT user_code) FROM public.user_profile_360 WHERE is_active_user_7d = 1 AND ds = '{latest_ds}'
        ORDER BY stage;
        """
    },
    {
        "question": "Sum of trading volume for these users (users invited by 10000047).",
        "sql": "SELECT SUM(total_trade_volume) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND inviter_user_code = 10000047;"
    },
    # Add this to the EXAMPLES list in app/cubes/user_profile.py
    {
        "question": "Compare daily new registered users (NRU) for this month vs last month.",
        "sql": """
        SELECT 'This Month' as period, registration_date_only, COUNT(user_code) as nru_count
        FROM public.user_profile_360
        WHERE ds = '{latest_ds}' AND registration_date_only >= '{start_this_month}'
        GROUP BY 1, 2
        UNION ALL
        SELECT 'Last Month' as period, registration_date_only, COUNT(user_code) as nru_count
        FROM public.user_profile_360
        WHERE ds = '{latest_ds}' AND registration_date_only BETWEEN '{start_last_month}' AND '{end_last_month}'
        GROUP BY 1, 2
        ORDER BY period, registration_date_only;
        """
    },
    # Add this to the end of the EXAMPLES list in app/cubes/user_profile.py
    {
        "question": "Calculate the total trade volume for each VIP partner including their referral network community volume",
        "sql": """
        SELECT 
            up.user_code AS partner_id,
            up.email,
            COALESCE(ref.total_community_volume, up.total_trade_volume) AS total_network_volume
        FROM public.user_profile_360 up
        LEFT JOIN public.ads_total_root_referral_volume_df ref 
            ON up.user_code::TEXT = ref.root_user_code::TEXT 
            AND ref.ds = '{latest_ds}'
        WHERE up.ds = '{latest_ds}' 
          AND up.user_segment = 'VIP'
        ORDER BY total_network_volume DESC;
        """
    },
    {
        "question": "Find high risk users who used a VPN or emulator and have a risk score above 0.8.",
        "sql": """
        SELECT user_code, email, max_risk_score, has_used_vpn, has_emulator_detected
        FROM public.user_profile_360 
        WHERE ds = '{latest_ds}' 
          AND (has_used_vpn = 1 OR has_emulator_detected = 1) 
          AND max_risk_score > 0.8;
        """
    },
    {
        "question": "Show the daily trend of active traders (both 7-day and 30-day) and total trading volume for the past 14 days. Limit the results to the most recent 14 days and order by date descending.",
        "sql": """
        SELECT 
            ds,
            COUNT(CASE WHEN is_active_trader_7d = 1 THEN 1 END) AS active_traders_7d,
            COUNT(CASE WHEN is_active_trader_30d = 1 THEN 1 END) AS active_traders_30d,
            SUM(total_trade_volume) AS cumulative_lifetime_volume
        FROM public.user_profile_360
        WHERE ds >= TO_CHAR(TO_DATE('{latest_ds}', 'YYYYMMDD') - 14, 'YYYYMMDD')
        GROUP BY ds
        ORDER BY ds DESC;
        """
    },
    {
        "question": "Calculate the success rate of withdrawals for users with VIP segment status.",
        "sql": """
        SELECT 
            AVG(withdraw_success_rate) AS overall_vip_withdraw_success_rate
        FROM public.user_profile_360
        WHERE ds = '{latest_ds}' 
          AND user_segment = 'VIP';
        """
    }
    
]