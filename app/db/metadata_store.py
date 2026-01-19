def get_ddl_context() -> str:
    """
    Returns the schema definition for the AI to understand the data structure.
    This is the ONLY source of truth for table names.
    """
    return """
    -- Table: User Profile 360 (The "User Cube")
    -- This is a DAILY SNAPSHOT table. 
    -- Granularity: One row per user_code per day (ds).
    -- KEY RULE: You MUST filter by 'ds' to get a specific day's state.
    
    CREATE TABLE public.user_profile_360 (
        -- IDENTITY
        user_code BIGINT,           -- Unique User ID
        email TEXT,
        country TEXT,
        
        -- DATES
        registration_date TIMESTAMPTZ,
        registration_date_only DATE, -- Use for daily registration counts
        first_deposit_date TIMESTAMPTZ, -- FTD Date. If NOT NULL, user is a depositor.
        
        -- STATUS FLAGS (1=Yes, 0=No)
        is_active_user_7d BIGINT,   -- Logged in recently
        is_active_trader_7d BIGINT, -- Traded recently
        is_good_user BIGINT,        -- Low risk
        kyc_status_desc TEXT,       -- e.g. 'Basic', 'Advanced'
        
        -- SEGMENTS
        user_segment TEXT,          -- 'VIP', 'High Value', 'Medium Value', 'Low Value'
        lifecycle_stage TEXT,       -- 'Acquisition', 'Active', 'Churned'
        
        -- LIFETIME METRICS (Accumulated totals as of 'ds')
        total_trade_volume DECIMAL,    -- All-time Volume
        total_deposit_volume DECIMAL,  -- All-time Deposit
        total_net_fees DECIMAL,        -- All-time Revenue
        total_wallet_balance DECIMAL,  -- Current Balance (Available + Frozen)
        
        -- REFERRALS
        inviter_user_code BIGINT,      -- Who invited them (NULL if none)
        total_direct_referrals BIGINT, -- How many people they invited
        
        -- PARTITION
        ds TEXT                        -- Date Partition 'YYYYMMDD' (e.g. '20260118')
    );
    """