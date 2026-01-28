import sys
import os

# Add the project root to python path so we can import 'app'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.vanna_wrapper import vn

# 1. The DDL (The Structure)
# We use the EXACT same DDL we refined earlier
user_cube_ddl = """
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
    total_net_fees DECIMAL,        -- All-time Revenue
    total_wallet_balance DECIMAL,  -- Current Balance (Available + Frozen)
    
    -- REFERRALS
    inviter_user_code BIGINT,      -- Who invited them (NULL if none)
    total_direct_referrals BIGINT, -- How many people they invited
    
    -- PARTITION
    ds TEXT                        -- Date Partition 'YYYYMMDD' (e.g. '20260118')
);
"""

# 2. The Documentation (The Semantics)
# This teaches Vanna the "Business Logic" we defined in metadata.py
business_rules = """
**Definition of Terms:**
- **Volume**: Use column `total_trade_volume`.
- **Deposit**: Use column `total_deposit_volume`.
- **Active User**: Use `is_active_user_7d = 1`.
- **Active Trader**: Use `is_active_trader_7d = 1`.
- **NRU (New Registration Users)**: Filter by `registration_date_only`.
- **FTD (First Time Depositors)**: Filter by `DATE(first_deposit_date)`.
- **Referral User**: Users where `inviter_user_code IS NOT NULL`.

**Important Rules:**
- **The 'Yesterday' Rule**: This is a SNAPSHOT table. You MUST filter by `ds = '{latest_ds}'` for any "current" status question (like "how many users do we have?").
- **Date Filtering**: For history trends, filter `registration_date_only` or `first_deposit_date`, but KEEP the `ds` filter locked to the latest partition to avoid duplicates.
"""

# 3. Gold Standard SQL (The Examples)
# This teaches Vanna correct syntax patterns
sql_examples = [
    {
        "question": "How many active traders do we have?",
        "sql": "SELECT count(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND is_active_trader_7d = 1;"
    },
    {
        "question": "Show me the trend of daily registrations for December 2025.",
        "sql": "SELECT registration_date_only, COUNT(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND registration_date_only BETWEEN '2025-12-01' AND '2025-12-31' GROUP BY registration_date_only ORDER BY registration_date_only;"
    },
    {
        "question": "List top 5 VIP users by volume.",
        "sql": "SELECT user_code, email, total_trade_volume FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND user_segment = 'VIP' ORDER BY total_trade_volume DESC LIMIT 5;"
    }
]

def train():
    print("🚀 Starting Vanna Training...")
    
    # A. Train DDL
    print(f"Training DDL... ({len(user_cube_ddl)} chars)")
    vn.train(ddl=user_cube_ddl)
    
    # B. Train Documentation
    print(f"Training Documentation... ({len(business_rules)} chars)")
    vn.train(documentation=business_rules)
    
    # C. Train SQL Examples
    print(f"Training {len(sql_examples)} SQL Examples...")
    for ex in sql_examples:
        vn.train(question=ex["question"], sql=ex["sql"])
        
    print("✅ Training Complete! The 'Brain' is ready in /vanna_storage")

if __name__ == "__main__":
    train()