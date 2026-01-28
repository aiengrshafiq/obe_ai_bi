# app/cubes/user_profile.py

NAME = "User Profile Cube"

DDL = """
CREATE TABLE public.user_profile_360 (
    user_code BIGINT,
    email TEXT,
    country TEXT,
    registration_date TIMESTAMPTZ,
    registration_date_only DATE, 
    first_deposit_date TIMESTAMPTZ,
    is_active_user_7d BIGINT,
    is_active_trader_7d BIGINT, 
    kyc_status_desc TEXT,
    user_segment TEXT,
    lifecycle_stage TEXT,
    total_trade_volume DECIMAL,
    total_deposit_volume DECIMAL,
    total_net_fees DECIMAL,
    total_wallet_balance DECIMAL,
    inviter_user_code BIGINT,
    ds TEXT -- Partition 'YYYYMMDD'
);
"""

DOCS = """
**Definitions:**
- **Volume**: Use `total_trade_volume`.
- **Deposit**: Use `total_deposit_volume`.
- **Active User**: `is_active_user_7d = 1`.
- **Active Trader**: `is_active_trader_7d = 1`.
- **NRU**: New Registration Users (Filter `registration_date_only`).
- **FTD**: First Time Depositors (Filter `DATE(first_deposit_date)`).

**Rules:**
- **Yesterday Rule**: ALWAYS filter by `ds = '{latest_ds}'` for current status.
- **Events**: To find events on a specific day, keep `ds` as latest and filter the specific date column.
"""

EXAMPLES = [
    {
        "question": "How many active traders?", 
        "sql": "SELECT count(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND is_active_trader_7d = 1;"
    },
    {
        "question": "Daily registrations trend Dec 2025", 
        "sql": "SELECT registration_date_only, COUNT(user_code) FROM public.user_profile_360 WHERE ds = '{latest_ds}' AND registration_date_only BETWEEN '2025-12-01' AND '2025-12-31' GROUP BY registration_date_only;"
    }
]