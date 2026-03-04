# app/cubes/transaction_detail.py

NAME = "Transaction Detail Cube"
DESCRIPTION = "Granular deposit and withdrawal records. Use for audit, fees, wallet checks, and net flow analysis."
HAS_TIME_FIELD = True
TIME_COLUMN = "create_at"
KIND = "di"
# 1. DDL
DDL = """
CREATE TABLE public.dws_user_deposit_withdraw_detail_di (
    user_code STRING,
    type STRING,                -- 'deposit' or 'withdraw'
    transaction_code STRING,    -- Internal ID
    transaction_id STRING,      -- External/Chain ID
    
    -- CRYPTO DETAILS
    coin STRING,                -- e.g. BTC, USDT
    chain STRING,               -- e.g. Ethereum, TRC20
    wallet_address STRING,
    transfer_hash STRING,
    
    -- FINANCIALS
    amount DECIMAL(38,18),      -- Requested Amount
    real_amount DECIMAL(38,18), -- Actual Amount Received
    fee_amount DECIMAL(38,18),
    fee_rate DECIMAL(38,18),
    is_bonus BIGINT,            -- 1=Bonus, 0=Regular
    
    -- STATUS & CLASSIFICATION
    audit_status_desc STRING,   -- e.g. 'Completed', 'Rejected'
    account_kind_desc STRING,
    
    -- TIMESTAMPS
    create_at DATETIME,
    update_at DATETIME,
    
    -- PARTITION
    ds STRING                   -- Date Partition 'YYYYMMDD'
);
"""

# 2. Documentation
# 2. Documentation
DOCS = """
**Table Purpose:**
Transaction-level log of all money entering or leaving the platform.
- **Granularity:** One row per transaction.
- **Key Columns:** `type` ('deposit'/'withdraw'), `coin`, `real_amount`, `chain`.

** ⚡ AGGREGATION WARNING (CRITICAL):**
- **DO NOT** use this table to calculate "Total Lifetime Deposits", "Net Deposits", or "Withdrawal Ratios". 
- Those metrics are pre-calculated in `user_profile_360` (e.g., `total_deposit_volume`, `net_deposit_amount`).
- **Use this table ONLY for:** Raw transaction lists, querying specific coins/chains (e.g., "USDT TRC20 deposits"), or specific time-window aggregates (e.g., "Deposits last 7 days").

**Partitioning Rule (`_di` table):**
- `ds = '{latest_ds}'` ONLY gives you transactions that happened on that exact day.
- To get history, you MUST use `ds BETWEEN '{start_7d}' AND '{latest_ds}'` or similar range filters.
"""

# 3. Training Examples (Dynamic Date)
EXAMPLES = [
    {
        "question": "Analyze fees per coin for withdrawals.",
        "sql": """
        SELECT user_code, coin,
               SUM(fee_amount) AS total_fee,
               AVG(fee_rate) AS avg_fee_rate
        FROM public.dws_user_deposit_withdraw_detail_di
        WHERE ds = '{latest_ds}' AND type = 'withdraw'
        GROUP BY user_code, coin
        ORDER BY total_fee DESC;
        """
    },
    {
        "question": "Find transactions with unusual amounts (outliers).",
        "sql": "SELECT * FROM public.dws_user_deposit_withdraw_detail_di WHERE ds = '{latest_ds}' AND (amount > 10000 OR amount < 0.01) ORDER BY amount DESC;"
    },
    {
        "question": "List wallets with multiple transactions on the same chain.",
        "sql": """
        SELECT chain, wallet_address, COUNT(*) AS tx_count, SUM(amount) AS total_amount
        FROM public.dws_user_deposit_withdraw_detail_di
        WHERE ds = '{latest_ds}'
        GROUP BY chain, wallet_address
        HAVING COUNT(*) > 1
        ORDER BY total_amount DESC;
        """
    },
    {
        # Changed "matches" to "does not match"
        "question": "Show discrepancies where real amount does not match requested amount.",
        "sql": """
        SELECT user_code, type, amount, real_amount, (amount - real_amount) AS discrepancy
        FROM public.dws_user_deposit_withdraw_detail_di
        WHERE ds = '{latest_ds}' AND amount <> real_amount
        ORDER BY discrepancy DESC;
        """
    },
    # ADD THESE TO THE EXAMPLES LIST
    {
        "question": "Show all USDT deposits on the TRC20 chain for the last 7 days.",
        "sql": """
        SELECT user_code, transaction_id, real_amount, create_at 
        FROM public.dws_user_deposit_withdraw_detail_di 
        WHERE ds >= '{start_7d}' 
          AND type = 'deposit' 
          AND coin = 'USDT' 
          AND chain = 'TRC20'
        ORDER BY create_at DESC;
        """
    },
    {
        "question": "What was the total withdrawal fee collected yesterday?",
        "sql": """
        SELECT coin, SUM(fee_amount) as total_fees_yesterday
        FROM public.dws_user_deposit_withdraw_detail_di 
        WHERE ds = '{latest_ds}' 
          AND type = 'withdraw'
        GROUP BY coin;
        """
    },
    {
        "question": "Show the daily trend of deposit amounts.",
        "sql": """
        SELECT ds AS report_date, SUM(real_amount) AS total_daily_deposit
        FROM public.dws_user_deposit_withdraw_detail_di
        WHERE type = 'deposit' AND ds >= '{start_30d}'
        GROUP BY ds
        ORDER BY ds ASC;
        """
    }
]