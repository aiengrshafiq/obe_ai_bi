# app/cubes/transaction_detail.py

NAME = "Transaction Detail Cube"
DESCRIPTION = "Granular deposit and withdrawal records. Use for audit, fees, wallet checks, and net flow analysis."

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
DOCS = """
**Table Purpose:**
Transaction-level log of all money entering or leaving the platform.
- **Granularity:** One row per transaction.
- **Key Columns:** `type` ('deposit'/'withdraw'), `coin`, `real_amount`.

**Critical Logic:**
1. **Net Deposit:** Calculated as `SUM(deposit real_amount) - SUM(withdraw real_amount)`.
2. **Discrepancy:** If `amount` != `real_amount`, there might be a blockchain fee or error.
3. **Partition:** Always filter by `ds = '{latest_ds}'` for daily analysis.
"""

# 3. Training Examples (Dynamic Date)
EXAMPLES = [
    {
        "question": "Show total deposits and withdrawals per user.",
        "sql": """
        SELECT user_code,
               SUM(CASE WHEN type = 'deposit' THEN amount ELSE 0 END) AS total_deposit,
               SUM(CASE WHEN type = 'withdraw' THEN amount ELSE 0 END) AS total_withdraw,
               COUNT(*) AS transaction_count
        FROM public.dws_user_deposit_withdraw_detail_di
        WHERE ds = '{latest_ds}'
        GROUP BY user_code
        ORDER BY total_deposit DESC;
        """
    },
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
        "question": "Show the deposit vs withdrawal ratio per user.",
        "sql": """
        SELECT user_code,
               CASE WHEN SUM(CASE WHEN type = 'withdraw' THEN amount ELSE 0 END) = 0 THEN NULL
                    ELSE SUM(CASE WHEN type = 'deposit' THEN amount ELSE 0 END) / SUM(CASE WHEN type = 'withdraw' THEN amount ELSE 0 END)
               END AS deposit_withdraw_ratio
        FROM public.dws_user_deposit_withdraw_detail_di
        WHERE ds = '{latest_ds}'
        GROUP BY user_code
        ORDER BY deposit_withdraw_ratio DESC;
        """
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
        "question": "Show discrepancies where real amount matches requested amount.",
        "sql": """
        SELECT user_code, type, amount, real_amount, (amount - real_amount) AS discrepancy
        FROM public.dws_user_deposit_withdraw_detail_di
        WHERE ds = '{latest_ds}' AND amount <> real_amount
        ORDER BY discrepancy DESC;
        """
    },
    {
        "question": "Top users by net deposit including bonuses.",
        "sql": """
        SELECT user_code,
               SUM(CASE WHEN type = 'deposit' THEN real_amount ELSE 0 END) - SUM(CASE WHEN type = 'withdraw' THEN real_amount ELSE 0 END) AS net_deposit,
               SUM(CASE WHEN is_bonus = 1 THEN real_amount ELSE 0 END) AS bonus_received
        FROM public.dws_user_deposit_withdraw_detail_di
        WHERE ds = '{latest_ds}'
        GROUP BY user_code
        ORDER BY net_deposit DESC;
        """
    }
]