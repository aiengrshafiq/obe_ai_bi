# app/cubes/trade_activity.py

NAME = "Trade Activity Cube"
DESCRIPTION = "Contains individual trade execution details from Futures and Spot markets. Use for volume, fees, specific symbols (BTC/ETH), and trade counts."

# 1. DDL (Optimized for AI Context)
# We flattened the partition 'ds' into the table for easier SQL generation.
DDL = """
CREATE TABLE public.dws_all_trades_di (
    -- USER INFO
    user_code BIGINT,
    country TEXT,
    user_type TEXT,
    
    -- MARKET INFO
    market_type TEXT,       -- 'futures' or 'spot'
    symbol TEXT,            -- e.g. 'BTCUSDT', 'ETHUSDT'
    product_code TEXT,
    
    -- TRADE METRICS
    deal_amount DECIMAL,    -- Total Trade Value (USDT)
    deal_quantity DECIMAL,  -- Asset Quantity
    deal_price DECIMAL,
    fee DECIMAL,
    net_fee DECIMAL,        -- Actual Revenue (Fee - Rebate)
    
    -- FUTURE SPECIFIC
    leverage DECIMAL,
    position_direction TEXT, -- 'long', 'short'
    order_side TEXT,         -- 'buy', 'sell'
    
    -- TIME
    trade_datetime TIMESTAMPTZ,
    ds TEXT                 -- Date Partition 'YYYYMMDD'
);
"""

# 2. Documentation (Business Logic)
DOCS = """
**Table Purpose:**
This table records EVERY single trade. It is huge.
- **Granularity:** One row per trade execution.
- **Volume:** Use `SUM(deal_amount)`.
- **Revenue:** Use `SUM(net_fee)`.
- **Spot vs Futures:** Filter by `market_type`.

**Critical Rules:**
1. **Partition Filtering:** This table is partitioned by `ds`. You SHOULD filter by `ds` whenever possible to make queries faster (e.g., `ds = '{latest_ds}'` or `ds BETWEEN ...`).
2. **Symbol Matching:** Symbols are formatted like 'BTCUSDT'. If a user asks for 'Bitcoin', use `symbol LIKE '%BTC%'`.
3. **Date Handling:** `ds` is a string 'YYYYMMDD'. To filter last 7 days, convert dates to this format.
"""

# 3. Training Examples (Pattern Matching)
EXAMPLES = [
    {
        "question": "What was the total trading volume yesterday?",
        "sql": "SELECT SUM(deal_amount) as total_volume FROM public.dws_all_trades_di WHERE ds = '{latest_ds}';"
    },
    {
        "question": "Show me the top 5 pairs by volume.",
        "sql": "SELECT symbol, SUM(deal_amount) as vol FROM public.dws_all_trades_di WHERE ds = '{latest_ds}' GROUP BY symbol ORDER BY vol DESC LIMIT 5;"
    },
    {
        "question": "How much revenue did we make from Futures today?",
        "sql": "SELECT SUM(net_fee) FROM public.dws_all_trades_di WHERE ds = '{latest_ds}' AND market_type = 'futures';"
    },
    {
        "question": "Count distinct users who traded BTC.",
        "sql": "SELECT COUNT(DISTINCT user_code) FROM public.dws_all_trades_di WHERE ds = '{latest_ds}' AND symbol LIKE '%BTC%';"
    }
]