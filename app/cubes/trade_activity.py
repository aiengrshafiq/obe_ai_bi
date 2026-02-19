# app/cubes/trade_activity.py

NAME = "Trade Activity Cube"
DESCRIPTION = "Contains individual trade execution details. Use for volume, fees, trade duration, risk analysis, and margin usage."
HAS_TIME_FIELD = True
TIME_COLUMN = "trade_datetime"
KIND = "di"
# 1. DDL (Expanded with BI Metrics)
DDL = """
CREATE TABLE public.dws_all_trades_di (
    -- IDENTIFIERS
    user_code BIGINT,
    order_id TEXT,          -- Unique Order ID
    symbol TEXT,            -- Base Asset (e.g. 'BTC')
    alias TEXT,             -- Pair (e.g. 'BTC/USDT')
    
    -- MARKET INFO
    market_type TEXT,       -- 'futures' or 'spot'
    order_side_desc TEXT,   -- 'buy', 'sell'
    order_status_desc TEXT, -- 'filled', 'partial', 'cancel'
    margin_mode_desc TEXT,  -- 'isolated', 'cross'
    
    -- METRICS
    deal_amount DECIMAL,    -- Volume
    net_fee DECIMAL,        -- Revenue
    deal_price DECIMAL,     -- Execution Price
    deal_quantity DECIMAL,  -- Quantity Traded
    duration_seconds BIGINT,-- Trade Duration (Seconds)
    leverage DECIMAL,       -- Leverage Used
    
    -- TIME
    trade_datetime TIMESTAMPTZ,
    ds TEXT                 -- Partition 'YYYYMMDD'
);
"""

# 2. Documentation (Business Logic)
DOCS = """
**Table Purpose:**
Records every trade execution. Key source for Volume, Revenue, and User Behavior.

**Critical Data Types & Rules:**
1. **Status Filtering:** ALWAYS use `_desc` columns. 
   - Cancelled Orders: `order_status_desc = 'cancel'`
   - Filled Orders: `order_status_desc = 'full'` or `order_status_desc = 'filled'`
2. **Symbol Matching:** - `symbol` = 'BTC' (Base Asset)
   - `alias` = 'BTC/USDT' (Trading Pair)
3. **Calculations:**
   - **Buy Volume:** Sum `deal_amount` where `order_side_desc = 'buy'`.
   - **Sell Volume:** Sum `deal_amount` where `order_side_desc = 'sell'`.
   - **Trade Duration:** Use `duration_seconds` to find outliers or average hold time.
4. **Time Analysis:** Use `EXTRACT(HOUR FROM trade_datetime)` for hourly analysis.
5. **User Code is a STRING:** ALWAYS wrap `user_code` in single quotes.
   - CORRRCT: `WHERE user_code = '10034920'`
   - WRONG: `WHERE user_code = 10034920` (This will fail).
6. **Partition:** Always filter by `ds = '{latest_ds}'` unless analyzing history.
"""

# 3. Training Examples (BI Team's Complex Queries - Adapted)
EXAMPLES = [
    {
        "question": "Show me the daily performance per symbol including buy and sell volume.",
        "sql": """
        SELECT symbol, 
               SUM(deal_amount) AS total_volume,
               SUM(CASE WHEN order_side_desc = 'buy' THEN deal_amount ELSE 0 END) AS buy_volume,
               SUM(CASE WHEN order_side_desc = 'sell' THEN deal_amount ELSE 0 END) AS sell_volume,
               AVG(deal_price) AS avg_price
        FROM public.dws_all_trades_di
        WHERE ds = '{latest_ds}'
        GROUP BY symbol
        ORDER BY total_volume DESC;
        """
    },
    {
        "question": "Find users with high trade duration outliers (longest 5%).",
        "sql": """
        SELECT * FROM (
            SELECT user_code, duration_seconds,
                   PERCENT_RANK() OVER (PARTITION BY user_code ORDER BY duration_seconds DESC) AS duration_rank
            FROM public.dws_all_trades_di
            WHERE ds = '{latest_ds}'
        ) t WHERE duration_rank > 0.95;
        """
    },
    {
        "question": "What is the cancellation ratio per user?",
        "sql": """
        SELECT user_code,
               ROUND(
                   SUM(CASE WHEN order_status_desc = 'cancel' THEN 1 ELSE 0 END)::DECIMAL / 
                   NULLIF(COUNT(order_id), 0), 2
               ) AS cancel_ratio
        FROM public.dws_all_trades_di
        WHERE ds = '{latest_ds}'
        GROUP BY user_code
        ORDER BY cancel_ratio DESC;
        """
    },
    {
        "question": "Analyze margin mode usage and average leverage.",
        "sql": """
        SELECT margin_mode_desc,
               COUNT(DISTINCT user_code) AS unique_users,
               SUM(deal_amount) AS total_volume,
               AVG(leverage) AS avg_leverage
        FROM public.dws_all_trades_di
        WHERE ds = '{latest_ds}' AND market_type = 'futures'
        GROUP BY margin_mode_desc
        ORDER BY total_volume DESC;
        """
    },
    {
        "question": "Show peak trading hours by volume.",
        "sql": """
        SELECT EXTRACT(HOUR FROM trade_datetime) AS trading_hour,
               COUNT(order_id) AS total_trades,
               SUM(deal_amount) AS total_volume
        FROM public.dws_all_trades_di
        WHERE ds = '{latest_ds}'
        GROUP BY 1
        ORDER BY trading_hour;
        """
    },
    {
        "question": "Top 10 users by fee contribution.",
        "sql": "SELECT user_code, SUM(net_fee) AS total_fee, COUNT(order_id) AS trades_count FROM public.dws_all_trades_di WHERE ds = '{latest_ds}' GROUP BY user_code ORDER BY total_fee DESC LIMIT 10;"
    }
]