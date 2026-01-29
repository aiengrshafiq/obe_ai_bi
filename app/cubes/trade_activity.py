# app/cubes/trade_activity.py

NAME = "Trade Activity Cube"
DESCRIPTION = "Contains individual trade execution details. Use for volume, fees, specific symbols (BTC/ETH), and trade counts."

DDL = """
CREATE TABLE public.dws_all_trades_di (
    -- USER INFO
    user_code BIGINT,
    country TEXT,
    
    -- MARKET INFO
    market_type TEXT,       -- 'futures' or 'spot'
    symbol TEXT,            -- BASE ASSET ONLY (e.g. 'BTC', 'ETH', 'ZEC')
    currency TEXT,          -- QUOTE ASSET (e.g. 'USDT')
    alias TEXT,             -- TRADING PAIR (e.g. 'BTC/USDT', 'ZEC/USDT')
    
    -- TRADE METRICS
    deal_amount DECIMAL,    -- Total Trade Value (USDT)
    fee DECIMAL,
    net_fee DECIMAL,        -- Actual Revenue (Fee - Rebate)
    
    -- TIME
    trade_datetime TIMESTAMPTZ,
    ds TEXT                 -- Date Partition 'YYYYMMDD'
);
"""

DOCS = """
**Critical Data Rules:**
1. **Symbol vs Alias:** - `symbol` column ONLY contains the base asset (e.g. 'BTC', 'ETH'). 
   - `alias` column contains the pair (e.g. 'BTC/USDT').
   - **User Request:** If user asks for "BTCUSDT", query `alias = 'BTC/USDT'` OR `(symbol = 'BTC' AND currency = 'USDT')`.
   - **User Request:** If user asks for "Bitcoin" or "BTC", query `symbol = 'BTC'`.

2. **Partition Filtering:** Always filter by `ds = '{latest_ds}'` for daily snapshots.
"""

EXAMPLES = [
    {
        "question": "What was the total trading volume for BTC yesterday?",
        "sql": "SELECT SUM(deal_amount) as total_volume FROM public.dws_all_trades_di WHERE ds = '{latest_ds}' AND symbol = 'BTC';"
    },
    {
        "question": "Show revenue for ETH/USDT pair.",
        "sql": "SELECT SUM(net_fee) FROM public.dws_all_trades_di WHERE ds = '{latest_ds}' AND alias = 'ETH/USDT';"
    },
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

