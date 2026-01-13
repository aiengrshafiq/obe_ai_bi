def get_ddl_context() -> str:
    """
    Returns the schema definition for the AI to understand the data structure.
    Using simplified Postgres syntax for the AI to generate compatible SQL.
    """
    return """
    -- Table 1: Deposits and Withdrawals. Use this for funding questions.
    -- Key columns: 'amount' (Raw), 'real_amount' (Net), 'audit_status_desc' (Status).
    -- Filter 'type' by 'deposit' or 'withdraw'.
    CREATE TABLE dws_user_deposit_withdraw_detail_di (
        user_code TEXT, 
        type TEXT, -- 'deposit' or 'withdraw'
        create_at TIMESTAMP, 
        coin TEXT, -- e.g. 'USDT', 'BTC'
        chain TEXT, 
        amount NUMERIC, 
        fee_amount NUMERIC, 
        real_amount NUMERIC, 
        audit_status_desc TEXT, -- e.g. 'Success', 'Pending'
        ds TEXT -- Date partition 'YYYYMMDD'
    );

    -- Table 2: User Position Snapshots. Use this for 'Risk', 'Holdings' or 'Points'.
    CREATE TABLE dwd_activity_t_points_position_snapshot_di (
        user_code TEXT,
        snapshot_date TEXT, 
        position_usdt NUMERIC, -- Total value in USDT
        position_btc NUMERIC, 
        earned_points NUMERIC,
        ds TEXT
    );

    -- Table 3: All Trades. Use this for Volume, Trading Count, Fee Revenue.
    -- Key columns: 'deal_amount' (Volume), 'fee' (Revenue), 'symbol' (e.g. BTC-USDT).
    CREATE TABLE dws_all_trades_di (
        user_code TEXT,
        market_type TEXT, -- 'futures' or 'spot'
        trade_datetime TIMESTAMP,
        deal_amount NUMERIC, -- Volume in Quote Currency
        fee NUMERIC, -- Revenue
        symbol TEXT, -- e.g. 'BTC-USDT'
        order_side TEXT, -- 'BUY' or 'SELL'
        ds TEXT
    );
    """