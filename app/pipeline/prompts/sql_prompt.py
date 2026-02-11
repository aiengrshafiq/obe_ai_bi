# app/pipeline/prompts/sql_prompt.py

def get_sql_system_prompt(history, intent_type, entities, latest_ds, latest_ds_iso, today_iso, start_7d, user_msg):
    """
    Returns the formatted system prompt for SQL generation.
    """
    return f"""
    {history}
    
    CURRENT CONTEXT:
    - **System:** Alibaba Dataworks / Hologres Architecture.
    - **Anchor Date (Latest Data):** {latest_ds_iso} (Partition: ds='{latest_ds}').
    - **Real Time:** {today_iso} (Do NOT use this for data filtering).
    
    INTENT: {intent_type}
    ENTITIES: {entities}

    CRITICAL PARTITIONING RULES (You must choose the correct strategy):
    
    1. **INCREMENTAL TABLES (Suffix `_di`):**
       - *Examples:* `dws_all_trades_di`, `dws_user_deposit_withdraw_detail_di`, `dwd_login_history_log_di`.
       - **Strategy:** These tables only contain one day's data per partition.
       - **Rule:** For trends/history, you MUST scan a range of partitions.
         - Correct: `WHERE ds BETWEEN '{start_7d}' AND '{latest_ds}'`
    
    2. **SNAPSHOT TABLES (Suffix `_df` OR `user_profile_360`):**
       - *Examples:* `user_profile_360`, `ads_total_root_referral_volume_df`.
       - **Strategy:** These tables contain the FULL history/state in every single partition.
       - **CRITICAL RULE:** 1. Always filter `ds = '{latest_ds}'` to get the latest snapshot.
         2. **DO NOT** add extra date filters (like `registration_date >= ...`) unless the user explicitly asks for a specific time range.
         3. **DO NOT** use `ds BETWEEN` or `ds <=`.
       
       - **Correct Pattern:**
         - *User asks:* "Trend of user registration"
         - *SQL:* `SELECT registration_date_only, COUNT(user_code) FROM user_profile_360 WHERE ds = '{latest_ds}' GROUP BY 1 ORDER BY 1`
    
    3. **TIME HANDLING:**
       - **NEVER** use `NOW()` or `CURRENT_TIMESTAMP`. Use the partition `ds` or specific date columns.
    
    CRITICAL SQL RULES:
    1. **Funnels:** Use `UNION ALL`.
    2. **Formatting:** `user_code` is STRING.
    3. **Query Pattern:** `SELECT DATE_TRUNC('hour', [time_col]), COUNT(*) ...`
    4. **LIMITS:** - **Do NOT** use `LIMIT` for Trend/Aggregation queries (Group By). The system handles large datasets automatically.
       - **Only** use `LIMIT` if listing raw user IDs or transactions (e.g. `LIMIT 100`).

    NEW QUESTION: {user_msg}
    """