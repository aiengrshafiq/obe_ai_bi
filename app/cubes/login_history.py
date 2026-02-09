# app/cubes/login_history.py

NAME = "Login History Cube"
DESCRIPTION = "Audit trail of user logins, IP addresses, locations, and device information (OS, Browser)."
HAS_TIME_FIELD = True
TIME_COLUMN = "create_at"
# 1. DDL
DDL = """
CREATE TABLE public.dwd_login_history_log_di (
    user_code BIGINT,
    code BIGINT,                -- Unique Login Event ID
    
    -- TIMESTAMPS
    create_at TIMESTAMPTZ,      -- Login Time
    update_at TIMESTAMPTZ,
    
    -- CONTEXT
    type STRING,                -- 'LOGIN', 'LOGOUT'
    status BIGINT,              -- 1=Success, 0=Fail (check data to confirm)
    business STRING,            -- e.g. 'MAIN'
    
    -- NETWORK & DEVICE
    ip STRING,
    location STRING,            -- City/Country derived from IP
    os STRING,                  -- e.g. 'Windows', 'iOS'
    device STRING,              -- Browser/Device Name
    user_agent STRING,          -- Raw User Agent
    language STRING,            -- Browser Language
    
    -- PARTITION
    ds STRING                   -- Date Partition 'YYYYMMDD'
);
"""

# 2. Documentation
DOCS = """
**Table Purpose:**
Tracks security events (Logins/Logouts). Use for analyzing user geography, device usage, and security audits.

**Critical Rules:**
1. **Event Type:** Filter by `type = 'LOGIN'` to count actual user sessions.
2. **Granularity:** One row per event.
3. **Partition:** Always filter by `ds = '{latest_ds}'` for daily analysis.
4. **User Code:** Treat `user_code` as a String in WHERE clauses (e.g. `user_code = '123'`).
"""

# 3. Training Examples (Dynamic Date)
EXAMPLES = [
    {
        "question": "Count distinct users by OS and user agent to find rare combinations.",
        "sql": """
        SELECT os, user_agent, COUNT(*) AS occurrences
        FROM public.dwd_login_history_log_di
        WHERE ds = '{latest_ds}'
        GROUP BY os, user_agent
        HAVING COUNT(*) < 5
        ORDER BY occurrences ASC
        LIMIT 200;
        """
    },
    {
        "question": "Show number of logins per IP and OS.",
        "sql": """
        SELECT ip, os, COUNT(*) AS login_count
        FROM public.dwd_login_history_log_di
        WHERE ds = '{latest_ds}' AND type = 'LOGIN'
        GROUP BY ip, os
        ORDER BY login_count DESC
        LIMIT 100;
        """
    },
    {
        "question": "What are the top 10 devices used for login?",
        "sql": """
        SELECT device, COUNT(*) AS login_count
        FROM public.dwd_login_history_log_di
        WHERE ds = '{latest_ds}'
        GROUP BY device
        ORDER BY login_count DESC
        LIMIT 10;
        """
    },
    {
        "question": "Distribution of users by language and business type.",
        "sql": """
        SELECT language, business, COUNT(DISTINCT user_code) AS unique_users
        FROM public.dwd_login_history_log_di
        WHERE ds = '{latest_ds}'
        GROUP BY language, business
        ORDER BY unique_users DESC;
        """
    },
    {
        "question": "Show login patterns by hour of day.",
        "sql": """
        SELECT EXTRACT(HOUR FROM create_at) AS hour, COUNT(*) AS logins
        FROM public.dwd_login_history_log_di
        WHERE ds = '{latest_ds}'
        GROUP BY 1
        ORDER BY hour ASC;
        """
    }
]