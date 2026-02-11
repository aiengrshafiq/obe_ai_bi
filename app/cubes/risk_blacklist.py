# app/cubes/risk_blacklist.py

NAME = "Risk Campaign Blacklist"
DESCRIPTION = "Registry of blocked users (abusers) for specific campaigns. Use to check if a user is banned."
HAS_TIME_FIELD = True
TIME_COLUMN = "start_date"

# 1. DDL (AI-Optimized)
DDL = """
CREATE TABLE public.risk_campaign_blacklist (
    user_code STRING PRIMARY KEY, -- The User ID to check
    reason STRING,                -- Why they were blocked
    owner STRING,                 -- Who blocked them
    
    -- TIMESTAMPS
    start_date TIMESTAMPTZ,       -- KEY TIME COLUMN: When the ban started
    end_date TIMESTAMPTZ          -- When the ban ends (default 2099)
);
"""

# 2. Documentation
DOCS = """
**Table Purpose:**
A security blacklist. If a user exists in this table, they are BLOCKED/BANNED.

**Critical Rules:**
1. **Time Analysis:** NEVER use 'create_at'. You MUST use `start_date` to find when a user was blacklisted.
2. **Lookup Pattern:** - Users usually provide a list of IDs. Use `WHERE user_code IN (...)`.
   - **User Code is STRING:** Always wrap IDs in quotes: `user_code IN ('1001', '1002')`.
3. **Status:** If a row exists, the user is blacklisted.
"""

# 3. Training Examples (Single & Bulk)
EXAMPLES = [
    {
        "question": "Is user 10034920 blacklisted?",
        "sql": "SELECT user_code, reason, start_date FROM public.risk_campaign_blacklist WHERE user_code = '10034920';"
    },
    {
        "question": "Check if these users are in the blacklist: 1001, 1002, 1003.",
        "sql": "SELECT user_code, reason FROM public.risk_campaign_blacklist WHERE user_code IN ('1001', '1002', '1003');"
    },
    {
        "question": "How many of the provided users are blocked?",
        "sql": "SELECT COUNT(*) as blocked_count FROM public.risk_campaign_blacklist WHERE user_code IN ('ID1', 'ID2', 'ID3');"
    },
    {
        "question": "Show me the reason why user 555 was blocked.",
        "sql": "SELECT user_code, reason, owner FROM public.risk_campaign_blacklist WHERE user_code = '555';"
    },
    {
        # NEW EXAMPLE: Teaches the model how to handle 'trend' for this specific table
        "question": "Show me the daily trend of blacklisted users.",
        "sql": "SELECT DATE(start_date) as ban_date, COUNT(user_code) as banned_users FROM public.risk_campaign_blacklist GROUP BY 1 ORDER BY 1;"
    }
]