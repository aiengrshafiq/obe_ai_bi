# app/cubes/points_system.py

NAME = "Points System Cube"
DESCRIPTION = "Tracks user points generation (Tasks), consumption, and campaign activities like Lucky Spin and Check-ins."
HAS_TIME_FIELD = True
TIME_COLUMN = "created_at"
# 1. DDL
DDL = """
CREATE TABLE public.dwd_activity_t_points_user_task_di (
    -- ID & METADATA
    id BIGINT,
    user_code STRING,
    invitee_user_code STRING, -- For referral tracking
    
    -- BUSINESS LOGIC
    rule_code STRING,   -- KEY FIELD: 'DAILY_CHECK-IN', 'LUCKY_SPIN', 'TRADE_REWARD', 'INVITE'
    state STRING,       -- Filter by 'COMPLETED' for valid points
    remark STRING,
    
    -- METRICS
    task_value DECIMAL,    -- The base value (e.g. trade volume amount)
    earned_points DECIMAL, -- The actual points given to user
    
    -- TIME
    task_at DATETIME,
    created_at DATETIME,
    ds STRING              -- Partition 'YYYYMMDD'
);
"""

# 2. Documentation (Encoding the Dashboard Rules)
DOCS = """
**Table Purpose:**
Tracks how users earn and spend points. 
- **Generation:** Activities where users get points (Check-in, Trading).
- **Consumption:** Activities where users spend points (Lucky Spin entry fee).

**Critical Dashboard Rules:**
1. **Valid Points:** ALWAYS filter by `state = 'COMPLETED'`.
2. **Rule Codes:** - Check-In: `rule_code = 'DAILY_CHECK-IN'`
   - Lucky Spin: `rule_code = 'LUCKY_SPIN'`
   - Invitation: `rule_code = 'INVITE'` or `rule_code LIKE '%INVITE%'`
3. **User Ranges (Distribution):** When asked for "User Point Distribution", group users by total points into these exact buckets:
   - '0-199', '200-499', '500-999', '1000-1999', '2000-4999', '5000-9999', '10000+'
4. **Time Trends:** For "Daily Generated" or "Daily Consumed", group by `ds`.
"""

# 3. Training Examples (The Complex Queries)
EXAMPLES = [
    {
        "question": "Show the distribution of users by total points holding.",
        "sql": """
        SELECT 
            CASE 
                WHEN total_p < 200 THEN '0-199'
                WHEN total_p BETWEEN 200 AND 499 THEN '200-499'
                WHEN total_p BETWEEN 500 AND 999 THEN '500-999'
                WHEN total_p BETWEEN 1000 AND 1999 THEN '1000-1999'
                WHEN total_p BETWEEN 2000 AND 4999 THEN '2000-4999'
                WHEN total_p BETWEEN 5000 AND 9999 THEN '5000-9999'
                ELSE '10000+' 
            END as point_range,
            COUNT(user_code) as user_count
        FROM (
            SELECT user_code, SUM(earned_points) as total_p 
            FROM public.dwd_activity_t_points_user_task_di 
            WHERE state = 'COMPLETED' 
            GROUP BY user_code
        ) t
        GROUP BY 1
        ORDER BY user_count DESC;
        """
    },
    {
        "question": "What are the most profitable activities (total generated points)?",
        "sql": """
        SELECT rule_code, SUM(earned_points) as total_generated 
        FROM public.dwd_activity_t_points_user_task_di 
        WHERE state = 'COMPLETED' AND ds = '{latest_ds}' 
        GROUP BY rule_code 
        ORDER BY total_generated DESC;
        """
    },
    {
        "question": "Show daily generated points trend broken down by activity.",
        "sql": """
        SELECT ds, rule_code, SUM(earned_points) as daily_points
        FROM public.dwd_activity_t_points_user_task_di
        WHERE state = 'COMPLETED'
        GROUP BY ds, rule_code
        ORDER BY ds ASC;
        """
    },
    {
        "question": "List top 10 users by points earned from Daily Check-ins.",
        "sql": """
        SELECT user_code, SUM(earned_points) as checkin_points
        FROM public.dwd_activity_t_points_user_task_di
        WHERE rule_code = 'DAILY_CHECK-IN' AND state = 'COMPLETED'
        GROUP BY user_code
        ORDER BY checkin_points DESC
        LIMIT 10;
        """
    }
]