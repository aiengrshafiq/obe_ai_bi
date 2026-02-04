# app/cubes/device_log.py

NAME = "Device & Fraud Log Cube"
DESCRIPTION = "Detailed device fingerprinting, risk scores (VPN/Proxy), and user event tracking (Register/Login)."

# 1. DDL (Exact Schema)
DDL = """
CREATE TABLE public.dwd_user_device_log_di (
    -- IDENTIFIERS
    user_code BIGINT,
    visitor_id STRING,          -- Unique Device Fingerprint
    request_id STRING,
    linked_id STRING,
    
    -- TIMESTAMPS
    create_at DATETIME,
    update_at DATETIME,
    
    -- EVENT CONTEXT
    operation STRING,           -- 'register', 'login', 'withdraw'
    event_id STRING,
    url STRING,
    
    -- NETWORK & GEO
    ip STRING,
    asn_name STRING,            -- ISP Name
    time_zone STRING,
    country_code STRING,
    country STRING,
    city STRING,
    
    -- DEVICE DETAILS
    device_model STRING,        -- e.g. iPhone 13
    os_name STRING,             -- iOS, Android
    os_version STRING,
    browser_name STRING,
    browser_version STRING,
    user_agent STRING,
    
    -- RISK FLAGS (1 = Yes, 0 = No)
    is_vpn BIGINT,
    is_proxy BIGINT,
    is_bot BIGINT,
    is_emulator BIGINT,
    is_root BIGINT,             -- Rooted/Jailbroken
    is_tampered BIGINT,         -- Browser Tampering
    
    -- SCORES & RAW DATA
    risk_confidence DOUBLE,     -- 0.0 to 1.0 (High is bad)
    proxy_confidence STRING,    -- 'high', 'medium', 'low'
    raw_fingerprint_response STRING,
    
    -- PARTITION
    ds STRING                   -- Date Partition 'YYYYMMDD'
);
"""

# 2. Documentation
DOCS = """
**Table Purpose:**
Used for fraud detection, finding multiple accounts on one device, and checking VPN usage during sensitive actions.

**Critical Logic:**
1. **Risk Analysis:** Use `is_vpn=1` or `is_proxy=1` to find suspicious traffic.
2. **Operations:** Filter by `operation` (e.g. `operation = 'register'`) to narrow down the event type.
3. **Scores:** `risk_confidence > 0.8` indicates high probability of fraud.
4. **Partition:** Always filter by `ds = '{latest_ds}'` for the most recent view.
"""

# 3. Training Examples (Dynamic Date)
EXAMPLES = [
    {
        "question": "Count registrations using VPN or Proxy.",
        "sql": """
        SELECT COUNT(*) AS vpn_proxy_registrations
        FROM public.dwd_user_device_log_di
        WHERE ds = '{latest_ds}'
        AND operation = 'register'
        AND (is_vpn = 1 OR is_proxy = 1);
        """
    },
    {
        "question": "Top 5 countries by registration count.",
        "sql": """
        SELECT country, COUNT(*) AS registrations
        FROM public.dwd_user_device_log_di
        WHERE ds = '{latest_ds}'
        AND operation = 'register'
        GROUP BY country
        ORDER BY registrations DESC
        LIMIT 5;
        """
    },
    {
        "question": "Show high-risk devices with confidence score > 0.8.",
        "sql": """
        SELECT user_code, device_model, risk_confidence
        FROM public.dwd_user_device_log_di
        WHERE ds = '{latest_ds}'
        AND CAST(risk_confidence AS DOUBLE) > 0.8
        ORDER BY risk_confidence DESC
        LIMIT 50;
        """
    },
    {
        "question": "Find users with multiple IP addresses in a single day.",
        "sql": """
        SELECT user_code, COUNT(DISTINCT ip) AS distinct_ips
        FROM public.dwd_user_device_log_di
        WHERE ds = '{latest_ds}'
        GROUP BY user_code
        HAVING COUNT(DISTINCT ip) > 1
        ORDER BY distinct_ips DESC;
        """
    },
    {
        "question": "List users utilizing Chrome browser.",
        "sql": """
        SELECT user_code, user_agent
        FROM public.dwd_user_device_log_di
        WHERE ds = '{latest_ds}'
        AND user_agent LIKE '%Chrome%'
        ORDER BY create_at ASC
        LIMIT 50;
        """
    }
]