# app/db/vanna_db.py
from sqlalchemy import create_engine, text
import pandas as pd
import re
from app.core.config import settings
from app.services.vanna_wrapper import vn

# --- LAYER 1: THE SANITIZER (Regex Blocklist) ---
FORBIDDEN_PATTERNS = [
    # 1. DDL & DML (Write Operations)
    r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b", r"\bALTER\b", 
    r"\bDROP\b", r"\bTRUNCATE\b", r"\bCREATE\b", r"\bREPLACE\b",
    r"\bGRANT\b", r"\bREVOKE\b", r"\bCOPY\b",
    
    # 2. System & Admin Functions
    r"\bpg_",             # Blocks pg_sleep, pg_shadow, pg_terminate_backend
    r"\bcurrent_user\b",  # Prevent recon
    r"\bversion\(\)",     # Prevent recon
    r"\binformation_schema\b", # Prevent schema sniffing
    
    # 3. Dangerous Hacks
    r"\bdblink\b",        # Cross-db connections
    r";.*;"               # Prevent multi-statement injection (roughly)
]

def validate_sql_safety(sql: str):
    """
    Checks SQL against a blocklist of dangerous keywords.
    Raises Exception if unsafe.
    """
    # Normalize: Upper case for keyword check, remove newlines
    normalized = sql.upper().replace('\n', ' ')
    
    for pattern in FORBIDDEN_PATTERNS:
        # We search case-insensitive, looking for word boundaries (\b)
        # So 'DROP' is banned, but 'DROPLET' (if it were a column) is safe.
        if re.search(pattern, sql, re.IGNORECASE):
            raise ValueError(f"Security Alert: Query contains forbidden pattern '{pattern}'")

    # Strict Check: Must start with SELECT or WITH
    if not (normalized.strip().startswith("SELECT") or normalized.strip().startswith("WITH")):
        raise ValueError("Security Alert: Only SELECT queries are allowed.")

def setup_vanna_db_connection():
    """
    Configures Vanna with a Secure, Read-Only Execution Engine.
    """
    try:
        # Convert Async URL to Sync URL (psycopg2)
        SYNC_DATABASE_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
        vanna_engine = create_engine(SYNC_DATABASE_URL)

        def run_sql_hologres(sql: str) -> pd.DataFrame:
            try:
                # 1. LAYER 1: Regex Validation
                validate_sql_safety(sql)

                # 2. LAYER 2 & 3: Session Security (Read-Only + Timeout)
                # We use a transaction to enforce session-level variables JUST for this query
                with vanna_engine.connect() as connection:
                    
                    # A. Set Timeout (20 seconds max) - Prevents DoS/pg_sleep
                    connection.execute(text("SET statement_timeout = '20s';"))
                    
                    # B. Force Read-Only - The ultimate safety net
                    # Even if 'DELETE' bypassed Regex, DB will reject it here.
                    connection.execute(text("SET TRANSACTION READ ONLY;"))
                    
                    # C. Execute the User Query
                    return pd.read_sql(text(sql), connection)
                    
            except ValueError as ve:
                print(f"🚨 Security Blocked: {ve}")
                # Return empty DF so app doesn't crash, but logs the attack
                return pd.DataFrame() 
            except Exception as e:
                print(f"❌ SQL Execution Error: {e}")
                return pd.DataFrame()

        # Attach to Vanna
        vn.run_sql = run_sql_hologres
        vn.run_sql_is_set = True
        print("✅ Vanna Bridge: Connected (Secure Mode: Read-Only + Timeout)")
        
    except Exception as e:
        print(f"❌ Vanna Bridge Failed: {e}")