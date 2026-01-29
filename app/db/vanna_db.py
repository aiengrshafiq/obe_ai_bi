# app/db/vanna_db.py
from sqlalchemy import create_engine, text  # <--- IMPORT text
import pandas as pd
from app.core.config import settings
from app.services.vanna_wrapper import vn

def setup_vanna_db_connection():
    """
    Configures Vanna to use a synchronous SQLAlchemy engine for Pandas.
    """
    try:
        # Convert Async URL to Sync URL (psycopg2)
        SYNC_DATABASE_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
        vanna_engine = create_engine(SYNC_DATABASE_URL)

        def run_sql_hologres(sql: str) -> pd.DataFrame:
            try:
                # --- FIX FOR SQLALCHEMY 2.0 ---
                # We must use a connection context and wrap the SQL in text()
                with vanna_engine.connect() as connection:
                    return pd.read_sql(text(sql), connection)
                # ------------------------------
            except Exception as e:
                print(f"❌ Vanna SQL Execution Error: {e}")
                return pd.DataFrame()

        # Attach to Vanna
        vn.run_sql = run_sql_hologres
        vn.run_sql_is_set = True
        print("✅ Vanna Bridge: Connected to Hologres (Sync Mode)")
        
    except Exception as e:
        print(f"❌ Vanna Bridge Failed: {e}")