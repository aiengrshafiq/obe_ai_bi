# app/db/vanna_db.py
from sqlalchemy import create_engine, text
from app.core.config import settings

def setup_vanna_db_connection():
    """
    Connectivity Check Only.
    
    NOTE: SQL Execution logic has moved to 'app.db.safe_sql_runner'.
    This function simply verifies we can talk to Hologres on startup.
    """
    try:
        # Convert Async URL to Sync URL for the check
        SYNC_DATABASE_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
        engine = create_engine(SYNC_DATABASE_URL)

        # Simple Ping
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            
        print("✅ Database Connectivity: OK")
        
    except Exception as e:
        print(f"❌ Database Connectivity Failed: {e}")
        # In production, you might want to raise e to stop the pod from starting
        # raise e