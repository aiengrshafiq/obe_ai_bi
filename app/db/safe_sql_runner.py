import pandas as pd
from sqlalchemy import create_engine, text
from app.core.config import settings
from app.pipeline.guardrails.sql_policy import SQLGuard, SQLPolicyException

# Create a Sync Engine for Pandas (Read-Only operations are fine with Sync)
# We use a dedicated engine to ensure we can set session-level params
DB_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
engine = create_engine(DB_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)

class SafeSQLRunner:
    """
    The Single Point of Execution.
    All SQL (AI-generated or Custom) MUST pass through here.
    """

    @staticmethod
    def execute(sql: str) -> pd.DataFrame:
        """
        1. Validates SQL (Guardrails).
        2. Sets Timeout & Read-Only mode.
        3. Returns Pandas DataFrame.
        """
        # 1. Validation
        safe_sql = SQLGuard.validate_and_fix(sql)

        # 2. Execution
        try:
            with engine.connect() as connection:
                # A. Safety Configuration (Session Level)
                connection.execute(text("SET statement_timeout = '15s';"))
                connection.execute(text("SET TRANSACTION READ ONLY;"))
                
                # B. Run Query
                # Using pandas read_sql ensures we get clean column names and types
                df = pd.read_sql(text(safe_sql), connection)
                
                return df
                
        except Exception as e:
            # We log the raw error here if needed
            print(f"❌ Execution Error: {e}")
            raise e

# Global Instance
runner = SafeSQLRunner()