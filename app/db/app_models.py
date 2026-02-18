# app/db/app_models.py
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, BigInteger, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from app.core.config import settings

# --- 1. Database Connection (Synchronous / Blocking) ---
# We force 'psycopg2' because Vanna/Pandas and Hologres work best with the standard driver.
# If config has 'postgresql+asyncpg', we strip it back to 'postgresql+psycopg2'.
APP_DB_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")

# Create Engine with Keep-Alive (pool_pre_ping) to handle Hologres timeouts
engine = create_engine(
    APP_DB_URL, 
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

# Session Factory (This is what Orchestrator & DateResolver use)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base Class for Models
Base = declarative_base()

# --- 2. Define Models in the 'ai_pilot' Schema ---

class User(Base):
    __tablename__ = "users"
    # This tells SQLAlchemy the table lives in the 'ai_pilot' schema
    __table_args__ = {"schema": "ai_pilot"}
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    role = Column(String, default="user")

class ChatLog(Base):
    __tablename__ = "chat_logs"
    __table_args__ = {"schema": "ai_pilot"}
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    username = Column(String)
    user_question = Column(Text)
    context_provided = Column(Text)
    
    # AI Output
    generated_sql = Column(Text)
    error_message = Column(Text, nullable=True)
    execution_success = Column(Boolean, default=False)
    
    # Telemetry & Debugging
    cube_used = Column(String, nullable=True)
    resolved_latest_ds = Column(String, nullable=True)
    row_count = Column(BigInteger, nullable=True)
    execution_ms = Column(BigInteger, nullable=True)
    visual_type = Column(String, nullable=True)
    correction_attempts = Column(Integer, default=0)
    tables_used = Column(Text, nullable=True)

# Note: We purposely skip 'Base.metadata.create_all' to avoid permission issues.
# Tables should be managed via DDL scripts in Alibaba Cloud.