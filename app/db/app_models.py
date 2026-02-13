# app/db/app_models.py
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, BigInt, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from app.core.config import settings

# 1. Connect to Hologres (Sync Driver)
APP_DB_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
engine = create_engine(APP_DB_URL, pool_pre_ping=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 2. Define Models in the 'ai_pilot' Schema
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
    generated_sql = Column(Text)
    error_message = Column(Text, nullable=True)
    execution_success = Column(Boolean, default=False)
    cube_used = Column(String, nullable=True)
    
    # --- NEW METRICS (Measurable Proof) ---
    resolved_latest_ds = Column(String, nullable=True)
    row_count = Column(BigInt, nullable=True)
    execution_ms = Column(BigInt, nullable=True)
    visual_type = Column(String, nullable=True)
    correction_attempts = Column(Integer, default=0)
    tables_used = Column(Text, nullable=True)

# Note: We skip 'Base.metadata.create_all' here because we ran the DDL manually.
# This prevents permission errors on startup.