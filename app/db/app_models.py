# app/db/app_models.py
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Local SQLite DB for Application State (Users, Logs)
APP_DB_URL = "sqlite:///./app_data.db"

engine = create_engine(APP_DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 1. User Table
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    role = Column(String, default="user") # 'admin' or 'user'

# 2. Audit Log (The "Black Box" Recorder)
class ChatLog(Base):
    __tablename__ = "chat_logs"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    username = Column(String)
    
    # Context
    user_question = Column(Text)
    context_provided = Column(Text) # What 'history' or 'date' we sent to AI
    
    # AI Performance
    generated_sql = Column(Text)
    error_message = Column(Text, nullable=True)
    execution_success = Column(Boolean, default=False)
    
    # Classification
    cube_used = Column(String, nullable=True) # Which table did it pick?

# Create Tables
Base.metadata.create_all(bind=engine)