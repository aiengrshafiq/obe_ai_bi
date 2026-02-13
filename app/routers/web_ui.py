# app/routers/web_ui.py
import json
import time
from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import desc

# Internal imports
from app.services.vanna_wrapper import vn
from app.db.safe_sql_runner import SafeSQLRunner 
from app.services.date_resolver import DateResolver
from app.db.app_models import SessionLocal, ChatLog, User
from app.services.auth import verify_token, get_password_hash, verify_password, create_access_token 

# Pipeline Imports
from app.pipeline.orchestrator import Orchestrator
from app.services.cache import cache

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# --- Dependency: Get DB Session ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Dependency: Get Current User ---
async def get_current_user(token: str = Depends(oauth2_scheme), db = Depends(get_db)):
    user = verify_token(token, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

# 1. Login Endpoint (Public)
@router.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db = Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

# 2. Register Endpoint (PROTECTED - Only logged in users can create new users)
class UserRegister(BaseModel):
    username: str
    password: str

def generate_id():
    return int(time.time() * 1000)

@router.post("/api/register")
async def register(
    user: UserRegister, 
    db = Depends(get_db), 
    current_user: User = Depends(get_current_user) # <--- SECURITY LOCKED
):
    if db.query(User).filter(User.username == user.username).first():
        raise HTTPException(status_code=400, detail="User already exists")
    
    new_user = User(id=generate_id(), username=user.username, hashed_password=get_password_hash(user.password))
    db.add(new_user)
    db.commit()
    return {"status": "User created", "created_by": current_user.username}

# 3. Chat Request Model
class ChatRequest(BaseModel):
    message: str
    history: List[Dict[str, str]] = []

# --- Public Pages (Login/Shell) ---

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@router.get("/")
async def get_chat_ui(request: Request):
    # We leave the shell public, but the API calls inside it will fail if not logged in.
    return templates.TemplateResponse("chat.html", {"request": request})

# --- PROTECTED PAGES (Admin Only) ---

@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, current_user: User = Depends(get_current_user)):
    # You cannot even see the registration form unless you are logged in.
    return templates.TemplateResponse("register.html", {"request": request, "user": current_user})

@router.get("/admin/logs", response_class=HTMLResponse)
async def view_logs(
    request: Request, 
    db = Depends(get_db), 
    page: int = 1, 
    username: str = "",
    current_user: User = Depends(get_current_user) # <--- SECURITY LOCKED
):
    page_size = 20
    query = db.query(ChatLog)
    if username: query = query.filter(ChatLog.username.ilike(f"%{username}%"))
    
    total_records = query.count()
    logs = query.order_by(desc(ChatLog.timestamp)).offset((page - 1) * page_size).limit(page_size).all()
    total_pages = (total_records + page_size - 1) // page_size
    
    return templates.TemplateResponse("admin_logs.html", {
        "request": request, 
        "logs": logs, 
        "page": page, 
        "total_pages": total_pages, 
        "username": username,
        "user": current_user
    })

@router.get("/admin/knowledge_base", response_class=HTMLResponse)
async def knowledge_base(request: Request, current_user: User = Depends(get_current_user)): # <--- SECURITY LOCKED
    return templates.TemplateResponse("knowledge_base.html", {"request": request, "user": current_user})

# --- PROTECTED API ENDPOINTS ---

@router.post("/api/chat")
async def chat_endpoint(
    payload: ChatRequest, 
    current_user: User = Depends(get_current_user)
):
    """
    The Main Entry Point.
    Delegates all logic to the Orchestrator Pipeline.
    """
    # 1. Ensure Date Context is Ready
    await DateResolver.get_latest_ds()
    
    user_msg = payload.message.strip()
    
    # 2. Build History Context
    history_context = ""
    if payload.history:
        recent = payload.history[-4:] 
        history_context = "PREVIOUS CONVERSATION:\n" + "\n".join(
            [f"{msg['role'].upper()}: {msg['content']}" for msg in recent]
        )

    # 3. Run Pipeline
    orchestrator = Orchestrator(user=current_user.username)
    result = await orchestrator.run_pipeline(user_msg, history_context)
    
    return result

# --- Custom SQL Endpoint ---
class CustomSQLRequest(BaseModel):
    sql_query: str

@router.post("/api/run_custom_sql")
async def run_custom_sql_endpoint(payload: CustomSQLRequest, current_user: User = Depends(get_current_user)):
    """
    Unified Endpoint for Custom SQL.
    Uses SafeSQLRunner to enforce policies.
    """
    from app.pipeline.agents.visualization import VisualizationAgent
    
    sql = payload.sql_query.strip()
    
    try:
        # 1. Execution (Validation happens inside SafeSQLRunner)
        df = SafeSQLRunner.execute(sql)
        
        if df is None or df.empty:
             return {"type": "success", "sql": sql, "data": [], "visual_type": "table"}

        # 2. Visualization (Auto-detect best chart)
        viz_result = await VisualizationAgent.determine_format(df, sql, "Custom SQL Execution")

        return {
            "type": "success", 
            "sql": sql,
            "data": df.head(100).to_dict(orient='records'),
            "visual_type": viz_result['visual_type'],
            "plotly_code": viz_result.get('plotly_code') 
        }
    except Exception as e:
        return {"type": "error", "message": str(e)}