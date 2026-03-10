# app/routers/web_ui.py
import json
import time
from fastapi import APIRouter, Request, Depends, HTTPException, Response, status
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import desc
from typing import Optional

# Internal imports
from app.services.vanna_wrapper import vn
from app.db.safe_sql_runner import SafeSQLRunner 
from app.services.date_resolver import DateResolver
from app.db.app_models import SessionLocal, ChatLog, User, PinnedChart
from app.services.auth import verify_token, get_password_hash, verify_password, create_access_token 

# Pipeline Imports
from app.pipeline.orchestrator import Orchestrator
from app.services.cache import cache

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# Auto error False allows us to handle both Token (API) and Cookie (Browser)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

# --- Dependency: Get DB Session ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Dependency: Get Current User ---
async def get_current_user(
    request: Request,
    token: str = Depends(oauth2_scheme), # Tries header first
    db = Depends(get_db)
):
    # 1. Try Header Token (API calls)
    user = None
    if token:
        user = verify_token(token, db)
    
    # 2. If Header failed, Try Cookie (Browser navigation)
    if not user:
        cookie_token = request.cookies.get("access_token")
        if cookie_token:
            # Clean "Bearer " prefix if present in cookie
            clean_token = cookie_token.replace("Bearer ", "")
            user = verify_token(clean_token, db)

    # 3. Final Check
    if not user:
        # If it's a browser page request (HTML), redirect to login instead of 401 JSON
        if "text/html" in request.headers.get("accept", ""):
            return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)
            
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

# 1. Login Endpoint (Public)
@router.post("/token")
async def login(response: Response, form_data: OAuth2PasswordRequestForm = Depends(), db = Depends(get_db)): 
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    access_token = create_access_token(data={"sub": user.username})
    token_str = f"Bearer {access_token}"
    
    # SET COOKIE (Critical for Admin Page access)
    response.set_cookie(
        key="access_token", 
        value=token_str, 
        httponly=True,   # Secure: JS cannot read it
        max_age=86400,   # 1 day
        samesite="lax"
    )
    
    return {"access_token": access_token, "token_type": "bearer"}

# 2. Register Endpoint (PROTECTED - Only logged in users can create new users)
class UserRegister(BaseModel):
    username: str
    password: str

class ExploreRequest(BaseModel):
    log_id: int
    action_type: str  # 'dimension' or 'measure'
    key: str
    agg: Optional[str] = None # e.g., 'SUM', 'COUNT'


def generate_id():
    return int(time.time() * 1000)

@router.post("/api/register")
async def register(
    user: UserRegister, 
    db = Depends(get_db), 
    current_user: User = Depends(get_current_user) 
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
    return templates.TemplateResponse("chat.html", {"request": request})

# --- PROTECTED PAGES (Admin Only) ---

@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, current_user: User = Depends(get_current_user)):
    return templates.TemplateResponse("register.html", {"request": request, "user": current_user})

@router.get("/admin/logs", response_class=HTMLResponse)
async def view_logs(
    request: Request, 
    db = Depends(get_db), 
    page: int = 1, 
    username: str = "",
    current_user: User = Depends(get_current_user) 
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
async def knowledge_base(request: Request, current_user: User = Depends(get_current_user)): 
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
    # 1. Warm up cache (optional but good practice)
    try:
        await DateResolver.get_date_context()
    except:
        pass
    
    user_msg = payload.message.strip()
    
    # 2. Run Pipeline
    # FIX: Pass the RAW payload.history list, NOT a string.
    # The Orchestrator needs the list for Context Resolution.
    orchestrator = Orchestrator(user=current_user.username)
    result = await orchestrator.run_pipeline(user_msg, payload.history)
    
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


@router.get("/admin/qa", response_class=HTMLResponse)
async def qa_tribunal_page(request: Request, current_user: User = Depends(get_current_user)):
    """
    Renders the AI Tribunal Dashboard.
    """
    return templates.TemplateResponse("admin_qa.html", {
        "request": request, 
        "user": current_user
    })

@router.post("/api/explore/transform")
async def explore_transform(req: ExploreRequest, current_user=Depends(get_current_user)):
    """
    Enterprise Semantic Layer: Slices or Swaps Metrics deterministically via AST.
    """
    orchestrator = Orchestrator(user=current_user.username)
    result = await orchestrator.explore_action(req.log_id, req.action_type, req.key, req.agg)
    return result


# --- OPTION B: DASHBOARD ENDPOINTS ---

class PinRequest(BaseModel):
    log_id: int
    title: str

@router.post("/api/dashboard/pin")
async def pin_chart_to_dashboard(req: PinRequest, db = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Pins a specific chart to the user's dashboard."""
    # 1. Verify the log exists
    log = db.query(ChatLog).filter(ChatLog.id == req.log_id).first()
    if not log:
        raise HTTPException(status_code=404, detail="Chart not found")
        
    # 2. Prevent duplicate pins for the same user
    existing = db.query(PinnedChart).filter(
        PinnedChart.username == current_user.username, 
        PinnedChart.log_id == req.log_id
    ).first()
    
    if existing:
        return {"status": "already_pinned", "message": "Chart is already on your dashboard."}
        
    # 3. Save the Pin
    new_pin = PinnedChart(
        username=current_user.username, 
        log_id=req.log_id, 
        title=req.title
    )
    db.add(new_pin)
    db.commit()
    
    return {"status": "success", "message": "Pinned to dashboard!"}

@router.delete("/api/dashboard/unpin/{pin_id}")
async def unpin_chart(pin_id: int, db = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Removes a chart from the user's dashboard."""
    pin = db.query(PinnedChart).filter(
        PinnedChart.id == pin_id, 
        PinnedChart.username == current_user.username
    ).first()
    
    if not pin:
        raise HTTPException(status_code=404, detail="Pin not found or unauthorized")
        
    db.delete(pin)
    db.commit()
    return {"status": "success", "message": "Chart removed."}

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request, current_user: User = Depends(get_current_user)):
    """Renders the empty Dashboard shell."""
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": current_user})

@router.get("/api/dashboard/list")
async def get_dashboard_list(db = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Returns the metadata of all pinned charts for the logged-in user."""
    pins = db.query(PinnedChart).filter(PinnedChart.username == current_user.username).order_by(PinnedChart.created_at.desc()).all()
    
    return [{
        "pin_id": pin.id,
        "log_id": pin.log_id,
        "title": pin.title
    } for pin in pins]

@router.get("/api/dashboard/refresh/{log_id}")
async def refresh_dashboard_chart(log_id: int, db = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Re-runs the SQL for a pinned chart to get live data."""
    # 1. Security: Ensure the user actually pinned this chart
    pin = db.query(PinnedChart).filter(
        PinnedChart.username == current_user.username, 
        PinnedChart.log_id == log_id
    ).first()
    
    if not pin:
        raise HTTPException(status_code=403, detail="Unauthorized chart access")
        
    # 2. Fetch the deterministically saved SQL
    log = db.query(ChatLog).filter(ChatLog.id == log_id).first()
    if not log or not log.generated_sql:
        raise HTTPException(status_code=404, detail="Original SQL not found")
        
    # 3. Execute live data
    df = await vn.run_sql_async(log.generated_sql)
    
    if df is None or df.empty:
        return {"type": "success", "visual_type": "text", "message": "No data available."}
        
    # 4. Format for UI
    import numpy as np
    try: df.replace([np.inf, -np.inf], np.nan, inplace=True) 
    except: pass
    
    from app.pipeline.agents.visualization import VisualizationAgent
    # Auto-detect the best chart format for the live data
    viz_result = await VisualizationAgent.determine_format(df, log.generated_sql, "Dashboard Refresh")
    visual_type = viz_result.get("visual_type", "table")
    
    df_safe = df.where(pd.notnull(df), None)
    safe_data = df_safe.head(5000 if visual_type == "plotly" else 100).to_dict(orient='records')
    
    return {
        "type": "success",
        "visual_type": visual_type,
        "plotly_code": viz_result.get("plotly_code"),
        "data": safe_data
    }