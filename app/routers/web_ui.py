# app/routers/web_ui.py
from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
import json

# Internal imports
from app.services.vanna_wrapper import vn
from app.db.raw_executor import execute_raw_sql
from app.db.app_models import SessionLocal, ChatLog, User
from app.services.auth import verify_token, get_password_hash, verify_password, create_access_token # We will create this next

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
        raise HTTPException(status_code=401, detail="Invalid authentication")
    return user

# 1. Login Endpoint
@router.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db = Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

# 2. Register Endpoint (For initial setup)
class UserRegister(BaseModel):
    username: str
    password: str

@router.post("/api/register")
async def register(user: UserRegister, db = Depends(get_db)):
    if db.query(User).filter(User.username == user.username).first():
        raise HTTPException(status_code=400, detail="User already exists")
    new_user = User(username=user.username, hashed_password=get_password_hash(user.password))
    db.add(new_user)
    db.commit()
    return {"status": "User created"}

# 3. Chat Request Model
class ChatRequest(BaseModel):
    message: str
    history: List[Dict[str, str]] = []



@router.get("/")
async def get_chat_ui(request: Request):
    return templates.TemplateResponse("chat.html", {"request": request})

# --- HELPER: Get Latest Partition ---
async def get_current_ds() -> str:
    """
    Checks the DB for the latest partition. 
    Crucial for the 'Yesterday Rule'.
    """
    try:
        # We use the raw async executor for speed
        sql = "SELECT max(ds) as max_ds FROM public.user_profile_360"
        result = await execute_raw_sql(sql)
        if result and result[0]['max_ds']:
            return result[0]['max_ds']
    except Exception as e:
        print(f"⚠️ Partition Check Failed: {e}")
    
    # Fallback to yesterday
    return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

@router.post("/api/chat")
async def chat_endpoint(
    payload: ChatRequest, 
    current_user: User = Depends(get_current_user), # Secured
    db = Depends(get_db)
):
    user_msg = payload.message
    
    # 1. Inject Context (The "Intelligence" Layer)
    latest_ds = await get_current_ds()


    # 2. Build Contextual Prompt (The Fix for "Chain of Questions")
    # We explicitly tell Vanna the history.
    history_context = ""
    if payload.history:
        # Take last 2 turns to avoid token overflow
        recent = payload.history[-4:] 
        history_context = "PREVIOUS CONVERSATION:\n" + "\n".join(
            [f"{msg['role'].upper()}: {msg['content']}" for msg in recent]
        )
    
    full_prompt = f"""
    {history_context}
    
    CURRENT CONTEXT:
    - Today's date partition is ds='{latest_ds}'.
    - If the user asks about "users" or "they" without context, refer to the PREVIOUS SQL generated.
    - If the question is completely unrelated to data (e.g. "hi", "weather"), return SQL: SELECT 'Greeting' as msg
    
    NEW QUESTION: {user_msg}
    """
    
    print(f"🤖 {current_user.username} is asking: {user_msg}")

    # 3. Initialize Log
    log_entry = ChatLog(
        username=current_user.username,
        user_question=user_msg,
        context_provided=history_context
    )
    db.add(log_entry)
    db.commit() # Get ID

    

    try:
        # 2. Generate SQL (RAG + LLM)
        # Vanna automatically searches the vector DB for the right tables
        sql_query = await vn.generate_sql_async(question=full_prompt)

        # Check for non-data questions
        if "SELECT 'Greeting'" in sql_query:
             return {"type": "text", "message": "Hello! I am your Data Copilot. Ask me about Users, Trades, or Points."}
        
        if not sql_query or "SELECT" not in sql_query.upper():
            return {
                "type": "error",
                "message": "I couldn't generate a valid query. Please try rephrasing."
            }

        # Log Generated SQL
        log_entry.generated_sql = sql_query
        db.commit()

        # 3. Execute SQL
        # This uses the Synchronous Engine via our wrapper
        df = await vn.run_sql_async(sql_query)

        # Log Success
        log_entry.execution_success = True
        db.commit()
        
        # Handle Empty Data
        if df is None or df.empty:
            return {
                "type": "success",
                "thought": "Query executed successfully but returned no data.",
                "sql": sql_query,
                "data": [],
                "visual_type": "none"
            }

        plotly_code = await vn.generate_plotly_code_async(user_msg, sql_query, df)
        table_data = df.head(100).to_dict(orient='records')
        visual_type = "table"
        if len(df) > 1 and len(df.columns) >= 2: visual_type = "plotly"

        return {
            "type": "success",
            "thought": f"Generated SQL based on logic for ds='{latest_ds}'",
            "sql": sql_query,
            "data": table_data,
            "visual_type": visual_type,
            "plotly_code": plotly_code # This is the Python code string
        }

    except Exception as e:
        log_entry.error_message = str(e)
        db.commit()
        return {"type": "error", "message": str(e)}

# --- Custom SQL Endpoint (Simplified for Vanna) ---
class CustomSQLRequest(BaseModel):
    sql_query: str

@router.post("/api/run_custom_sql")
async def run_custom_sql_endpoint(payload: CustomSQLRequest):
    sql = payload.sql_query.strip()
    
    if any(x in sql.lower() for x in ["drop", "delete", "insert", "update", "truncate"]):
        return {"type": "error", "message": "Unsafe query detected."}

    try:
        # Run via Vanna wrapper
        df = await vn.run_sql_async(sql)
        
        if df is None or df.empty:
             return {"type": "success", "sql": sql, "data": [], "visual_type": "table"}

        # Auto-chart for custom queries
        visual_type = "table"
        plotly_code = ""
        if len(df) > 1 and len(df.columns) >= 2:
            visual_type = "plotly"
            # We assume a generic question for custom SQL to get a generic chart
            plotly_code = await vn.generate_plotly_code_async(
                question="Visualize this data", 
                sql=sql, 
                df=df
            )

        return {
            "type": "success",
            "sql": sql,
            "data": df.head(100).to_dict(orient='records'),
            "visual_type": visual_type,
            "plotly_code": plotly_code
        }

    except Exception as e:
        return {"type": "error", "message": str(e)}

# 4. Admin Log Viewer
@router.get("/admin/logs")
async def view_logs(request: Request, db = Depends(get_db)):
    # Simple HTML table dump for debugging
    logs = db.query(ChatLog).order_by(ChatLog.timestamp.desc()).limit(50).all()
    return templates.TemplateResponse("admin_logs.html", {"request": request, "logs": logs})

@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})