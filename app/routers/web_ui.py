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
from app.db.raw_executor import execute_raw_sql
from app.db.app_models import SessionLocal, ChatLog, User
from app.services.auth import verify_token, get_password_hash, verify_password, create_access_token # We will create this next

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# --- 1. SIMPLE CACHE FOR DATE PARTITION ---
# This prevents hitting the DB on every request.
_DS_CACHE = {"value": None, "expires": 0}

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


def generate_id():
    return int(time.time() * 1000)

@router.post("/api/register")
async def register(user: UserRegister, db = Depends(get_db)):
    if db.query(User).filter(User.username == user.username).first():
        raise HTTPException(status_code=400, detail="User already exists")
    new_user = User(id=generate_id(), username=user.username, hashed_password=get_password_hash(user.password))
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
    current_time = time.time()
    
    # Return cached value if valid (TTL: 1 hour)
    if _DS_CACHE["value"] and current_time < _DS_CACHE["expires"]:
        return _DS_CACHE["value"]

    try:
        # DB Hit
        sql = "SELECT max(ds) as max_ds FROM public.user_profile_360"
        result = await execute_raw_sql(sql)
        if result and result[0]['max_ds']:
            new_ds = result[0]['max_ds']
            # Update Cache (Expires in 3600 seconds = 1 hour)
            _DS_CACHE["value"] = new_ds
            _DS_CACHE["expires"] = current_time + 3600
            print(f"🔄 Refreshed Partition Cache: {new_ds}")
            return new_ds
            
    except Exception as e:
        print(f"⚠️ Partition Check Failed: {e}")
    
    # Fallback
    return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

@router.post("/api/chat")
async def chat_endpoint(
    payload: ChatRequest, 
    current_user: User = Depends(get_current_user), # Secured
    db = Depends(get_db)
):
    user_msg = payload.message.strip()
    
    # 1. Inject Context (The "Intelligence" Layer)
    latest_ds = await get_current_ds()
    today_iso = datetime.now().strftime("%Y-%m-%d") # Actual Calendar Date


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
    - You are the Data Analyst for **OneBullEx (OBE)**. All questions refer to this crypto exchange.
    - Today's Date is: {today_iso} (Use this for "yesterday", "last 7 days").
    - Latest Snapshot Partition: ds='{latest_ds}'.
    
    
    CRITICAL INSTRUCTIONS:
    1. If the question is clear and related to data, generate the SQL.
    2. If the question is AMBIGUOUS (e.g., "Show me top users" without metric), DO NOT GENERATE SQL.
       Instead, reply exactly like this: "CLARIFICATION: <Your question to the user>"
    3. If the question is conversational (e.g., "Hello", "Thanks"), reply exactly like this: 
       "CLARIFICATION: Hello! I am your Data Copilot. Ask me about Users, Volume, or Points."
    4. **User Code is STRING:** Always use quotes: `user_code = '12345'`. NEVER use numbers.
    5. **Identity:** If user asks about "OneBullEx", "OBE", or "Platform", query the data normally. Do NOT ask for clarification.
    6. **Granularity:** If user asks for "Hourly" or "Last X hours", check if the table has a timestamp column (like `create_at`). 
       - If YES: Generate SQL using `HOUR(create_at)` or `NOW() - INTERVAL 'X hours'`. 
       - If NO: Generate SQL for the daily trend instead using `ds`.

    CRITICAL SQL RULES:
    1. **Snapshots:** For "Current Status" (e.g. "total users", "balance"), YOU MUST filter by `ds='{latest_ds}'`.
    2. **History/Trends:** If user asks for "Trend", "History", "Last Month", or "MAU", you MAY filter by a date range (e.g. `ds BETWEEN ...`).
    3. **Funnels:** Use `UNION ALL` to stack stages vertically.
    4. **Formatting:** `user_code` is STRING (use quotes).

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
        #sql_query = await vn.generate_sql_async(question=full_prompt)
        ai_response = await vn.generate_sql_async(question=full_prompt)

        # 3. CHECK FOR CLARIFICATION (The Logic Switch)
        if ai_response.strip().upper().startswith("CLARIFICATION") or "SELECT" not in ai_response.upper():
            clean_msg = ai_response.replace("CLARIFICATION:", "").strip()
            
            # Log it as a success (valid interaction)
            log_entry.generated_sql = "TEXT_RESPONSE: " + clean_msg
            log_entry.execution_success = True 
            db.commit()
            
            return {
                "type": "text", 
                "message": clean_msg,
                "visual_type": "none"
            }

        # --- THE FIX: DETERMINISTIC REPLACEMENT ---
        # If the AI was lazy and returned the placeholder '{latest_ds}' 
        # instead of the value, we fix it here manually.
        
        # 1. Fix partition ds
        final_sql = ai_response.replace("{latest_ds}", latest_ds)
        
        # 2. Fix dashed dates (used in User Cube for NRU)
        latest_ds_dash = f"{latest_ds[:4]}-{latest_ds[4:6]}-{latest_ds[6:]}"
        final_sql = final_sql.replace("{latest_ds_dash}", latest_ds_dash)
        
        # 3. Fix today's date placeholder if it exists
        final_sql = final_sql.replace("{today_iso}", today_iso)

        # 4. FORCE ROW LIMIT (The Safety Brake)
        # If the query is a SELECT and doesn't have a LIMIT, we add one.
        # We use a simple check to avoid breaking complex subqueries, 
        # but for top-level generation, this catches 99% of "Select All" risks.
        if "LIMIT" not in final_sql.upper() and "SELECT" in final_sql.upper():
            # Remove trailing semicolon if present
            final_sql = final_sql.strip().rstrip(';')
            final_sql += " LIMIT 100;"
        # ------------------------------------------
        # Log Generated SQL
        log_entry.generated_sql = final_sql
        db.commit()

        # 3. Execute SQL
        # This uses the Synchronous Engine via our wrapper
        df = await vn.run_sql_async(final_sql)

        # Log Success
        log_entry.execution_success = True
        db.commit()
        
        # Handle Empty Data
        if df is None or df.empty:
            return {
                "type": "success",
                "thought": "Query executed successfully but returned no data.",
                "sql": final_sql,
                "data": [],
                "visual_type": "none"
            }

        # --- CRITICAL FIX: SANITIZE DATA FOR JSON ---
        # Python JSON cannot handle NaN or Infinity. We replace them with 0 or None.
        try:
            # Replace Infinity with 0
            df.replace([float('inf'), float('-inf')], 0, inplace=True)
            # Replace NaN (Nulls) with 0 (or use None if you prefer strict nulls)
            df.fillna(0, inplace=True)
        except Exception as cleanup_err:
            print(f"⚠️ Data Cleanup Warning: {cleanup_err}")
        # --------------------------------------------

        
        table_data = df.head(100).to_dict(orient='records')
        visual_type = "table"
        plotly_code = ""

        # Logic Config
        row_count = len(df)
        col_count = len(df.columns)
        user_lower = user_msg.lower()
        sql_upper = final_sql.upper()
        is_funnel = "funnel" in user_lower or "stage" in df.columns or "step" in df.columns
        
        # Detect Forced Format
        force_table = any(x in user_lower for x in ["table", "list", "records", "details", "sample"])
        force_chart = any(x in user_lower for x in ["chart", "graph", "plot", "trend", "visualize"])
        # 3. Detect SQL Semantics (Is it Analytical?)
        # Analytical Queries usually have GROUP BY or Aggregates (SUM, COUNT, AVG)
        has_aggregates = any(x in sql_upper for x in ["GROUP BY", "SUM(", "COUNT(", "AVG(", "MIN(", "MAX("])
        
        # A raw select is usually "SELECT * FROM..." or "SELECT col1, col2..." without math
        is_raw_select = ("SELECT *" in sql_upper) or (not has_aggregates)

        thought_msg = f"Generated SQL based on logic for ds='{latest_ds}'"

        # --- DECISION TREE ---

        if row_count == 0:
            visual_type = "none"

        # A. Funnels
        elif is_funnel:
            visual_type = "plotly"
            chart_instructions = "Create a Funnel Chart. Use the text column for stages and numeric column for values."
            plotly_code = await vn.generate_plotly_code_async(chart_instructions, final_sql, df)

        # B. Raw Data / Forced Table (Fixes "Show Random Records")
        # If user asks for records OR the SQL is just a raw SELECT -> Show Table
        elif force_table or (is_raw_select and not force_chart):
            visual_type = "table"
            thought_msg += " (Showing Table: Detected raw data query or user request)"

        # C. User Forced Chart
        elif force_chart:
            if row_count == 1 and col_count < 2:
                visual_type = "table"
                thought_msg += " (User requested Chart, but data is insufficient. Showing Table.)"
            else:
                visual_type = "plotly"
                plotly_code = await vn.generate_plotly_code_async(f"Visualize this data. Question: {user_msg}", final_sql, df)

        # D. Single Row Result -> Always Table
        elif row_count == 1:
            visual_type = "table"

        # E. Analytical Data (Grouped/Aggregated) -> Chart
        elif has_aggregates and row_count > 1:
            visual_type = "plotly"
            chart_instructions = (
                f"Visualize this data. "
                f"User Question: '{user_msg}'. "
                f"Rules: "
                f"1. If multiple metrics (e.g. deposit, trade), use Grouped Bar. "
                f"2. Format date x-axis as '%Y-%m-%d'. "
                f"3. Descriptive title."
            )
            plotly_code = await vn.generate_plotly_code_async(chart_instructions, final_sql, df)

        # F. Fallback -> Table
        else:
            visual_type = "table"

        return {
            "type": "success",
            "thought": thought_msg,
            "sql": final_sql,
            "data": table_data,
            "visual_type": visual_type,
            "plotly_code": plotly_code
        }


        

    except Exception as e:
        # Log the full error internally for you
        error_msg = str(e)
        print(f"❌ Internal Error: {error_msg}")
        
        log_entry.error_message = error_msg
        db.commit()
        
        # Sanitize for User
        user_friendly_msg = "An internal error occurred while processing your request."
        
        # Pass through specific useful errors (like your Guardrail blocks)
        if "Security Alert" in error_msg:
            user_friendly_msg = error_msg
        elif "No data found" in error_msg:
             user_friendly_msg = "I couldn't find any data matching that request."

        return {"type": "error", "message": user_friendly_msg}



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

# --- 2. UPDATED ADMIN LOGS ENDPOINT (Pagination & Filter) ---
@router.get("/admin/logs", response_class=HTMLResponse)
async def view_logs(
    request: Request, 
    db = Depends(get_db), 
    page: int = 1, 
    username: str = ""
):
    page_size = 20
    query = db.query(ChatLog)
    
    # Filter
    if username:
        query = query.filter(ChatLog.username.ilike(f"%{username}%"))
    
    # Sort & Paginate
    total_records = query.count()
    logs = query.order_by(desc(ChatLog.timestamp))\
                .offset((page - 1) * page_size)\
                .limit(page_size)\
                .all()
    
    total_pages = (total_records + page_size - 1) // page_size
    
    return templates.TemplateResponse("admin_logs.html", {
        "request": request, 
        "logs": logs,
        "page": page,
        "total_pages": total_pages,
        "username": username
    })

@router.get("/admin/knowledge_base", response_class=HTMLResponse)
async def knowledge_base(request: Request):
    return templates.TemplateResponse("knowledge_base.html", {"request": request})


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Serves the Login HTML page"""
    return templates.TemplateResponse("login.html", {"request": request})

