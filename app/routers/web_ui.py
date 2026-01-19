from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from app.services.llm_service import LLMService
from app.db.metadata_store import get_ddl_context
from app.db.raw_executor import execute_raw_sql
from typing import List, Optional, Dict
from app.db.raw_executor import execute_raw_sql


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# Initialize Service
llm_service = LLMService()

class ChatRequest(BaseModel):
    message: str
    # New Field: Optional list of previous messages
    # Format: [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]
    history: List[Dict[str, str]] = []

@router.get("/")
async def get_chat_ui(request: Request):
    return templates.TemplateResponse("chat.html", {"request": request})

@router.post("/api/chat")
async def chat_endpoint(payload: ChatRequest):
    """
    Orchestrator Logic:
    1. User Question -> LLM -> JSON Plan
    2. JSON Plan -> Execute SQL -> Result
    3. Result -> Return to UI
    """
    user_msg = payload.message
    history = payload.history
    
    # 1. Get Context
    ddl = get_ddl_context()

    # --- DEBUG PRINT ---
    print(f"DEBUG: Current DDL Size: {len(ddl)} chars")
    if "dws_user_deposit_withdraw_detail_di" in ddl:
        print("CRITICAL ERROR: Old DDL is still loaded!")
    else:
        print("SUCCESS: New DDL is loaded.")
    # -------------------
    
    # 2. Call AI (Pydantic Schema ensures structure)
    plan = await llm_service.generate_sql(user_msg, ddl, history)
    
    if not plan.is_safe:
        return {
            "type": "error",
            "message": "I cannot answer this question. It might be asking for data outside my permission scope.",
            "thought": plan.thought_process
        }
    
    # 3. Execute SQL (If valid)
    data = []
    insight = ""
    if plan.sql_query:
        # Safety check: Ensure strict read-only
        if "drop" in plan.sql_query.lower() or "delete" in plan.sql_query.lower():
             return {"type": "error", "message": "Unsafe query detected."}
             
        data = await execute_raw_sql(plan.sql_query)

        # 4. Generate Insight (NEW STEP)
        # Only generate if we have data and it's not a huge raw dump
        if data and len(data) > 0:
            insight = llm_service.generate_insight(user_msg, data)

    return {
        "type": "success",
        "thought": plan.thought_process,
        "sql": plan.sql_query,
        
        # Pass the new visual config
        "visual_type": plan.visualization_type,
        "x_axis": plan.chart_x_axis,
        "y_axis": plan.chart_y_axis,
        "title": plan.chart_title,
        
        "data": data,
        "insight": insight
    }


# 1. Request Model for Custom SQL
class CustomSQLRequest(BaseModel):
    sql_query: str

# 2. New Endpoint
@router.post("/api/run_custom_sql")
async def run_custom_sql_endpoint(payload: CustomSQLRequest):
    """
    Executes SQL manually edited by the user.
    """
    sql = payload.sql_query.strip()
    
    # Basic Safety Checks
    if not sql.lower().startswith("select"):
        return {"type": "error", "message": "Only SELECT statements are allowed."}
    if any(x in sql.lower() for x in ["drop", "delete", "insert", "update", "truncate"]):
        return {"type": "error", "message": "Unsafe query detected."}

    try:
        # Execute
        data = await execute_raw_sql(sql)
        
        # Auto-detect visualization (Simple logic)
        visual_type = "table"
        if len(data) > 0:
            keys = list(data[0].keys())
            # If 2 columns and one is number -> Bar Chart
            if len(keys) == 2 and any(isinstance(data[0][k], (int, float)) for k in keys):
                visual_type = "bar"
            # If date detected -> Line Chart
            if "date" in keys[0].lower() or "time" in keys[0].lower() or "ds" in keys[0].lower():
                visual_type = "line"
                
        return {
            "type": "success",
            "sql": sql,
            "data": data,
            "visual_type": visual_type,
            "title": "Custom Query Result"
        }

    except Exception as e:
        return {"type": "error", "message": str(e)}