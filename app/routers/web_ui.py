from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from app.services.llm_service import LLMService
from app.db.metadata_store import get_ddl_context
from app.db.raw_executor import execute_raw_sql

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# Initialize Service
llm_service = LLMService()

class ChatRequest(BaseModel):
    message: str

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
    
    # 1. Get Context
    ddl = get_ddl_context()
    
    # 2. Call AI (Pydantic Schema ensures structure)
    plan = llm_service.generate_sql(user_msg, ddl)
    
    if not plan.is_safe:
        return {
            "type": "error",
            "message": "I cannot answer this question. It might be asking for data outside my permission scope.",
            "thought": plan.thought_process
        }

    # 3. Execute SQL (If valid)
    data = []
    if plan.sql_query:
        # Safety check: Ensure strict read-only
        if "drop" in plan.sql_query.lower() or "delete" in plan.sql_query.lower():
             return {"type": "error", "message": "Unsafe query detected."}
             
        data = await execute_raw_sql(plan.sql_query)

    return {
        "type": "success",
        "thought": plan.thought_process,
        "sql": plan.sql_query,
        "visual_type": plan.visualization_type,
        "data": data
    }