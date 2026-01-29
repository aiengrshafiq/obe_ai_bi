from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Dict, Any
import pandas as pd
import json
from datetime import datetime, timedelta

# Import Vanna and Database Tools
from app.services.vanna_wrapper import vn
from app.db.raw_executor import execute_raw_sql # Still needed for async partition check

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

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
async def chat_endpoint(payload: ChatRequest):
    user_msg = payload.message
    
    # 1. Inject Context (The "Intelligence" Layer)
    latest_ds = await get_current_ds()
    
    # We prepend the constraint so Vanna knows the date context
    contextualized_question = (
        f"The latest valid partition is ds='{latest_ds}'. "
        f"Always filter by this ds unless the user asks for history. "
        f"Question: {user_msg}"
    )
    
    print(f"🤖 Vanna Asking: {contextualized_question}")

    try:
        # 2. Generate SQL (RAG + LLM)
        # Vanna automatically searches the vector DB for the right tables
        sql_query = await vn.generate_sql_async(question=contextualized_question)
        
        if not sql_query or "SELECT" not in sql_query.upper():
            return {
                "type": "error",
                "message": "I couldn't generate a valid query. Please try rephrasing."
            }

        # 3. Execute SQL
        # This uses the Synchronous Engine via our wrapper
        df = await vn.run_sql_async(sql_query)
        
        # Handle Empty Data
        if df is None or df.empty:
            return {
                "type": "success",
                "thought": "Query executed successfully but returned no data.",
                "sql": sql_query,
                "data": [],
                "visual_type": "none"
            }

        # 4. Generate Chart Code (The "Manager's Requirement")
        # Vanna returns Python code string for Plotly
        plotly_code = await vn.generate_plotly_code_async(
            question=user_msg, 
            sql=sql_query, 
            df=df
        )
        
        # Convert Dataframe to JSON-friendly dict for the UI table
        table_data = df.head(100).to_dict(orient='records')
        
        # Determine Visual Type for Frontend
        visual_type = "table"
        if len(df) > 1 and len(df.columns) >= 2:
            visual_type = "plotly"  # Tell frontend to look for Plotly code
        
        return {
            "type": "success",
            "thought": f"Generated SQL based on logic for ds='{latest_ds}'",
            "sql": sql_query,
            "data": table_data,
            "visual_type": visual_type,
            "plotly_code": plotly_code # This is the Python code string
        }

    except Exception as e:
        print(f"❌ Chat Error: {e}")
        return {
            "type": "error", 
            "message": f"An error occurred: {str(e)}"
        }

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