# app/api/qa_routes.py
from fastapi import APIRouter, Request, HTTPException
from app.tests.service import QAService

router = APIRouter()

@router.post("/run")
async def run_qa_test(request: Request):
    """
    Triggers the AI Tribunal. Payload: {"count": 5}
    """
    try:
        body = await request.json()
        count = body.get("count", 5)
        # Cap at 50 to prevent timeout/abuse
        if count > 50: count = 50
        
        result = await QAService.run_suite(count=count)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history")
async def get_qa_history():
    """
    Fetches the scorecard history.
    """
    try:
        return QAService.get_history()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))