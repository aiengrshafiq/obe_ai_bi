from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from app.tests.service import QAService

router = APIRouter()

@router.post("/run")
async def run_qa_test(request: Request):
    try:
        body = await request.json()
        count = body.get("count", 5)
        if count > 50: count = 50
        
        # Run logic
        result = await QAService.run_suite(count=count)
        return result
        
    except Exception as e:
        # Return JSON error instead of crashing (avoids < error)
        return JSONResponse(
            status_code=500, 
            content={"detail": f"Tribunal Crash: {str(e)}"}
        )

@router.get("/history")
async def get_qa_history():
    try:
        return QAService.get_history()
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})