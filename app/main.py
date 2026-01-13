import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings

from app.routers import web_ui

# Initialize App
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION
)

# 1. CORS Middleware (Allow all for local dev, restrict in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(web_ui.router)


# 2. Mount Static Files (CSS/JS)
# Ensure the directory exists to avoid startup errors
static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# 3. Simple Health Check
@app.get("/health")
async def health_check():
    return {"status": "ok", "app_name": settings.PROJECT_NAME}

# 4. Include Routers (We will add these in the next step)
# from app.routers import web_ui, api_chat
# app.include_router(web_ui.router)
# app.include_router(api_chat.router, prefix="/api/v1")

if __name__ == "__main__":
    import uvicorn
    # Auto-reload enabled for local development
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)