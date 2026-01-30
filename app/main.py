# app/main.py
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.routers import web_ui
import uvicorn

# Import Services
from app.db.vanna_db import setup_vanna_db_connection
from app.services.vanna_training import train_vanna_on_startup

# 1. Initialize App & Templates
app = FastAPI(title=settings.PROJECT_NAME, version=settings.VERSION)
templates = Jinja2Templates(directory="app/templates")

# 2. Mount Static
static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir): os.makedirs(static_dir)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# 3. Setup Vanna DB Connection (The Bridge)
setup_vanna_db_connection()

# 4. Register Startup Event (Auto-Train)
@app.on_event("startup")
async def startup_event():
    await train_vanna_on_startup(force_retrain=False)

# 5. CORS (Keep this)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DELETED: AuthMiddleware (It was causing the loop) ---
# The security is now handled inside web_ui.py routers.

# 6. Include Router
app.include_router(web_ui.router)

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)