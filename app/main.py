# app/main.py
import os
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.routers import web_ui
from app.db.vanna_db import setup_vanna_db_connection
# REMOVED: from app.services.vanna_training import train_vanna_on_startup

# --- 1. LIFESPAN MANAGER ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🟢 System Starting Up...")
    
    # A. Connectivity Check
    try:
        setup_vanna_db_connection()
        print("✅ Vanna DB Connectivity: OK")
    except Exception as e:
        print(f"❌ Critical Error: Vanna DB Connection Failed. {e}")
        # In strict mode, raise e here.

    # B. Knowledge Base
    # We now trust the 'vanna_storage' baked into the Docker image.
    print("🧠 Knowledge Base: Loaded from Build Image (vanna_storage).")

    yield
    
    print("🔴 System Shutting Down...")

# --- 2. INITIALIZE APP ---
app = FastAPI(
    title=settings.PROJECT_NAME, 
    version=settings.VERSION,
    lifespan=lifespan
)

# --- 3. MOUNT STATIC & TEMPLATES ---
templates = Jinja2Templates(directory="app/templates")

static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# --- 4. MIDDLEWARE (CORS) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 5. ROUTERS ---
app.include_router(web_ui.router)

# --- 6. ENTRY POINT ---
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)