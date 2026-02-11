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
from app.services.vanna_training import train_vanna_on_startup

# --- 1. LIFESPAN MANAGER (The Modern Way) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the application lifecycle.
    Code before 'yield' runs on startup.
    Code after 'yield' runs on shutdown.
    """
    print("🟢 System Starting Up...")
    
    # A. Setup Vanna Connection
    try:
        setup_vanna_db_connection()
        print("✅ Vanna DB Connected")
    except Exception as e:
        print(f"❌ Critical Error: Could not connect to Vanna DB. {e}")
        # In strict product mode, you might want to raise e here to stop deployment
    
    # B. Auto-Train Knowledge Base (Smart Training)
    try:
        await train_vanna_on_startup(force_retrain=True)
    except Exception as e:
        print(f"⚠️ Warning: Knowledge Base training failed. System will run with existing data. {e}")

    yield
    
    print("🔴 System Shutting Down...")
    # Clean up resources (e.g., close DB pools) here if needed later

# --- 2. INITIALIZE APP ---
app = FastAPI(
    title=settings.PROJECT_NAME, 
    version=settings.VERSION,
    lifespan=lifespan # Inject the lifecycle manager
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
    allow_origins=["*"], # In strict production, replace "*" with specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 5. ROUTERS ---
app.include_router(web_ui.router)

# --- 6. ENTRY POINT ---
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)