import os
from fastapi import FastAPI, Request, Form, Response
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from app.core.config import settings
from app.routers import web_ui

# 1. Initialize App & Templates
app = FastAPI(title=settings.PROJECT_NAME, version=settings.VERSION)
templates = Jinja2Templates(directory="app/templates")

# 2. Mount Static
static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir): os.makedirs(static_dir)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# 3. Security Middleware (Protect everything except /login and /static)
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Allow these paths without login
        if request.url.path in ["/login", "/health"] or request.url.path.startswith("/static"):
            return await call_next(request)
        
        # Check Cookie
        auth_cookie = request.cookies.get("auth_token")
        if auth_cookie != settings.APP_ACCESS_CODE:
            return RedirectResponse(url="/login")
        
        return await call_next(request)

app.add_middleware(AuthMiddleware)

# 4. CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 5. Login Routes
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_submit(response: Response, access_code: str = Form(...)):
    if access_code == settings.APP_ACCESS_CODE:
        # Success: Set Cookie and Redirect
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(key="auth_token", value=access_code, max_age=86400) # 1 Day expiry
        return response
    else:
        # Fail
        return RedirectResponse(url="/login?error=1", status_code=302)

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login")
    response.delete_cookie("auth_token")
    return response

    
@app.get("/health")
async def health_check():
    return {"status": "ok", "app_name": settings.PROJECT_NAME}

# 6. Include Main Router
app.include_router(web_ui.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)