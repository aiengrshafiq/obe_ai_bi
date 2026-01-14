import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

class Settings:
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "OneBullEx AI Copilot")
    VERSION: str = os.getenv("VERSION", "1.0.0")
    
    # 1. Database Credentials
    RAW_USER: str = os.getenv("DB_USER", "postgres")
    RAW_PASS: str = os.getenv("DB_PASS", "postgres")
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: str = os.getenv("DB_PORT", "5432")
    DB_NAME: str = os.getenv("DB_NAME", "onebullex_local")

    # 2. Encode to handle special chars
    DB_USER_ENC = quote_plus(RAW_USER) if RAW_USER else ""
    DB_PASS_ENC = quote_plus(RAW_PASS) if RAW_PASS else ""

    # 3. Construct Async URL
    DATABASE_URL: str = f"postgresql+asyncpg://{DB_USER_ENC}:{DB_PASS_ENC}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # 4. Security & AI
    SECRET_KEY: str = os.getenv("SECRET_KEY", "supersecretkey")
    ALGORITHM: str = os.getenv("ALGORITHM", "HS256")
    DASHSCOPE_API_KEY: str = os.getenv("DASHSCOPE_API_KEY") # Qwen Key
    # print(f"DEBUG: Loaded Key: '{DASHSCOPE_API_KEY}'")

    # 1. Exact Endpoint from your Curl
    AI_BASE_URL: str = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
    
    # 2. Exact Model Name from your Curl
    AI_MODEL_NAME: str = "qwen3-coder-plus"

    APP_ACCESS_CODE: str = os.getenv("APP_ACCESS_CODE", "admin")


settings = Settings()