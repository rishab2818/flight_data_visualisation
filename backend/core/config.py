# backend/core/config.py
import os
from pathlib import Path

# 1) Load .env explicitly (python-dotenv)
try:
    from dotenv import load_dotenv, find_dotenv
except ImportError:
    raise RuntimeError("Install python-dotenv: pip install python-dotenv")

# Prefer a .env next to backend/ (i.e., backend/.env)
BACKEND_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BACKEND_DIR / ".env"

# Use find_dotenv as a fallback (cwd) if not found at expected place
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=False)
else:
    load_dotenv(find_dotenv(filename=".env", usecwd=True), override=False)

def _get_env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else v

class Settings:
    DATABASE_URL: str = _get_env("DATABASE_URL")  # must be postgres now
    REDIS_URL: str = _get_env("REDIS_URL", "")
    JWT_SECRET: str = _get_env("JWT_SECRET", "dev_secret_change_me")
    JWT_ALG: str = _get_env("JWT_ALG", "HS256")
    JWT_EXPIRE_MINUTES: int = int(_get_env("JWT_EXPIRE_MINUTES", "4320"))

    REDIS_URL = os.getenv("REDIS_URL", "")
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", os.getenv("REDIS_URL", ""))
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", os.getenv("REDIS_URL", ""))
    DATA_ROOT = os.getenv("DATA_ROOT", "./data")
    SCHEMA_FILE = os.getenv("SCHEMA_FILE", os.path.join(os.path.dirname(__file__), "..", "packet_schema.json"))
    # Data dir relative to backend/
    DATA_DIR: str = str(BACKEND_DIR / "data_store")

    # DB pool tuning
    DB_POOL_SIZE: int = int(_get_env("DB_POOL_SIZE", "10"))
    DB_MAX_OVERFLOW: int = int(_get_env("DB_MAX_OVERFLOW", "20"))
    DB_POOL_RECYCLE: int = int(_get_env("DB_POOL_RECYCLE", "1800"))

settings = Settings()

# Safety: enforce postgres only
if not settings.DATABASE_URL or not settings.DATABASE_URL.startswith("postgresql"):
    raise RuntimeError(
        "PostgreSQL is required. Set DATABASE_URL=postgresql+psycopg2://user:pass@host:port/db"
    )

# Ensure data dir exists
os.makedirs(settings.DATA_DIR, exist_ok=True)
