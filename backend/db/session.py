from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session
from core.config import settings
import os

def _ensure_sqlite_path(url: str) -> str:
    assert url.startswith("sqlite"), "Only for sqlite URLs"
    raw = url.replace("sqlite:///", "", 1)
    if raw.startswith("./") or raw.startswith(".\\"):
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        raw = os.path.abspath(os.path.join(base, raw[2:]))
    else:
        raw = os.path.abspath(raw)
    os.makedirs(os.path.dirname(raw), exist_ok=True)
    return f"sqlite:///{raw}"

if settings.DATABASE_URL.startswith("sqlite"):
    sqlite_url = _ensure_sqlite_path(settings.DATABASE_URL)
    engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})
else:
    engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)

SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
Base = declarative_base()

def init_db():
    import models
    Base.metadata.create_all(bind=engine)
