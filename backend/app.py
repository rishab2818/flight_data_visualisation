from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio

# ✅ package-qualified imports
from backend.db.session import init_db, SessionLocal
from backend.models import User, Job, JobStatus
from backend.security import hash_password
from backend.routers import auth, datasets, jobs, plots, projects
from backend.events import events

app = FastAPI(title="Flight Data Platform — Phase 1 (Product, Polished)")

# CORS (keep or tighten as you like)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def on_startup() -> None:
    # init DB & bind events to this loop so background threads can push WS messages
    init_db()
    try:
        events.bind_loop(asyncio.get_running_loop())
    except Exception:
        pass

    db = SessionLocal()
    try:
        # seed default admin if DB is empty
        if db.query(User).count() == 0:
            db.add(User(username="admin", password_hash=hash_password("admin"),
                        role="admin", active=True))
            db.commit()

        # mark any interrupted (running) jobs as failed after restart
        stuck = db.query(Job).filter(Job.status == JobStatus.running).all()
        for j in stuck:
            j.status = JobStatus.failed
            j.message = "server restarted during processing"
            db.add(j)
        db.commit()
    finally:
        db.close()

# Routers
app.include_router(auth.router)
app.include_router(datasets.router)
app.include_router(jobs.router)
app.include_router(plots.router)
app.include_router(projects.router)
