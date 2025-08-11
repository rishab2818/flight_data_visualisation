from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db.session import init_db, SessionLocal
from models import User,Job, JobStatus
from security import hash_password
from routers import auth, datasets, jobs, plots, projects
from db.session import SessionLocal
app = FastAPI(title="Flight Data Platform — Phase 1 (Product, Polished)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

@app.on_event("startup")
def on_startup():
    init_db()
    db = SessionLocal()
    try:
        if db.query(User).count() == 0:
            db.add(User(username="admin", password_hash=hash_password("admin"), role="admin", active=True))
            db.commit()
    finally:
        db.close()

def mark_stuck_jobs_failed():
    db = SessionLocal()
    try:
        stuck = db.query(Job).filter(Job.status == JobStatus.running).all()
        for j in stuck:
            j.status = JobStatus.failed
            j.message = "server restarted during processing"
            db.add(j)
        db.commit()
    finally:
        db.close()

app.include_router(auth.router)
app.include_router(datasets.router)
app.include_router(jobs.router)
app.include_router(plots.router)
app.include_router(projects.router)
