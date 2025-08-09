import json, asyncio
from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session
from db.session import SessionLocal
from repositories.job_repo import get_job
from routers.auth import require_user
from models import Job

router = APIRouter(prefix="/jobs", tags=["jobs"])

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@router.get("/{job_id}")
def job_status(job_id: str, user = Depends(require_user), db: Session = Depends(get_db)):
    j = get_job(db, job_id)
    if not j: raise HTTPException(status_code=404, detail="not found")
    return {"id": j.id, "dataset_id": j.dataset_id, "status": j.status.value, "progress": j.progress or 0.0, "message": j.message or "", "logs": j.logs or ""}

@router.websocket("/ws/{job_id}")
async def ws_job(websocket: WebSocket, job_id: str):
    await websocket.accept()
    prev = None
    try:
        while True:
            db = SessionLocal()
            j = db.query(Job).filter(Job.id==job_id).first()
            db.expunge_all(); db.close()
            payload = {"id": job_id, "status": "unknown", "progress": 0.0, "message": "job not found", "logs": ""}
            if j:
                payload = {"id": j.id, "dataset_id": j.dataset_id, "status": j.status.value, "progress": j.progress or 0.0, "message": j.message or "", "logs": j.logs or ""}
            if json.dumps(payload) != json.dumps(prev):
                await websocket.send_json(payload); prev = payload
            if payload.get("status") in ("success","failed"):
                await asyncio.sleep(0.4); break
            await asyncio.sleep(0.8)
    except WebSocketDisconnect:
        pass
    finally:
        try: await websocket.close()
        except: pass
