import asyncio
from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from sqlalchemy.orm import Session

from backend.db.session import SessionLocal
from backend.repositories.job_repo import get_job
from backend.routers.auth import require_user
from backend.models import Job, JobStatus
from backend.events import events

router = APIRouter(prefix="/jobs", tags=["jobs"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/{job_id}")
def get_job_status(job_id: str, user = Depends(require_user), db: Session = Depends(get_db)):
    j = get_job(db, job_id)
    if not j:
        raise HTTPException(status_code=404, detail="not found")
    return {
        "id": j.id,
        "status": str(j.status),
        "progress": float(j.progress or 0.0),
        "message": j.message or "",
        # logs are not persisted by design
        "logs": "",
    }

# (Optional) legacy endpoint: keep but return empty since logs aren't stored
@router.get("/{job_id}/logdump")
def job_logdump(job_id: str, user = Depends(require_user), db: Session = Depends(get_db)):
    return {"job_id": job_id, "events": []}

async def _ws_loop(websocket: WebSocket, job_id: str):
    await websocket.accept()

    # send initial snapshot from DB
    db = SessionLocal()
    try:
        j = db.query(Job).filter(Job.id == job_id).first()
        if not j:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        await websocket.send_json({
            "job_id": job_id,
            "type": "snapshot",
            "status": str(j.status),
            "progress": float(j.progress or 0.0),
            "message": j.message or "",
        })
    finally:
        db.close()

    # live stream via Redis pub/sub (falls back to in-process if Redis is absent)
    try:
        async for payload in events.stream(job_id):
            await websocket.send_json(payload)
    except WebSocketDisconnect:
        return
    except Exception:
        # best-effort; just drop the socket on unexpected errors
        return

# compat: /jobs/{job_id}/ws
@router.websocket("/{job_id}/ws")
async def ws_job_old(websocket: WebSocket, job_id: str):
    await _ws_loop(websocket, job_id)

# current: /jobs/ws/{job_id}
@router.websocket("/ws/{job_id}")
async def ws_job(websocket: WebSocket, job_id: str):
    await _ws_loop(websocket, job_id)
