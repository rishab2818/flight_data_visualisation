import asyncio, json
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
    if not j: raise HTTPException(status_code=404, detail="not found")
    return {
        "id": j.id,
        "status": str(j.status),
        "progress": float(j.progress or 0.0),
        "message": j.message or "",
        # logs intentionally omitted from DB per requirements
        "logs": "",
    }

@router.get("/{job_id}/logdump")
def job_logdump(job_id: str, user = Depends(require_user), db: Session = Depends(get_db)):
    # returns recent in-memory events so legacy UIs can poll logs
    return {"job_id": job_id, "events": events.dump(job_id)}

async def _ws_loop(websocket: WebSocket, job_id: str):
    await websocket.accept()
    # initial snapshot
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

    # live stream
    q = await events.subscribe(job_id)
    try:
        while True:
            payload = await q.get()
            await websocket.send_json(payload)
            if payload.get("type") == "status" and str(payload.get("status")) in (
                "success", "failed", str(JobStatus.success), str(JobStatus.failed)
            ):
                await asyncio.sleep(0.3)
                break
    except WebSocketDisconnect:
        pass
    finally:
        await events.unsubscribe(job_id, q)
        try:
            await websocket.close()
        except Exception:
            pass

# support old pattern: /jobs/{job_id}/ws
@router.websocket("/{job_id}/ws")
async def ws_job_old(websocket: WebSocket, job_id: str):
    await _ws_loop(websocket, job_id)

# support your frontend pattern: /jobs/ws/{job_id}
@router.websocket("/ws/{job_id}")
async def ws_job(websocket: WebSocket, job_id: str):
    await _ws_loop(websocket, job_id)
