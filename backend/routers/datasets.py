import os, uuid, json, shutil
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from core.config import settings
from db.session import SessionLocal
from models import Dataset, Job, JobStatus, User
from routers.auth import require_user
from repositories.dataset_repo import list_datasets, get_dataset
import tasks as taskmod

router = APIRouter(prefix="/datasets", tags=["datasets"])

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

SCHEMA_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "packet_schema.json"))

@router.get("")
def api_list(user: User = Depends(require_user), db: Session = Depends(get_db)):
    return list_datasets(db)

@router.get("/{dataset_id}/columns")
def columns(dataset_id: str, user: User = Depends(require_user), db: Session = Depends(get_db)):
    ds = get_dataset(db, dataset_id)
    if not ds: raise HTTPException(status_code=404, detail="not found")
    return {"columns": json.loads(ds.columns_json) if ds.columns_json else []}

@router.get("/{dataset_id}/download_proxy")
def download_proxy(dataset_id: str, file_type: str = Query("raw", enum=["raw","parquet"]), user: User = Depends(require_user), db: Session = Depends(get_db)):
    ds = get_dataset(db, dataset_id)
    if not ds: raise HTTPException(status_code=404, detail="not found")
    if file_type=="raw":
        if not ds.raw_path or not os.path.exists(ds.raw_path): raise HTTPException(status_code=400, detail="raw missing")
        return FileResponse(ds.raw_path, filename=os.path.basename(ds.raw_path))
    else:
        if not ds.parquet_path or not os.path.exists(ds.parquet_path): raise HTTPException(status_code=400, detail="parquet missing")
        return FileResponse(ds.parquet_path, filename=os.path.basename(ds.parquet_path))

@router.post("/upload")
async def upload(file: UploadFile = File(...), name: str = Form(None), user: User = Depends(require_user), db: Session = Depends(get_db)):
    dataset_id = str(uuid.uuid4())
    dataset_dir = os.path.join(settings.DATA_DIR, dataset_id); os.makedirs(dataset_dir, exist_ok=True)
    raw_filename = file.filename or "serial_data.txt"; raw_path = os.path.join(dataset_dir, raw_filename)
    content = await file.read(); open(raw_path, "wb").write(content)
    ds = Dataset(id=dataset_id, owner_id=user.id, name=name or os.path.splitext(raw_filename)[0], original_filename=raw_filename, raw_path=raw_path)
    db.add(ds); db.commit()
    job_id = str(uuid.uuid4())
    job = Job(id=job_id, user_id=user.id, dataset_id=dataset_id, status=JobStatus.pending, progress=0.0, message="queued", logs="")
    db.add(job); db.commit()
    if settings.REDIS_URL and hasattr(taskmod, "celery_app"):
        taskmod.parse_task.delay(job_id, dataset_id, raw_path, SCHEMA_FILE)
    else:
        import threading, importlib
        t = threading.Thread(
        target=parse_module.stream_to_parquet,
        args=(job_id, dataset_id, raw_path, SCHEMA_FILE),
        daemon=True
        )
        t.start()
    return {"job_id": job_id, "dataset_id": dataset_id}

@router.post("/demo_upload")
async def demo_upload(name: str = Form(None), user: User = Depends(require_user), db: Session = Depends(get_db)):
    sample = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "serial_data.txt"))
    if not os.path.exists(sample): raise HTTPException(status_code=404, detail="sample missing")
    dataset_id = str(uuid.uuid4())
    dataset_dir = os.path.join(settings.DATA_DIR, dataset_id); os.makedirs(dataset_dir, exist_ok=True)
    raw_filename = os.path.basename(sample); raw_path = os.path.join(dataset_dir, raw_filename)
    open(raw_path, "wb").write(open(sample, "rb").read())
    ds = Dataset(id=dataset_id, owner_id=user.id, name=name or os.path.splitext(raw_filename)[0], original_filename=raw_filename, raw_path=raw_path)
    db.add(ds); db.commit()
    job_id = str(uuid.uuid4())
    job = Job(id=job_id, user_id=user.id, dataset_id=dataset_id, status=JobStatus.pending, progress=0.0, message="queued", logs="")
    db.add(job); db.commit()
    if settings.REDIS_URL and hasattr(taskmod, "celery_app"):
        taskmod.parse_task.delay(job_id, dataset_id, raw_path, SCHEMA_FILE)
    else:
        import threading
        threading.Thread(target=__import__("services.parse_service", fromlist=[""]).stream_to_parquet, args=(job_id, dataset_id, raw_path, SCHEMA_FILE), daemon=True).start()
    return {"job_id": job_id, "dataset_id": dataset_id}

@router.delete("/{dataset_id}")
def delete(dataset_id: str, user: User = Depends(require_user), db: Session = Depends(get_db)):
    ds = get_dataset(db, dataset_id)
    if not ds: raise HTTPException(status_code=404, detail="not found")
    if str(user.role).lower() != "admin" and user.id != ds.owner_id:
        raise HTTPException(status_code=403, detail="forbidden")
    root = os.path.dirname(ds.raw_path) if ds.raw_path else None
    db.delete(ds); db.commit()
    if root and os.path.exists(root): shutil.rmtree(root, ignore_errors=True)
    return {"ok": True}
