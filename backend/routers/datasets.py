# backend/routers/datasets.py
import os, uuid, json, shutil
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from backend.core.config import settings
from backend.db.session import SessionLocal
from backend.models import Dataset, Job, JobStatus, User
from backend.routers.auth import require_user
from backend.services.parse_service import stream_to_parquet
import backend.tasks as taskmod

router = APIRouter(prefix="/datasets", tags=["datasets"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

DATA_ROOT = getattr(settings, "DATA_ROOT", "./data")
SCHEMA_FILE = getattr(settings, "SCHEMA_FILE", os.path.join(os.path.dirname(__file__), "..", "packet_schema.json"))
SCHEMA_FILE = os.path.abspath(SCHEMA_FILE)

@router.get("")
def list_datasets(user: User = Depends(require_user), db: Session = Depends(get_db)):
    rows = db.query(Dataset).all()
    return [{
        "id": r.id,
        "name": r.name,
        "original_filename": r.original_filename,
        "packet_count": r.packet_count,
        "parquet_path": r.parquet_path
    } for r in rows]

@router.get("/{dataset_id}/columns")
def columns(dataset_id: str, user: User = Depends(require_user), db: Session = Depends(get_db)):
    ds = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="object not found")
    return {"columns": json.loads(ds.columns_json) if ds.columns_json else []}

# Your frontend calls /download_proxy — keep it
@router.get("/{dataset_id}/download_proxy")
def download_proxy(dataset_id: str, file_type: str = Query("parquet"), user: User = Depends(require_user), db: Session = Depends(get_db)):
    ds = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="object not found")
    if file_type == "raw":
        if not ds.raw_path or not os.path.exists(ds.raw_path):
            raise HTTPException(status_code=400, detail="raw missing")
        return FileResponse(ds.raw_path, filename=os.path.basename(ds.raw_path))
    else:
        if not ds.parquet_path or not os.path.exists(ds.parquet_path):
            raise HTTPException(status_code=400, detail="parquet missing")
        return FileResponse(ds.parquet_path, filename=os.path.basename(ds.parquet_path))

# (Optional) Also support /download for future-proofing
@router.get("/{dataset_id}/download")
def download(dataset_id: str, file_type: str = Query("parquet"), user: User = Depends(require_user), db: Session = Depends(get_db)):
    return download_proxy(dataset_id, file_type, user, db)

@router.post("/upload")
def upload_dataset(
    file: UploadFile = File(...),
    name: str = Form(None),
    user: User = Depends(require_user),
    db: Session = Depends(get_db),
):
    dataset_id = str(uuid.uuid4())
    root = os.path.join(DATA_ROOT, dataset_id)
    os.makedirs(root, exist_ok=True)
    raw_path = os.path.join(root, file.filename)

    # stream to disk
    with open(raw_path, "wb") as out:
        shutil.copyfileobj(file.file, out, length=1024 * 1024)

    ds = Dataset(
        id=dataset_id,
        owner_id=getattr(user, "id", None),
        name=name or file.filename,
        original_filename=file.filename,
        raw_path=raw_path,
    )
    db.add(ds); db.commit()

    job_id = str(uuid.uuid4())
    job = Job(
        id=job_id,
        user_id=getattr(user, "id", None),
        dataset_id=dataset_id,
        status=JobStatus.pending,
        progress=0.0,
        message="queued",
        logs=None,  # not used anymore
    )
    db.add(job); db.commit()

    _start_parse_async(job_id, dataset_id, raw_path, SCHEMA_FILE)
    return {"job_id": job_id, "dataset_id": dataset_id}

@router.post("/{dataset_id}/parse")
def parse_dataset(dataset_id: str, user: User = Depends(require_user), db: Session = Depends(get_db)):
    ds = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="object not found")

    job_id = str(uuid.uuid4())
    job = Job(
        id=job_id,
        user_id=getattr(user, "id", None),
        dataset_id=dataset_id,
        status=JobStatus.pending,
        progress=0.0,
        message="queued",
        logs=None,
    )
    db.add(job); db.commit()

    _start_parse_async(job_id, dataset_id, ds.raw_path, SCHEMA_FILE)
    return {"job_id": job_id, "dataset_id": dataset_id}

@router.post("/demo_upload")
def demo_upload(name: str = Form(None), user: User = Depends(require_user), db: Session = Depends(get_db)):
    sample = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "serial_data.txt"))
    if not os.path.exists(sample):
        raise HTTPException(status_code=404, detail="sample missing")

    dataset_id = str(uuid.uuid4())
    root = os.path.join(DATA_ROOT, dataset_id)
    os.makedirs(root, exist_ok=True)
    raw_path = os.path.join(root, "serial_data.txt")
    shutil.copyfile(sample, raw_path)

    ds = Dataset(
        id=dataset_id,
        owner_id=getattr(user, "id", None),
        name=name or "demo_serial_data",
        original_filename="serial_data.txt",
        raw_path=raw_path,
    )
    db.add(ds); db.commit()

    job_id = str(uuid.uuid4())
    job = Job(
        id=job_id,
        user_id=getattr(user, "id", None),
        dataset_id=dataset_id,
        status=JobStatus.pending,
        progress=0.0,
        message="queued",
        logs=None,
    )
    db.add(job); db.commit()

    _start_parse_async(job_id, dataset_id, raw_path, SCHEMA_FILE)
    return {"job_id": job_id, "dataset_id": dataset_id}

@router.delete("/{dataset_id}")
@router.delete("/{dataset_id}")
def delete_dataset(dataset_id: str, user: User = Depends(require_user), db: Session = Depends(get_db)):
    ds = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="object not found")

    # block if a job is running/pending for this dataset
    running = db.query(Job).filter(
        Job.dataset_id == dataset_id,
        Job.status.in_([JobStatus.pending, JobStatus.running])
    ).first()
    if running:
        raise HTTPException(status_code=400, detail="job running; stop it before delete")

    root = os.path.dirname(ds.raw_path) if ds.raw_path else None
    db.delete(ds)
    db.commit()

    if root and os.path.exists(root):
        shutil.rmtree(root, ignore_errors=True)

    return {"ok": True}

    ds = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="object not found")

    root = os.path.dirname(ds.raw_path) if ds.raw_path else None
    db.delete(ds); db.commit()

    if root and os.path.exists(root):
        shutil.rmtree(root, ignore_errors=True)
    return {"ok": True}

def _start_parse_async(job_id: str, dataset_id: str, raw_path: str, schema_file: str) -> None:
    redis_url = getattr(settings, "REDIS_URL", None)
    if redis_url and getattr(taskmod, "celery_app", None):
        try:
            taskmod.celery_app.send_task(
                "tasks.parse_task",
                args=[job_id, dataset_id, raw_path, schema_file],
                queue="default",
            )
            return
        except Exception:
            pass
    import threading
    threading.Thread(target=stream_to_parquet, args=(job_id, dataset_id, raw_path, schema_file), daemon=True).start()
