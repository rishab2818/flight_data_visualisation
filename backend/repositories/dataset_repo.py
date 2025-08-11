import json
from sqlalchemy.orm import Session
from backend.models import Dataset

def list_datasets(db: Session):
    rows = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
    out = []
    for r in rows:
        out.append({
            "id": r.id, "name": r.name, "original_filename": r.original_filename,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "packet_count": r.packet_count or 0,
            "columns": json.loads(r.columns_json) if r.columns_json else [],
            "plots": json.loads(r.plots_json) if r.plots_json else []
        })
    return out

def get_dataset(db: Session, dataset_id: str):
    return db.query(Dataset).filter(Dataset.id==dataset_id).first()
