from sqlalchemy.orm import Session
from backend.models import Job

def get_job(db: Session, job_id: str):
    return db.query(Job).filter(Job.id==job_id).first()
