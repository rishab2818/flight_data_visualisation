import enum
from sqlalchemy import Column, String, Integer, DateTime, Text, Float, Enum
from sqlalchemy.sql import func
from db.session import Base

class JobStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    success = "success"
    failed = "failed"

class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True, index=True)
    user_id = Column(Integer, nullable=True)
    dataset_id = Column(String, nullable=True)
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.pending)
    progress = Column(Float, nullable=True)
    message = Column(Text, nullable=True)
    logs = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
