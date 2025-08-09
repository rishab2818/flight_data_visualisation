from sqlalchemy import Column, String, Integer, DateTime, Text
from sqlalchemy.sql import func
from db.session import Base

class Dataset(Base):
    __tablename__ = "datasets"
    project_id = Column(Integer, nullable=True)
    id = Column(String, primary_key=True, index=True)
    owner_id = Column(Integer, nullable=True)
    name = Column(String, nullable=True)
    original_filename = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    raw_path = Column(String, nullable=False)
    parquet_path = Column(String, nullable=True)
    columns_json = Column(Text, nullable=True)
    packet_count = Column(Integer, nullable=True)
    plots_json = Column(Text, nullable=True)
    tags_json = Column(Text, nullable=True)
