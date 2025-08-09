from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from db.session import Base

class PlotPreset(Base):
    __tablename__ = "plot_presets"
    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    config_json = Column(Text, nullable=False)  # stores x_col, series, filters, computes, options
    created_at = Column(DateTime(timezone=True), server_default=func.now())
