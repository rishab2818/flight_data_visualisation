import enum
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from db.session import Base

class Role(str, enum.Enum):
    admin = "admin"
    editor = "editor"
    viewer = "viewer"

class Project(Base):
    __tablename__ = "projects"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    members = relationship("ProjectMember", back_populates="project")

class ProjectMember(Base):
    __tablename__ = "project_members"
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    user_id = Column(Integer, nullable=False)
    role = Column(Enum(Role), default=Role.viewer, nullable=False)
    project = relationship("Project", back_populates="members")
    __table_args__ = (UniqueConstraint('project_id','user_id', name='uq_project_user'),)
