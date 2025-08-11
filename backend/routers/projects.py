from fastapi import APIRouter, Depends, HTTPException, Body
from sqlalchemy.orm import Session
from backend.db.session import SessionLocal
from backend.models import Project, ProjectMember, Role, User
from backend.routers.auth import require_user

router = APIRouter(prefix="/projects", tags=["projects"])

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@router.post("")
def create_project(payload: dict = Body(...), user: User = Depends(require_user), db: Session = Depends(get_db)):
    name = payload.get("name")
    if not name: raise HTTPException(status_code=400, detail="name required")
    if db.query(Project).filter(Project.name==name).first():
        raise HTTPException(status_code=400, detail="project exists")
    pr = Project(name=name); db.add(pr); db.commit(); db.refresh(pr)
    db.add(ProjectMember(project_id=pr.id, user_id=user.id, role=Role.admin)); db.commit()
    return {"id": pr.id, "name": pr.name}

@router.get("")
def list_projects(user: User = Depends(require_user), db: Session = Depends(get_db)):
    # list projects where user is member
    q = db.query(Project).join(ProjectMember, Project.id==ProjectMember.project_id).filter(ProjectMember.user_id==user.id).all()
    return [{"id": p.id, "name": p.name} for p in q]
