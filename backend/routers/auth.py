from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from backend.db.session import SessionLocal
from backend.models import User
from backend.schemas.auth import RegisterReq, LoginReq
from backend.security import hash_password, verify_password, create_token, decode_token

router = APIRouter(prefix="/auth", tags=["auth"])

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@router.post("/register")
def register(req: RegisterReq, db: Session = Depends(get_db)):
    if db.query(User).filter(User.username==req.username).first():
        raise HTTPException(status_code=400, detail="username exists")
    u = User(username=req.username, password_hash=hash_password(req.password), role=req.role, active=True)
    db.add(u); db.commit()
    return {"ok": True}

@router.post("/login")
def login(req: LoginReq, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.username==req.username, User.active==True).first()
    if not u or not verify_password(req.password, u.password_hash):
        raise HTTPException(status_code=401, detail="invalid credentials")
    token = create_token(str(u.id), u.role)
    return {"access_token": token, "token_type": "bearer", "user": {"id": u.id, "username": u.username, "role": u.role}}

def require_user(authorization: str = Header(None), db: Session = Depends(get_db)) -> User:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ",1)[1].strip()
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="invalid token")
    user_id = int(payload.get("sub"))
    u = db.query(User).get(user_id)
    if not u or not u.active:
        raise HTTPException(status_code=401, detail="inactive user")
    return u
