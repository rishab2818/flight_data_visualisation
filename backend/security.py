from passlib.hash import bcrypt
from jose import jwt, JWTError
from datetime import datetime, timedelta
from typing import Optional
from core.config import settings

def hash_password(pw: str) -> str: return bcrypt.hash(pw)
def verify_password(pw: str, pw_hash: str) -> bool: return bcrypt.verify(pw, pw_hash)
def create_token(sub: str, role: str) -> str:
    exp = datetime.utcnow() + timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    payload = {"sub": sub, "role": role, "exp": exp}
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALG)
def decode_token(token: str) -> Optional[dict]:
    try: return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALG])
    except JWTError: return None
