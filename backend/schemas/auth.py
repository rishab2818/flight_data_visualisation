from pydantic import BaseModel

class RegisterReq(BaseModel):
    username: str
    password: str
    role: str = 'user'

class LoginReq(BaseModel):
    username: str
    password: str
