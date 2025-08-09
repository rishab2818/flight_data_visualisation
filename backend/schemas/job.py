from pydantic import BaseModel

class JobOut(BaseModel):
    id: str
    dataset_id: str | None
    status: str
    progress: float | None
    message: str | None
    logs: str | None
