from pydantic import BaseModel

class DatasetOut(BaseModel):
    id: str
    name: str | None
    original_filename: str | None
    created_at: str | None
    packet_count: int | None
    columns: list
    plots: list
