from core.config import settings
from services.parse_service import stream_to_parquet
if settings.REDIS_URL:
    from celery import Celery
    celery_app = Celery("tasks", broker=settings.REDIS_URL, backend=settings.REDIS_URL)
    @celery_app.task
    def parse_task(job_id: str, dataset_id: str, raw_path: str, schema_file: str):
        stream_to_parquet(job_id, dataset_id, raw_path, schema_file)
