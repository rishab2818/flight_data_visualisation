# backend/tasks.py
import os
from celery import Celery

# Read connection from env (core/config is fine too)
REDIS_URL = os.getenv("CELERY_BROKER_URL", os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))

celery_app = Celery(
    "flight",
    broker=REDIS_URL,
    backend=os.getenv("CELERY_RESULT_BACKEND", REDIS_URL),
    include=[],
)

# Windows-friendly baseline config
celery_app.conf.update(
    broker_connection_retry_on_startup=True,
    task_acks_late=True,
    worker_max_tasks_per_child=50,   # avoid memory creep
    worker_prefetch_multiplier=1,    # fair dispatch
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",
    enable_utc=True,
)

@celery_app.task(name="tasks.parse_task", bind=True, acks_late=True)
def parse_task(self, job_id: str, dataset_id: str, raw_path: str, schema_file: str):
    from backend.services.parse_service import stream_to_parquet
    stream_to_parquet(job_id, dataset_id, raw_path, schema_file)

