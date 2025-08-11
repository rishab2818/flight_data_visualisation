Set-ExecutionPolicy -Scope Process Bypass
$env:REDIS_URL="redis://127.0.0.1:6379/0"
$env:CELERY_BROKER_URL=$env:REDIS_URL
$env:CELERY_RESULT_BACKEND=$env:REDIS_URL
python -m uvicorn backend.app:app --reload --host 0.0.0.0 --port 8000
