$env:REDIS_URL="redis://127.0.0.1:6379/0"
$env:CELERY_BROKER_URL=$env:REDIS_URL
$env:CELERY_RESULT_BACKEND=$env:REDIS_URL
python -m celery -A backend.tasks.celery_app worker -l info -Q default -P threads -c 4
