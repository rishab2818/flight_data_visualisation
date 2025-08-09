# Flight Data Platform — Product Phase 1 (Polished, Offline)

## Backend
```
cd backend
python -m pip install -r requirements.txt
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000
```
- Login: admin / admin
- Optional: set `DATABASE_URL` (Postgres) and `REDIS_URL` (Celery).

## Frontend
```
cd frontend
npm install
npm run dev
# http://localhost:5173
```


