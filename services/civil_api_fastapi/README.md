# civil_api_fastapi

FastAPI scaffold for apartment complaint handling (`/api/v1`).

## Run

```bash
cd services/civil_api_fastapi
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8100
```

## Auth model (scaffold)

- Resident calls: set `X-User-Id` header.
- Admin calls: set both `X-User-Id` and `X-Role: admin` (or `staff`).

This is a development scaffold backed by in-memory storage. Replace `app/repository.py`
with PostgreSQL persistence using the DDL in `sql/postgres/20260212_complaints_v1.sql`.
