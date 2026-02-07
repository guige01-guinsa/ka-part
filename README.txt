# ka-part (Relational v2)

## Run (Android Pydroid/Termux/Windows/macOS)
```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open:
- PWA: http://127.0.0.1:8000/pwa/?v=20260205
- API routes list: http://127.0.0.1:8000/api/routes
- Health: http://127.0.0.1:8000/api/health

## Notes
- Database file: `data/ka.db`
- Schema: `sql/schema.sql` (auto-applied on startup)
