# Parking Service (Independent)

This service runs independently from the main `ka-part` app.

## Local run

```powershell
cd services\parking
pwsh -File run.ps1 -ListenHost 127.0.0.1 -Port 8011
```

Default endpoints:
- Health: `http://127.0.0.1:8011/health`
- Login: `http://127.0.0.1:8011/login`

When reverse-proxied under `/parking`, set:
- `PARKING_ROOT_PATH=/parking`

Example env file:
- `.env.production` (copy from `.env.production.example`)
