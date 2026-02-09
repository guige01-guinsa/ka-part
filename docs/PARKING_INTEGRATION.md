# Parking Integration

`parking_management` is integrated as an independent service in this repository:

- Main app: `app.main:app` on `127.0.0.1:8000`
- Parking app: `services/parking/app.main:app` on `127.0.0.1:8011`
- Reverse proxy:
  - Production: `ops/Caddyfile`
  - Local: `ops/Caddyfile.local`

## Start full stack (local)

```powershell
pwsh -File ops\start_stack.ps1
```

Access URLs:
- Main PWA: `http://127.0.0.1:8080/pwa/`
- Parking login: `http://127.0.0.1:8080/parking/login`

## Stop full stack

```powershell
pwsh -File ops\stop_stack.ps1
```

## Windows Service deployment (production)

See `ops/deploy.ps1`.

Service topology:
- `ka-part-api` -> port 8000
- `ka-part-parking` -> port 8011
- `ka-part-web` (Caddy) -> public domain reverse proxy

## Backup targets

- Main DB: `data/ka.db`
- Parking DB: `services/parking/app/data/parking.db`
- Parking uploads: `services/parking/app/uploads/`
