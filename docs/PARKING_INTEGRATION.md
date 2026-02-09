# Parking Integration

`parking_management` is integrated as an independent service in this repository:

- Main app: `app.main:app` on `127.0.0.1:8000`
- Parking app: `services/parking/app.main:app` on `127.0.0.1:8011`
- Reverse proxy:
  - Production: `ops/Caddyfile`
  - Local: `ops/Caddyfile.local`

## ka-part.com 운영 모드

`ka-part.com`이 단일 FastAPI 프로세스로 운영되는 환경에서는
메인 앱(`app.main`)이 주차 앱을 `/parking` 경로로 자동 마운트합니다.

- URL: `https://ka-part.com/parking/login`
- 환경변수:
  - `ENABLE_PARKING_EMBED=1` (기본값)
  - `PARKING_API_KEY` (필수 변경)
  - `PARKING_SECRET_KEY` (필수 변경)
  - `PARKING_ROOT_PATH=/parking` (기본 자동 설정)

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

Note:
- Caddy production mode requires `ka-part.com` DNS to point to this host.
- If DNS points to another platform (e.g., Render), use the embedded mode above.

## Backup targets

- Main DB: `data/ka.db`
- Parking DB: `services/parking/app/data/parking.db`
- Parking uploads: `services/parking/app/uploads/`
