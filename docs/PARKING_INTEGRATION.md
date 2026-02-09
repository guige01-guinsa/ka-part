# Parking Integration

`parking_management` is integrated in-process in this repository:

- Main app: `app.main:app` on `127.0.0.1:8000`
- Parking app: mounted under `/parking` in the same process
- Reverse proxy:
  - Production: `ops/Caddyfile`
  - Local: `ops/Caddyfile.local`

## ka-part.com 운영 모드

`ka-part.com` 운영은 단일 FastAPI 프로세스 모드입니다.
메인 앱(`app.main`)이 주차 앱을 `/parking` 경로로 자동 마운트합니다.

- URL: `https://ka-part.com/parking/admin2`
- 환경변수:
  - `ENABLE_PARKING_EMBED=1` (기본값)
  - `PARKING_API_KEY` (필수 변경)
  - `PARKING_SECRET_KEY` (필수 변경)
  - `PARKING_LOCAL_LOGIN_ENABLED=0` (별도 주차 로그인 비활성, 기본 권장)
  - `PARKING_ROOT_PATH=/parking` (기본 자동 설정)

### 공유 데이터 경계(강제)

두 시스템 간 공유는 아래 두 항목으로 제한됩니다.

- `site_code` (아파트 단지코드)
- `permission_level` (`admin` / `site_admin` / `user`)

구현 방식:
- `GET /api/parking/context`가 인증 사용자 기준으로 위 2개 값만 포함된 서명 토큰 발급
- PWA의 `주차관리` 버튼은 해당 토큰으로 `/parking/sso` 접속(주차 별도 로그인 없음)
- 주차 서비스는 세션에 `site_code`, 권한 매핑(`admin`/`guard`/`viewer`)만 저장
- 차량/위반 조회·저장은 `site_code` 스코프 내에서만 처리

## Start full stack (local)

```powershell
pwsh -File ops\start_stack.ps1
```

Access URLs:
- Main PWA: `http://127.0.0.1:8080/pwa/`
- Parking entry: `http://127.0.0.1:8080/parking/admin2`

## Stop full stack

```powershell
pwsh -File ops\stop_stack.ps1
```

## Windows Service deployment (production)

See `ops/deploy.ps1`.

Service topology:
- `ka-part-api` -> port 8000
- `ka-part-web` (Caddy) -> public domain reverse proxy

Note:
- Caddy production mode requires `ka-part.com` DNS to point to this host.
- If DNS points to another platform (e.g., Render), use the embedded mode above.

## Backup targets

- Main DB: `data/ka.db`
- Parking DB: `services/parking/app/data/parking.db`
- Parking uploads: `services/parking/app/uploads/`
