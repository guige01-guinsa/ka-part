# ka-part

아파트 수변전실 일지(PWA) + FastAPI + SQLite 프로젝트입니다.

## 실행
```bash
cd ka-part
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

- 메인: `http://localhost:8000/pwa/`
- 로그인: `http://localhost:8000/pwa/login.html`

## 주차관리 통합 실행
- 소스 경로: `services/parking`
- 실행 방식: 메인 앱(`app.main`)이 주차 앱을 `/parking`으로 자동 마운트
- 진입 URL: `http://127.0.0.1:8080/parking/admin2`
- 별도 주차 로그인 없음(메인 시스템 인증 토큰으로 자동 진입)

`ka-part.com` 운영 환경도 동일하게 단일 프로세스 모드입니다.
- URL: `https://ka-part.com/parking/admin2`
- 제어 변수: `ENABLE_PARKING_EMBED=1` (기본값)
- 권장 보안 변수: `PARKING_API_KEY`, `PARKING_SECRET_KEY`

외부 주차 서버(`parking_man`) 연동 모드:
- `ENABLE_PARKING_EMBED=0`
- `PARKING_BASE_URL=https://<parking-man-domain>`
- `PARKING_SSO_PATH=/parking/sso` (환경에 따라 `/sso`)
- `PARKING_CONTEXT_SECRET`는 parking_man의 `PARKING_CONTEXT_SECRET`와 동일값 사용

공유 경계:
- 메인 시스템과 주차 시스템은 `site_code`와 `permission_level`만 공유
- 주차 진입은 `/api/parking/context` 서명 토큰을 통해 수행
- 주차 데이터(차량/위반)는 주차 DB 내부에서 `site_code` 기준으로만 조회/저장

전체 스택 실행(메인 + Caddy):
```powershell
pwsh -File ops\start_stack.ps1
```

전체 스택 중지:
```powershell
pwsh -File ops\stop_stack.ps1
```

## 배포(Render)
- Build: `pip install -r requirements.txt`
- Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## 인증/권한
- 세션 토큰(Bearer) 인증 사용
- 일반 사용자: 일지 조회/저장
- 관리자: 사용자 등록/수정/삭제 가능
- 사용자 관리 화면: `/pwa/users.html` (관리자 전용)

## 주요 API
- 인증
  - `GET /api/auth/bootstrap_status`
  - `POST /api/auth/bootstrap`
  - `POST /api/auth/login`
  - `POST /api/auth/logout`
  - `GET /api/auth/me`
  - `POST /api/auth/change_password`
  - `POST /api/auth/signup/request_phone_verification`
  - `POST /api/auth/signup/verify_phone_and_issue_id`
- 사용자
  - `GET /api/users`
  - `POST /api/users`
  - `PATCH /api/users/{user_id}`
  - `DELETE /api/users/{user_id}`
- 일지
  - `POST /api/save`
  - `GET /api/load`
  - `DELETE /api/delete`
  - `GET /api/list_range`
  - `GET /api/export`
  - `GET /api/pdf`
- 단지 제원(환경변수)
  - `GET /api/site_env_template` (관리자)
  - `GET /api/site_env_templates` (관리자)
  - `GET /api/base_schema` (관리자)
  - `GET /api/site_env?site_name=...` (관리자)
  - `PUT /api/site_env` (관리자, `{site_name, config}`)
  - `DELETE /api/site_env?site_name=...` (관리자)
  - `GET /api/site_env_list` (관리자)

## 참고
- 운영 DB: `data/ka.db`
- 스키마 확장과 자동 보정은 `app/db.py`의 `init_db()`/`ensure_domain_tables()`에서 처리합니다.
- 관리자 제원 설정 화면: `/pwa/spec_env.html`

## 문서
- 사용자/인증 운영: `docs/USERS.md`
- 단지 제원 설정: `docs/SITE_ENV.md`
- 시설관리 사용자 매뉴얼: `docs/USER_MANUAL_FACILITY_MANAGER_KO.md`
- 주차 통합 운영: `docs/PARKING_INTEGRATION.md`
