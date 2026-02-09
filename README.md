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

## 주차관리 독립 서비스
- 소스 경로: `services/parking`
- 독립 포트: `127.0.0.1:8011`
- 리버스프록시 경로: `/parking/*`
- 진입 URL(프록시): `http://127.0.0.1:8080/parking/login`

전체 스택 실행(메인 + 주차 + Caddy):
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
