# ka-part complaint engine

기존 민원시스템을 제거하고 다시 만든 웹 MVP입니다. 핵심 목적은 관리사무소 직원이 전화, 카톡, 방문 민원을 빠르게 입력하고 AI 자동분류, 실시간 대시보드, 일일보고, 테넌트별 API 연동까지 처리하는 것입니다.

## Run

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

- 공개 안내: `http://localhost:8000/pwa/public.html`
- 로그인: `http://localhost:8000/pwa/login.html`
- 운영 포털: `http://localhost:8000/pwa/`

## Main API

- `GET /api/health`
- `POST /api/complaints`
- `GET /api/complaints`
- `GET /api/complaints/{id}`
- `PUT /api/complaints/{id}`
- `POST /api/complaints/{id}/attachments`
- `POST /api/ai/classify`
- `POST /api/ai/kakao_digest`
- `GET /api/dashboard/summary`
- `GET /api/report/daily`
- `GET /api/admin/tenants`
- `POST /api/admin/tenants`
- `POST /api/admin/tenants/{tenant_id}/rotate_key`

## Auth

- 웹 포털: 세션 쿠키 로그인
- 외부 시스템 연동: 테넌트별 API Key `Bearer sk-ka-...`

## Intake Features

- 민원인 연락처 입력
- 사진 첨부 최대 6장
- 첨부 전체선택, 선택삭제, 전체삭제
- Render 영속 스토리지용 `KA_STORAGE_ROOT` 지원

## Deployment Seed

배포 서버에서 초기 계정과 테넌트를 자동 생성하려면 아래 환경변수를 설정합니다.

- `KA_BOOTSTRAP_ADMIN_LOGIN`
- `KA_BOOTSTRAP_ADMIN_NAME`
- `KA_BOOTSTRAP_ADMIN_PASSWORD`
- `KA_BOOTSTRAP_TENANT_ID`
- `KA_BOOTSTRAP_TENANT_NAME`
- `KA_BOOTSTRAP_TENANT_SITE_CODE`
- `KA_BOOTSTRAP_TENANT_SITE_NAME`
- `KA_BOOTSTRAP_TENANT_API_KEY`
- `KA_BOOTSTRAP_MANAGER_LOGIN`
- `KA_BOOTSTRAP_MANAGER_NAME`
- `KA_BOOTSTRAP_MANAGER_PASSWORD`
- `KA_BOOTSTRAP_DESK_LOGIN`
- `KA_BOOTSTRAP_DESK_NAME`
- `KA_BOOTSTRAP_DESK_PASSWORD`
- `KA_STORAGE_ROOT`

앱 시작 시 값이 있으면 같은 ID 기준으로 재시드하며, 비밀번호와 API Key도 지정값으로 맞춰집니다.

## Test

```bash
pip install -r requirements-dev.txt
pytest -q tests/test_engine_routes.py
python -m compileall app
```
