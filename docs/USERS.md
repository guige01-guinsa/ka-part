# 사용자/인증 운영 가이드

## 1) 로그인
- 로그인 화면: `/pwa/login.html`
- 인증 방식: `Bearer` 세션 토큰 (`Authorization: Bearer <token>`)
- 일반 사용자는 일지 화면(`/pwa/`) 사용 가능
- 관리자만 사용자관리 화면(`/pwa/users.html`) 접근 가능

## 2) 최초 관리자 생성 (Bootstrap)
- 활성 관리자 계정이 0명일 때만 동작
- 화면에서 초기 관리자 아이디/비밀번호를 입력해 1회 생성
- 생성 후 즉시 로그인 세션 발급

## 3) 사용자 관리
- API
  - `GET /api/users`
  - `POST /api/users`
  - `PATCH /api/users/{user_id}`
  - `DELETE /api/users/{user_id}`
- 신규 사용자 생성 시 비밀번호 필수(8자 이상)
- 비밀번호 입력 시 해당 사용자 비밀번호 변경
- 안전장치
  - 마지막 활성 관리자 삭제/비활성화 불가
  - 현재 로그인한 자기 계정 삭제/권한해제/비활성화 불가

## 4) 인증 API
- `GET /api/auth/bootstrap_status`
- `POST /api/auth/bootstrap`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`
- `POST /api/auth/change_password`

## 5) 테이블
- `staff_users`
  - `login_id`, `name`, `role`, `password_hash`, `is_admin`, `is_active`, `last_login_at`
- `auth_sessions`
  - `user_id`, `token_hash`, `expires_at`, `revoked_at`
