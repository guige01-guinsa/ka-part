# 사용자등록/관리 기능

## 개요
- 관리사무소 직원 사용자 등록/수정/삭제를 위한 기능
- 권장 인원(9명) 기준을 화면에 표시

## 화면
- `/pwa/users.html`

## API
- `GET /api/user_roles`
- `GET /api/users`
- `POST /api/users`
- `PATCH /api/users/{user_id}`
- `DELETE /api/users/{user_id}`

## DB
- `staff_users` 테이블 사용
- `login_id`는 고유값(대소문자 무시)
