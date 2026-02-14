# 보안/안정 운영 체크리스트

이 문서는 `ka-part` 운영 환경(Render 등)에서 보안과 안정성을 유지하기 위한 최소 체크리스트입니다.

## 1) 즉시(High)

- **유출된 비밀값 회수/재발급**
  - DB 접속 URL/비밀번호, Render Deploy Hook URL/API Key 등은 유출 시 즉시 폐기/재발급합니다.
- **부트스트랩(최초 관리자 생성) 보호**
  - 운영에서는 `KA_BOOTSTRAP_TOKEN`을 설정하고, 최초 관리자 생성 시에만 토큰을 제출하도록 구성합니다.
- **문자 인증(가입) 남용 방지**
  - `KA_SIGNUP_SMS_RATE_LIMIT_*`를 설정해 SMS 재요청 폭주/남용을 차단합니다.

## 2) 권장(Production 기본값)

- 세션/쿠키
  - `KA_AUTH_COOKIE_SECURE=1`
  - `KA_AUTH_COOKIE_SAMESITE=lax` (필요 시 `strict`, `none`은 반드시 HTTPS)
  - `KA_ALLOW_QUERY_ACCESS_TOKEN=0` (URL 토큰은 로그/리퍼러로 유출 위험)
- 강한 비밀값(고정값) 필수
  - `KA_PHONE_VERIFY_SECRET` (휴대폰 인증 코드 해시)
  - `PARKING_CONTEXT_SECRET` (주차 SSO 컨텍스트 서명)
  - `PARKING_API_KEY`, `PARKING_SECRET_KEY` (주차 서비스 연동)
- TLS/HSTS
  - `KA_HSTS_ENABLED=1` (HTTPS 환경에서만 적용)

## 3) 신규가입 SMS Rate Limit(권장값)

- `KA_SIGNUP_SMS_RATE_LIMIT_ENABLED=1`
- `KA_SIGNUP_SMS_RATE_LIMIT_WINDOW_MIN=15`
- `KA_SIGNUP_SMS_RATE_LIMIT_MAX_PER_PHONE=3`
- `KA_SIGNUP_SMS_RATE_LIMIT_MAX_PER_IP=30`

운영 환경에서 가입이 집중되는 경우(동시간 다수 가입)에는 IP 기준 상한을 완화하세요.

## 4) 로그인 실패 Rate Limit(권장값)

- `KA_LOGIN_RATE_LIMIT_ENABLED=1`
- `KA_LOGIN_RATE_LIMIT_WINDOW_SEC=600`
- `KA_LOGIN_RATE_LIMIT_MAX_FAILURES=10`

## 5) 데이터 무결성/테넌트(단지) 범위

- 민원/일지/백업 등 모든 저장/조회는 사용자 소속 단지 범위를 벗어나지 않도록 서버에서 강제합니다.
- 민원 생성 시 non-admin 계정의 `site_code/site_name`은 서버가 소속값으로 강제합니다.

## 6) 점검/모니터링

- `GET /api/health` : 기본 상태 + 스키마 정렬 여부
- `GET /api/schema_alignment` (관리자) : 스키마/인덱스 불일치 감지
- `GET /api/security/audit_logs` (관리자) : 중요 변경/오류 추적
- `GET /api/ops/diagnostics` (관리자) : 자동 운영 진단 상태

## 7) SQLite 안정성(권장)

동시 접근이 많을 때 `database is locked` 오류가 발생할 수 있습니다.

- `KA_SQLITE_TIMEOUT_SEC=30`
- `KA_SQLITE_BUSY_TIMEOUT_MS=30000`
