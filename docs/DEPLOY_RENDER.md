# Render 배포 가이드 (ka-part.com)

## 1) Deploy Hook URL 갱신 (Render 대시보드)
1. Render Dashboard > 서비스 `ka-part` > `Settings`
2. `Build & Deploy` 섹션의 `Deploy Hook`에서 `Regenerate hook` 클릭
3. 새 URL 복사
4. 기존 URL은 즉시 폐기되므로 더 이상 사용하지 않음

## 2) 로컬 환경변수에 새 Hook URL 저장
PowerShell (현재 세션):

```powershell
$env:RENDER_DEPLOY_HOOK_URL="https://api.render.com/deploy/srv-xxxx?key=yyyy"
```

PowerShell (사용자 영구 저장):

```powershell
[Environment]::SetEnvironmentVariable("RENDER_DEPLOY_HOOK_URL","https://api.render.com/deploy/srv-xxxx?key=yyyy","User")
```

## 3) 수동 배포 실행 (Hook 방식)
저장소 루트에서:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1
```

참고:
- `deploy_render.ps1`는 Hook 호출이 실패하면(`404` 등) Render API(`RENDER_SERVICE_ID` + `RENDER_API_KEY`)로 자동 fallback합니다.
- 만료된 Hook URL은 재발급 후 교체하거나, 변수 자체를 비워 API 모드만 사용해도 됩니다.

또는:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1 -HookUrl "https://api.render.com/deploy/srv-xxxx?key=yyyy"
```

## 4) API Key 방식(대체/고급)
Hook 대신 Render API를 써서 배포할 수도 있음:

```powershell
$env:RENDER_SERVICE_ID="srv-xxxx"
$env:RENDER_API_KEY="rnr_xxxx"
powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1 -Wait
```

`-Wait`를 사용하면 배포 상태를 polling해서 완료/실패를 출력함.

## 5) 배포 확인
```powershell
curl.exe -s https://www.ka-part.com/api/health
curl.exe -I https://www.ka-part.com/pwa/
curl.exe -I https://www.ka-part.com/parking/admin2
```

## 6) Render Runtime 설정 확인

- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## 7) 주차 연동 모드별 환경변수 체크리스트

### A. 내장 모드 (ka-part 단일 서비스, 기본)

입력 순서:

1. `ENABLE_PARKING_EMBED=1`
2. `PARKING_ROOT_PATH=/parking`
3. `PARKING_API_KEY=<강한 랜덤 문자열>`
4. `PARKING_SECRET_KEY=<강한 랜덤 문자열>`
5. `PARKING_CONTEXT_SECRET=<강한 랜덤 문자열>`
6. `PARKING_CONTEXT_MAX_AGE=300`
7. `PARKING_PORTAL_LOGIN_URL=https://www.ka-part.com/pwa/login.html?next=%2Fparking%2Fadmin2`

### B. 외부 parking_man 게이트웨이 모드 (독립 운영)

입력 순서:

1. `ENABLE_PARKING_EMBED=0`
2. `PARKING_BASE_URL=https://<parking-man-domain>`
3. `PARKING_SSO_PATH=/sso`
4. `PARKING_CONTEXT_SECRET=<parking_man과 동일값>`
5. `PARKING_CONTEXT_MAX_AGE=300`

`PARKING_SSO_PATH` 규칙:
- parking_man이 도메인 루트(`/sso`)로 서비스되면 `/sso`
- parking_man 자체가 `/parking` 하위로 서비스되면 `/parking/sso`

설정 후 Deploy Hook 또는 재배포를 실행합니다.

## 보안 메모
- Deploy Hook URL과 API Key는 비밀값으로 취급
- Git 저장소, 채팅, 스크린샷에 노출하지 않기
- 유출 의심 시 즉시 `Regenerate hook` 또는 API Key 재발급
