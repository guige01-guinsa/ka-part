# Render 배포 가이드 (ka-part.com)

## 현재 확인된 상태
- `www.ka-part.com` -> `ka-part.onrender.com`
- 현재 서비스는 이 저장소 코드와 다른 앱(`/ui/works`)이 구동 중

즉, `ka-part.com`에 이 코드를 올리려면 Render 서비스 권한이 필요합니다.

## 방법 1) Render 대시보드에서 수동 배포
1. Render Dashboard 접속
2. `ka-part` 서비스 선택
3. `Manual Deploy` 실행
4. 배포 완료 후 `https://www.ka-part.com/` 확인

## 방법 2) API로 배포 트리거 (자동)
이 저장소 루트의 `deploy_render.ps1` 사용:

```powershell
$env:RENDER_SERVICE_ID="srv-xxxxxxxxxxxx"
$env:RENDER_API_KEY="rnr_xxxxxxxxxxxx"
powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1
```

또는:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1 -ServiceId srv-xxxxxxxxxxxx -ApiKey rnr_xxxxxxxxxxxx
```

## 배포 후 확인
```powershell
curl.exe -I https://www.ka-part.com/
curl.exe -s https://www.ka-part.com/openapi.json
```
