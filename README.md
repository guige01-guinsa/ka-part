# ka-part (배포용 패키지)

현장 기록(PWA) + FastAPI + SQLite(ka-part/data/ka.db)로 구성된 경량 운영 앱입니다.

## 1) 실행 (PC / Linux / 서버)

```bash
cd ka-part
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt

python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

- 접속: `http://<서버IP>:8000/`  → 자동으로 `/pwa/`로 리다이렉트
- PWA: `http://<서버IP>:8000/pwa/`

## 2) 안드로이드(Termux) 실행

```bash
cd /storage/emulated/0/수변전일지/ka-part
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

같은 Wi‑Fi/LTE 환경에서 브라우저로 `http://<폰IP>:8000/` 접속 후 홈화면에 추가(PWA).

## 3) DB 경로 / 구조

- 운영 DB: `ka-part/data/ka.db`
- 초기 테이블:
  - sites / entries / entry_values (유연 스키마: 탭/필드 추가 대응)
  - home_logs / transformer_450_reads / transformer_400_reads / power_meter_reads / facility_tasks / facility_checks (도메인 테이블)

> 현재 앱은 **entry_values** 기반으로 저장/조회가 동작합니다.
> 도메인 테이블은 보고서/집계 고도화를 위한 기반으로 준비되어 있습니다.

## 4) 스키마/마이그레이션

- 기본 스키마: `sql/schema.sql`
- 현장 도메인 테이블 추가: `sql/20260206_migration_domain_tables.sql`

수동 실행:
```bash
sqlite3 data/ka.db ".read sql/schema.sql"
sqlite3 data/ka.db ".read sql/20260206_migration_domain_tables.sql"
```

## 5) 배포 (권장)

### A. Docker
```bash
docker build -t ka-part .
docker run -p 8000:8000 -v $(pwd)/data:/app/data ka-part
```

### B. Render/Railway 등
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Persistent disk(또는 volume)에 `data/`를 마운트(DB 유지)

## 6) 운영 팁
- PWA UI가 안 바뀌면: 브라우저 강력 새로고침 / PWA 앱 삭제 후 재설치
- 엑셀 다운로드 한글 파일명은 `filename*=`로 처리(안전)

버전: 2026-02-06
