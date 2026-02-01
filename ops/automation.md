# 자동 보고서(월 1회) 설정 가이드

## 목적
- 매월 1회 입주자대표자 회의 제출용 월간 보고서를 자동 생성합니다.
- 기본 출력: PDF/HTML/JSON (reportgen 스크립트)
- API 출력: PDF/DOCX/XLSX/HWP (FastAPI endpoints)

## 1) reportgen 스크립트 방식(권장: 오프라인 자동화)
1. Python 가상환경 활성화
2. 다음 명령으로 전월 보고서 생성

```powershell
cd c:\ka-part
.\.venv\Scripts\python.exe .\reportgen\generate_monthly_work_report.py --ym 2026-01
```

3. 결과물 확인
- `c:\ka-part\reports\2026\01\RPT-WORK-2026-01-0001.pdf`
- `c:\ka-part\reports\2026\01\RPT-WORK-2026-01-0001.html`
- `c:\ka-part\reports\2026\01\RPT-WORK-2026-01-0001.json`

## 2) FastAPI API 방식(웹)
- PDF: `GET /api/reports/monthly-work.pdf?yyyymm=2026-01&login=admin`
- DOCX: `GET /api/reports/monthly-work.docx?yyyymm=2026-01&login=admin`
- XLSX: `GET /api/reports/monthly-work.xlsx?yyyymm=2026-01&login=admin`
- HWP: `GET /api/reports/monthly-work.hwp?yyyymm=2026-01&login=admin`

## 3) Windows 작업 스케줄러 등록 예시
1. 작업 스케줄러 실행
2. 기본 작업 만들기 → 트리거: 매월 1일 09:00
3. 동작: 프로그램 시작
   - 프로그램/스크립트: `c:\ka-part\.venv\Scripts\python.exe`
   - 인수: `c:\ka-part\reportgen\generate_monthly_work_report.py --ym $(Get-Date -Format "yyyy-MM" -Date (Get-Date).AddMonths(-1))`
   - 시작 위치: `c:\ka-part`

## 4) HWP 변환 주의사항
- HWP 변환은 외부 변환기(한컴 Office 또는 LibreOffice)가 필요합니다.
- 설치 후 `SOFFICE_PATH` 환경변수에 soffice 경로를 지정하세요.
