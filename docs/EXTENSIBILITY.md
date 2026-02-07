# 확장 구조 가이드 (시설관리 하부 업무)

## 목표
- `tr450` / `tr400` / `meter` / `facility_check`는 기존처럼 분리 저장
- 향후 하부 업무(소방/기계/통신)는 동일 패턴으로 안전하게 확장

## 현재 적용된 구조
- 폼 정의 단일 기준: `app/schema_defs.py`
- 저장 매핑 단일 기준: `app/schema_defs.py`의 `TAB_STORAGE_SPECS`
- 레거시 키 자동 정규화: `LEGACY_FIELD_ALIASES`
- 확장용 공통 테이블: `facility_subtasks`
  - `domain_key`: `fire` / `mechanical` / `telecom`
  - `UNIQUE(site_name, entry_date, domain_key)`로 중복 방지

## 신규 하부 업무 추가 방법
1. `SCHEMA_DEFS`에 탭 추가
2. `TAB_STORAGE_SPECS`에 table/key/column_map 추가
3. 필요한 경우 `domain_key` 고정값(`fixed`) 지정
4. 프론트 수정 없이 자동으로 탭 렌더링

## 정합성 점검 API
- `GET /api/schema_alignment`
- 폼 필드와 DB 컬럼 매핑이 맞지 않으면 항목별로 반환
