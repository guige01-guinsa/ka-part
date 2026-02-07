# OPS / 점검 기록 (Relational v2)

## 핵심 변경점
- '탭 전환이 되면 DB가 안 되고, DB가 되면 탭이 안 되는' 문제는 **DOM 재생성(innerHTML) + 초기 바인딩 방식** 충돌이었습니다.
- v2에서는 **모든 탭/폼을 최초 1회 생성**하고, 이후에는 display 토글만 합니다. 이벤트 바인딩은 1회만 수행됩니다.

## API 확인
- /api/routes : 현재 서버에 실제 등록된 API 목록
- /api/health : 기동 확인
- /api/schema_alignment : DB 컬럼과 폼 입력항목 매핑 정합성 확인

## 버튼 동작
- 조회 → GET /api/load
- 저장 → POST /api/save
- 삭제 → DELETE /api/delete
- 엑셀 → GET /api/export (xlsx 다운로드)
- PDF → GET /api/pdf (pdf 다운로드)

## 향후 확장(권장 순서)
1) 필드 확장: app/schema_defs.py의 SCHEMA_DEFS/TAB_STORAGE_SPECS만 수정
2) 이력 화면: /api/list 추가 후, 날짜 범위 목록 렌더
3) 점검이력 누적 PDF: date_from/date_to 받아 다중 페이지 생성
4) 사진 첨부: uploads 저장 + entry_files 테이블 추가
