# 단지 제원 환경변수 가이드

단지별로 입력 양식을 다르게 적용할 때 사용하는 설정입니다.

- 관리자 화면: `/pwa/spec_env.html`
- 단지별 저장 테이블: `site_env_configs`

## 관리자 사용 절차 (시각 편집기)
1. `site_name` 입력
2. 템플릿 선택 후 `템플릿 불러오기`
3. 체크/선택 방식으로 설정
- 숨길 탭: 체크박스로 선택
- 숨길 항목: 탭 선택 후 항목 체크
- 항목명 변경: 입력칸에서 라벨 변경
- 추가 항목: 키/라벨/타입/범위 입력
- 행 레이아웃: 줄 단위(콤마 구분)로 재배치
4. 필요 시 JSON 직접 수정
5. `저장`으로 단지별 적용

## 기본 구조
```json
{
  "hide_tabs": ["facility_telecom"],
  "tabs": {
    "tr450": {
      "title": "변압기450 (지하)",
      "hide_fields": ["lv1_temp"],
      "field_labels": {
        "lv1_L1_V": "R상 전압(V)"
      },
      "field_overrides": {
        "lv1_L1_V": { "warn_min": 190, "warn_max": 250 }
      },
      "add_fields": [
        { "k": "lv1_oil_level", "label": "유면(%)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 100 }
      ],
      "rows": [
        ["lv1_L1_V", "lv1_L1_A", "lv1_L1_KW"],
        ["lv1_L2_V", "lv1_L2_A", "lv1_L2_KW"],
        ["lv1_L3_V", "lv1_L3_A", "lv1_L3_KW"],
        ["lv1_oil_level"]
      ]
    }
  }
}
```

## 동작
- `hide_tabs`: 탭 자체 숨김
- `tabs.{tab}.title`: 탭 제목 변경
- `hide_fields`: 특정 필드 숨김
- `field_labels`: 라벨 변경
- `field_overrides`: 범위/타입/placeholder 등 속성 변경
- `add_fields`: 신규 필드 추가
- `rows`: 레이아웃 행 재구성

## 템플릿 API
- `GET /api/site_env_templates`
- `GET /api/site_env_template`

## 주의
- `add_fields`로 추가한 값은 `entry_values`에 저장됩니다.
- 도메인 고정 테이블(`transformer_450_reads` 등)에는 기본 매핑 필드만 저장됩니다.
