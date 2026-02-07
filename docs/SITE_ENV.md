# 단지 제원 환경변수 가이드

단지별로 입력 양식을 다르게 적용할 때 사용하는 설정입니다.

- 관리자 화면: `/pwa/spec_env.html`
- 단지별 저장 테이블: `site_env_configs`

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

## 주의
- `add_fields`로 추가한 값은 `entry_values`에 저장됩니다.
- 도메인 고정 테이블(`transformer_450_reads` 등)에는 기본 매핑 필드만 저장됩니다.
