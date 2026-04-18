# 업무보고 이미지 매칭 평가 기준

최종 업데이트: 2026-04-18

## 목적

업무보고 이미지 매칭 개선이 실제로 좋아졌는지 `피드백 데이터` 기준으로 수치화한다.
감으로 배포하지 않고, 최소 기준을 넘는 경우에만 매칭 로직을 강화하거나 재랭킹 가중치를 높인다.

## 평가 스크립트

단일 테넌트:

```bash
python scripts/evaluate_work_report_feedback.py --tenant-id ys_thesharp
```

전체 활성 테넌트:

```bash
python scripts/evaluate_work_report_feedback.py
```

JSON 출력:

```bash
python scripts/evaluate_work_report_feedback.py --tenant-id ys_thesharp --json
```

## 핵심 지표

- `top1_accuracy`
  현재 추천 1순위가 사람의 최종 선택과 같았던 비율
- `top3_hit_rate`
  사람의 최종 선택이 추천 후보 3개 안에 들어간 비율
- `human_intervention_rate`
  사람이 `재배정` 또는 `미매칭 전환`까지 한 비율
- `unmatched_false_positive_rate`
  추천 후보가 있었지만 사람이 `미매칭`으로 남긴 비율
- `confirm_current_rate`
  사람이 현재 선택이 맞다고 확정한 비율
- `stage_adjustment_rate`
  항목 매칭은 유지하고 사진 단계만 바꾼 비율

## 권장 배포 기준

아래 기준을 모두 만족하면 `테넌트별 재랭킹`이나 매칭 로직 개선안을 운영 반영 후보로 본다.

- `choice_feedback_rows >= 30`
- `top1_accuracy >= 70%`
- `top3_hit_rate >= 90%`
- `human_intervention_rate <= 35%`
- `unmatched_false_positive_rate <= 18%`

## 해석 가이드

- `top1`은 낮고 `top3`는 높다:
  후보 추출은 괜찮지만 순위가 아쉽다. 재랭킹 개선 우선.
- `top3`도 낮다:
  후보 자체가 잘못 나오므로 토큰/시간/위치 단서 로직을 먼저 봐야 한다.
- `human_intervention_rate`가 높다:
  검토 큐가 너무 많이 뜨거나 기본 매칭이 약하다.
- `unmatched_false_positive_rate`가 높다:
  시스템이 억지 매칭을 많이 시도하고 있다는 뜻이다. 보수성 강화 필요.

## 운영 원칙

- 피드백이 적을 때는 가중치를 크게 바꾸지 않는다.
- 전체 공통 규칙보다 `테넌트별 패턴`을 우선 재랭킹으로 다룬다.
- 오프라인 지표가 개선되지 않으면 새 보정 규칙은 배포하지 않는다.
