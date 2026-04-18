# 업무보고 이미지 매칭 학습 데이터셋

최종 업데이트: 2026-04-18

## 목적

사람이 수정하거나 확정한 이미지 매칭 결과를 `few-shot 예시`와 `장기 학습용 JSONL`로 재사용한다.
즉시 온라인 재학습은 하지 않고, 먼저 안전한 프롬프트 보강과 오프라인 데이터 축적에 쓴다.

## export 스크립트

단일 테넌트 JSONL 출력:

```bash
python scripts/export_work_report_learning_dataset.py --tenant-id ys_thesharp
```

파일로 저장:

```bash
python scripts/export_work_report_learning_dataset.py --tenant-id ys_thesharp --output runtime/work-report-learning.jsonl
```

전체 활성 테넌트 export:

```bash
python scripts/export_work_report_learning_dataset.py --limit 500 --output runtime/work-report-learning-all.jsonl
```

## JSONL 구조

각 줄은 아래 성격의 레코드다.

- `task`
  현재는 `work_report_image_feedback`
- `tenant_id`, `job_id`, `created_at`
  피드백이 발생한 범위와 시점
- `input`
  파일명, 원래 선택 항목, 후보 3개, 검토 사유, 분석 모델/사유
- `target`
  사람이 최종적으로 선택한 결과
  `keep_current_item`, `reassign_item`, `leave_unmatched`, `change_stage`

## 운영 원칙

- few-shot 예시는 최근 고신호 피드백만 제한적으로 사용한다.
- 잘못된 한 번의 수정을 전체 모델에 즉시 학습시키지 않는다.
- JSONL은 재랭커 검증, few-shot 예시 선별, 향후 파인튜닝 후보셋 정리에 사용한다.
