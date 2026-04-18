from __future__ import annotations

from app.work_report_evaluation import (
    WorkReportDeployThresholds,
    evaluate_deploy_readiness,
    summarize_feedback_rows,
)


def test_summarize_feedback_rows_computes_core_metrics() -> None:
    rows = [
        {
            "tenant_id": "ys_thesharp",
            "feedback_type": "confirm_current",
            "to_item_index": 2,
            "candidate_items": [
                {"item_index": 2, "title": "104동 키패드 교체", "score": 10},
                {"item_index": 1, "title": "101동 센서등 교체", "score": 7},
            ],
        },
        {
            "tenant_id": "ys_thesharp",
            "feedback_type": "reassign_item",
            "to_item_index": 3,
            "candidate_items": [
                {"item_index": 1, "title": "101동 센서등 교체", "score": 10},
                {"item_index": 3, "title": "103동 출입문 보수", "score": 9},
            ],
        },
        {
            "tenant_id": "ys_thesharp",
            "feedback_type": "mark_unmatched",
            "to_item_index": 0,
            "candidate_items": [
                {"item_index": 4, "title": "104동 램프 교체", "score": 8},
            ],
        },
        {
            "tenant_id": "ys_thesharp",
            "feedback_type": "change_stage",
            "to_item_index": 2,
            "candidate_items": [],
        },
    ]

    summary = summarize_feedback_rows(rows)

    assert summary["total_feedback_rows"] == 4
    assert summary["choice_feedback_rows"] == 3
    assert summary["candidate_feedback_rows"] == 3
    assert summary["top1_eligible_rows"] == 2
    assert summary["top1_hits"] == 1
    assert summary["top3_hits"] == 2
    assert summary["human_intervention_rows"] == 2
    assert summary["unmatched_false_positive_rows"] == 1
    assert summary["top1_accuracy"] == 0.5
    assert summary["top3_hit_rate"] == 1.0
    assert round(summary["human_intervention_rate"], 4) == round(2 / 3, 4)
    assert round(summary["stage_adjustment_rate"], 4) == 0.25


def test_evaluate_deploy_readiness_checks_thresholds() -> None:
    summary = {
        "choice_feedback_rows": 42,
        "top1_accuracy": 0.78,
        "top3_hit_rate": 0.95,
        "human_intervention_rate": 0.22,
        "unmatched_false_positive_rate": 0.08,
    }

    result = evaluate_deploy_readiness(
        summary,
        thresholds=WorkReportDeployThresholds(
            min_choice_feedback_rows=30,
            min_top1_accuracy=0.70,
            min_top3_hit_rate=0.90,
            max_human_intervention_rate=0.35,
            max_unmatched_false_positive_rate=0.18,
        ),
    )

    assert result["ready"] is True
    assert all(check["ok"] for check in result["checks"])
