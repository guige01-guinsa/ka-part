from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _candidate_items(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    value = row.get("candidate_items")
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, dict)]
    return []


def _candidate_top1(row: Dict[str, Any]) -> int:
    candidates = _candidate_items(row)
    if not candidates:
        return 0
    return _int(candidates[0].get("item_index") or 0)


def _candidate_contains_choice(row: Dict[str, Any], *, limit: int = 3) -> bool:
    selected = _int(row.get("to_item_index") or 0)
    if selected <= 0:
        return False
    for candidate in _candidate_items(row)[: max(1, int(limit or 1))]:
        if _int(candidate.get("item_index") or 0) == selected:
            return True
    return False


def _normalize_feedback_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["feedback_type"] = _text(row.get("feedback_type") or "")
        row["tenant_id"] = _text(row.get("tenant_id") or "")
        row["job_id"] = _text(row.get("job_id") or "")
        row["filename"] = _text(row.get("filename") or "")
        row["to_item_index"] = _int(row.get("to_item_index") or 0)
        row["from_item_index"] = _int(row.get("from_item_index") or 0)
        row["candidate_items"] = _candidate_items(row)
        normalized.append(row)
    return normalized


def summarize_feedback_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    normalized = _normalize_feedback_rows(rows)
    total_rows = len(normalized)
    reassign_rows = [row for row in normalized if row["feedback_type"] == "reassign_item"]
    confirm_rows = [row for row in normalized if row["feedback_type"] == "confirm_current"]
    unmatched_rows = [row for row in normalized if row["feedback_type"] == "mark_unmatched"]
    stage_rows = [row for row in normalized if row["feedback_type"] == "change_stage"]
    choice_rows = [row for row in normalized if row["feedback_type"] in {"reassign_item", "confirm_current", "mark_unmatched"}]
    candidate_rows = [row for row in choice_rows if row["candidate_items"]]
    matched_candidate_rows = [row for row in candidate_rows if row["to_item_index"] > 0]
    top1_hits = sum(1 for row in matched_candidate_rows if _candidate_top1(row) == row["to_item_index"])
    top3_hits = sum(1 for row in matched_candidate_rows if _candidate_contains_choice(row, limit=3))
    intervention_rows = [row for row in choice_rows if row["feedback_type"] in {"reassign_item", "mark_unmatched"}]
    unmatched_false_positive_rows = [row for row in unmatched_rows if row["candidate_items"]]

    return {
        "total_feedback_rows": total_rows,
        "choice_feedback_rows": len(choice_rows),
        "candidate_feedback_rows": len(candidate_rows),
        "top1_eligible_rows": len(matched_candidate_rows),
        "top1_hits": top1_hits,
        "top3_hits": top3_hits,
        "confirm_current_rows": len(confirm_rows),
        "reassign_rows": len(reassign_rows),
        "mark_unmatched_rows": len(unmatched_rows),
        "change_stage_rows": len(stage_rows),
        "human_intervention_rows": len(intervention_rows),
        "unmatched_false_positive_rows": len(unmatched_false_positive_rows),
        "top1_accuracy": _safe_ratio(top1_hits, len(matched_candidate_rows)),
        "top3_hit_rate": _safe_ratio(top3_hits, len(matched_candidate_rows)),
        "human_intervention_rate": _safe_ratio(len(intervention_rows), len(choice_rows)),
        "confirm_current_rate": _safe_ratio(len(confirm_rows), len(choice_rows)),
        "stage_adjustment_rate": _safe_ratio(len(stage_rows), total_rows),
        "unmatched_false_positive_rate": _safe_ratio(len(unmatched_false_positive_rows), len(candidate_rows)),
    }


@dataclass(frozen=True)
class WorkReportDeployThresholds:
    min_choice_feedback_rows: int = 30
    min_top1_accuracy: float = 0.70
    min_top3_hit_rate: float = 0.90
    max_human_intervention_rate: float = 0.35
    max_unmatched_false_positive_rate: float = 0.18


def evaluate_deploy_readiness(
    summary: Dict[str, Any],
    *,
    thresholds: WorkReportDeployThresholds | None = None,
) -> Dict[str, Any]:
    limits = thresholds or WorkReportDeployThresholds()
    checks = [
        {
            "name": "choice_feedback_rows",
            "ok": _int(summary.get("choice_feedback_rows") or 0) >= int(limits.min_choice_feedback_rows),
            "actual": _int(summary.get("choice_feedback_rows") or 0),
            "expected": int(limits.min_choice_feedback_rows),
            "direction": ">=",
        },
        {
            "name": "top1_accuracy",
            "ok": float(summary.get("top1_accuracy") or 0.0) >= float(limits.min_top1_accuracy),
            "actual": float(summary.get("top1_accuracy") or 0.0),
            "expected": float(limits.min_top1_accuracy),
            "direction": ">=",
        },
        {
            "name": "top3_hit_rate",
            "ok": float(summary.get("top3_hit_rate") or 0.0) >= float(limits.min_top3_hit_rate),
            "actual": float(summary.get("top3_hit_rate") or 0.0),
            "expected": float(limits.min_top3_hit_rate),
            "direction": ">=",
        },
        {
            "name": "human_intervention_rate",
            "ok": float(summary.get("human_intervention_rate") or 0.0) <= float(limits.max_human_intervention_rate),
            "actual": float(summary.get("human_intervention_rate") or 0.0),
            "expected": float(limits.max_human_intervention_rate),
            "direction": "<=",
        },
        {
            "name": "unmatched_false_positive_rate",
            "ok": float(summary.get("unmatched_false_positive_rate") or 0.0) <= float(limits.max_unmatched_false_positive_rate),
            "actual": float(summary.get("unmatched_false_positive_rate") or 0.0),
            "expected": float(limits.max_unmatched_false_positive_rate),
            "direction": "<=",
        },
    ]
    ready = all(bool(check["ok"]) for check in checks)
    return {
        "ready": ready,
        "checks": checks,
        "thresholds": {
            "min_choice_feedback_rows": limits.min_choice_feedback_rows,
            "min_top1_accuracy": limits.min_top1_accuracy,
            "min_top3_hit_rate": limits.min_top3_hit_rate,
            "max_human_intervention_rate": limits.max_human_intervention_rate,
            "max_unmatched_false_positive_rate": limits.max_unmatched_false_positive_rate,
        },
    }


def summarize_by_tenant(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = _normalize_feedback_rows(rows)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in normalized:
        tenant_id = _text(row.get("tenant_id") or "")
        grouped.setdefault(tenant_id, []).append(row)
    results: List[Dict[str, Any]] = []
    for tenant_id, tenant_rows in sorted(grouped.items(), key=lambda item: item[0]):
        summary = summarize_feedback_rows(tenant_rows)
        readiness = evaluate_deploy_readiness(summary)
        results.append(
            {
                "tenant_id": tenant_id,
                "summary": summary,
                "readiness": readiness,
            }
        )
    return results
