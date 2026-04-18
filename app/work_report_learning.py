from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Sequence


def _collapse(value: Any) -> str:
    return " ".join(str(value or "").replace("\u0000", " ").split()).strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _candidate_items(value: Any) -> List[Dict[str, Any]]:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    if not isinstance(raw, list):
        return []
    items: List[Dict[str, Any]] = []
    for candidate in raw:
        if not isinstance(candidate, dict):
            continue
        title = _collapse(candidate.get("title") or "")
        if not title:
            continue
        items.append(
            {
                "item_index": _int(candidate.get("item_index") or 0),
                "title": title[:240],
                "score": _int(candidate.get("score") or 0),
            }
        )
    return items


def _normalize_feedback_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        normalized.append(
            {
                "tenant_id": _collapse(raw.get("tenant_id") or "").lower(),
                "job_id": _collapse(raw.get("job_id") or ""),
                "feedback_type": _collapse(raw.get("feedback_type") or ""),
                "filename": _collapse(raw.get("filename") or "")[:240],
                "from_item_index": _int(raw.get("from_item_index") or 0),
                "from_item_title": _collapse(raw.get("from_item_title") or "")[:240],
                "to_item_index": _int(raw.get("to_item_index") or 0),
                "to_item_title": _collapse(raw.get("to_item_title") or "")[:240],
                "from_stage": _collapse(raw.get("from_stage") or "")[:40],
                "to_stage": _collapse(raw.get("to_stage") or "")[:40],
                "review_reason": _collapse(raw.get("review_reason") or "")[:240],
                "review_confidence": _collapse(raw.get("review_confidence") or "")[:40],
                "analysis_model": _collapse(raw.get("analysis_model") or "")[:80],
                "analysis_reason": _collapse(raw.get("analysis_reason") or "")[:80],
                "report_title": _collapse(raw.get("report_title") or "")[:160],
                "period_label": _collapse(raw.get("period_label") or "")[:120],
                "created_at": _collapse(raw.get("created_at") or ""),
                "candidate_items": _candidate_items(raw.get("candidate_items") if "candidate_items" in raw else raw.get("candidate_items_json")),
            }
        )
    return normalized


def build_feedback_few_shot_examples(rows: Sequence[Dict[str, Any]], *, limit: int = 6) -> List[Dict[str, Any]]:
    examples: List[Dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    max_examples = max(1, int(limit or 1))
    for row in _normalize_feedback_rows(rows):
        feedback_type = row["feedback_type"]
        candidate_items = list(row.get("candidate_items") or [])
        from_title = _collapse(row.get("from_item_title") or "")
        to_title = _collapse(row.get("to_item_title") or "")
        if feedback_type not in {"reassign_item", "confirm_current", "mark_unmatched"}:
            continue
        if feedback_type != "mark_unmatched" and not to_title:
            continue
        if feedback_type == "confirm_current" and len(candidate_items) < 2:
            continue
        if feedback_type == "mark_unmatched" and not candidate_items:
            continue
        candidate_titles = [str(candidate.get("title") or "").strip()[:240] for candidate in candidate_items if str(candidate.get("title") or "").strip()]
        dedupe_key = (
            feedback_type,
            row.get("filename") or "",
            from_title,
            to_title,
            tuple(candidate_titles[:3]),
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        if feedback_type == "mark_unmatched":
            decision_label = "미매칭 유지"
        elif feedback_type == "reassign_item":
            decision_label = "사람이 다른 작업으로 재배정"
        else:
            decision_label = "현재 선택 확정"
        examples.append(
            {
                "feedback_type": feedback_type,
                "decision_label": decision_label,
                "filename": row.get("filename") or "",
                "from_item_index": int(row.get("from_item_index") or 0),
                "from_item_title": from_title,
                "to_item_index": int(row.get("to_item_index") or 0),
                "to_item_title": to_title,
                "candidate_items": candidate_items[:3],
                "review_reason": _collapse(row.get("review_reason") or ""),
                "review_confidence": _collapse(row.get("review_confidence") or ""),
                "created_at": _collapse(row.get("created_at") or ""),
            }
        )
        if len(examples) >= max_examples:
            break
    return examples


def build_feedback_learning_dataset(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dataset: List[Dict[str, Any]] = []
    for row in _normalize_feedback_rows(rows):
        feedback_type = row["feedback_type"]
        candidate_items = list(row.get("candidate_items") or [])
        if feedback_type not in {"reassign_item", "confirm_current", "mark_unmatched", "change_stage"}:
            continue
        target: Dict[str, Any] = {
            "feedback_type": feedback_type,
            "item_index": int(row.get("to_item_index") or 0),
            "item_title": _collapse(row.get("to_item_title") or ""),
            "stage": _collapse(row.get("to_stage") or ""),
        }
        if feedback_type == "mark_unmatched":
            target["decision"] = "leave_unmatched"
            target["item_index"] = 0
            target["item_title"] = ""
        elif feedback_type == "change_stage":
            target["decision"] = "change_stage"
            target["item_index"] = int(row.get("from_item_index") or row.get("to_item_index") or 0)
            target["item_title"] = _collapse(row.get("to_item_title") or row.get("from_item_title") or "")
        elif feedback_type == "confirm_current":
            target["decision"] = "keep_current_item"
        else:
            target["decision"] = "reassign_item"
        dataset.append(
            {
                "task": "work_report_image_feedback",
                "tenant_id": _collapse(row.get("tenant_id") or ""),
                "job_id": _collapse(row.get("job_id") or ""),
                "created_at": _collapse(row.get("created_at") or ""),
                "input": {
                    "filename": _collapse(row.get("filename") or ""),
                    "report_title": _collapse(row.get("report_title") or ""),
                    "period_label": _collapse(row.get("period_label") or ""),
                    "analysis_model": _collapse(row.get("analysis_model") or ""),
                    "analysis_reason": _collapse(row.get("analysis_reason") or ""),
                    "from_item": {
                        "item_index": int(row.get("from_item_index") or 0),
                        "title": _collapse(row.get("from_item_title") or ""),
                        "stage": _collapse(row.get("from_stage") or ""),
                    },
                    "candidate_items": candidate_items[:3],
                    "review_reason": _collapse(row.get("review_reason") or ""),
                    "review_confidence": _collapse(row.get("review_confidence") or ""),
                },
                "target": target,
            }
        )
    return dataset
