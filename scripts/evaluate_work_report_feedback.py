from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import list_tenants, list_work_report_image_feedback  # noqa: E402
from app.work_report_evaluation import evaluate_deploy_readiness, summarize_feedback_rows  # noqa: E402


def _format_percent(value: float) -> str:
    return f"{float(value or 0.0) * 100:.1f}%"


def _tenant_targets(tenant_id: str) -> List[str]:
    clean = str(tenant_id or "").strip().lower()
    if clean:
        return [clean]
    return [str(item.get("id") or "").strip().lower() for item in list_tenants(active_only=True) if str(item.get("id") or "").strip()]


def _tenant_report(tenant_id: str, limit: int) -> Dict[str, Any]:
    rows = list_work_report_image_feedback(tenant_id=tenant_id, limit=limit)
    summary = summarize_feedback_rows(rows)
    readiness = evaluate_deploy_readiness(summary)
    return {
        "tenant_id": tenant_id,
        "feedback_rows": rows,
        "summary": summary,
        "readiness": readiness,
    }


def _print_text_report(report: Dict[str, Any]) -> None:
    summary = dict(report.get("summary") or {})
    readiness = dict(report.get("readiness") or {})
    print(f"[{report.get('tenant_id')}]")
    print(
        "  rows={rows} choice={choice} top1={top1} top3={top3} intervention={intervention} unmatched_fp={unmatched}".format(
            rows=int(summary.get("total_feedback_rows") or 0),
            choice=int(summary.get("choice_feedback_rows") or 0),
            top1=_format_percent(float(summary.get("top1_accuracy") or 0.0)),
            top3=_format_percent(float(summary.get("top3_hit_rate") or 0.0)),
            intervention=_format_percent(float(summary.get("human_intervention_rate") or 0.0)),
            unmatched=_format_percent(float(summary.get("unmatched_false_positive_rate") or 0.0)),
        )
    )
    print(f"  deploy_ready={'YES' if readiness.get('ready') else 'NO'}")
    for check in list(readiness.get("checks") or []):
        status = "OK" if check.get("ok") else "FAIL"
        actual = check.get("actual")
        expected = check.get("expected")
        if isinstance(actual, float):
            actual_text = _format_percent(actual)
            expected_text = _format_percent(float(expected or 0.0))
        else:
            actual_text = str(actual)
            expected_text = str(expected)
        print(f"    - {status} {check.get('name')}: {actual_text} {check.get('direction')} {expected_text}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate work-report image matching quality from stored feedback.")
    parser.add_argument("--tenant-id", default="", help="Single tenant_id to evaluate. Omit to evaluate all active tenants.")
    parser.add_argument("--limit", type=int, default=500, help="Feedback rows to inspect per tenant.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a text summary.")
    args = parser.parse_args()

    reports = [_tenant_report(tenant_id, max(1, int(args.limit))) for tenant_id in _tenant_targets(str(args.tenant_id or ""))]
    if args.json:
        payload = [
            {
                "tenant_id": report["tenant_id"],
                "summary": report["summary"],
                "readiness": report["readiness"],
            }
            for report in reports
        ]
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if not reports:
        print("No tenants found.")
        return 0
    for index, report in enumerate(reports):
        if index > 0:
            print()
        _print_text_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
