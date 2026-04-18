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
from app.work_report_learning import build_feedback_learning_dataset  # noqa: E402


def _tenant_targets(tenant_id: str) -> List[str]:
    clean = str(tenant_id or "").strip().lower()
    if clean:
        return [clean]
    return [str(item.get("id") or "").strip().lower() for item in list_tenants(active_only=True) if str(item.get("id") or "").strip()]


def _collect_examples(tenant_id: str, limit: int) -> List[Dict[str, Any]]:
    rows = list_work_report_image_feedback(tenant_id=tenant_id, limit=limit)
    return build_feedback_learning_dataset(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export work-report image feedback as JSONL learning data.")
    parser.add_argument("--tenant-id", default="", help="Single tenant_id to export. Omit to export all active tenants.")
    parser.add_argument("--limit", type=int, default=500, help="Feedback rows to inspect per tenant.")
    parser.add_argument("--output", default="", help="Optional JSONL output path. Omit to print to stdout.")
    args = parser.parse_args()

    lines: List[str] = []
    for tenant_id in _tenant_targets(str(args.tenant_id or "")):
        for row in _collect_examples(tenant_id, max(1, int(args.limit or 1))):
            lines.append(json.dumps(row, ensure_ascii=False))

    if args.output:
        output_path = Path(str(args.output)).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        print(f"Wrote {len(lines)} rows to {output_path}")
        return 0

    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
