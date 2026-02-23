from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import find_site_name_by_id, resolve_or_create_site_code, site_identity_consistency_report


def _orphan_sites(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw in report.get("issues") or []:
        item = raw if isinstance(raw, dict) else {}
        if str(item.get("section") or "").strip() != "sites_without_registry_code":
            continue
        site_id = int(item.get("site_id") or 0)
        site_name = str(item.get("site_name") or "").strip()
        if site_id <= 0 and not site_name:
            continue
        out.append({"site_id": site_id, "site_name": site_name})
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair site identity consistency (site_registry orphan sites).")
    parser.add_argument("--limit", type=int, default=500, help="max issues scanned from consistency report")
    parser.add_argument("--dry-run", action="store_true", help="only print candidates; do not write DB")
    args = parser.parse_args()

    before = site_identity_consistency_report(limit=max(10, min(int(args.limit), 2000)))
    targets = _orphan_sites(before)
    repaired: List[Dict[str, Any]] = []

    if not args.dry_run:
        for item in targets:
            site_id = int(item.get("site_id") or 0)
            site_name = str(item.get("site_name") or "").strip()
            if not site_name and site_id > 0:
                site_name = str(find_site_name_by_id(site_id) or "").strip()
            if not site_name:
                continue
            code = resolve_or_create_site_code(site_name, allow_create=True)
            repaired.append({"site_id": site_id, "site_name": site_name, "site_code": code})

    after = site_identity_consistency_report(limit=max(10, min(int(args.limit), 2000)))
    result = {
        "dry_run": bool(args.dry_run),
        "before_issue_count": int(before.get("issue_count") or 0),
        "after_issue_count": int(after.get("issue_count") or 0),
        "target_count": len(targets),
        "repaired_count": len(repaired),
        "targets": targets,
        "repaired": repaired,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
