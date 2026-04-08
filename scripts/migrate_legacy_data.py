from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import legacy facility admin data into ka-part.")
    parser.add_argument("--source", required=True, help="Legacy source path: JSON file, CSV directory, or SQLite file")
    parser.add_argument("--tenant-id", required=True, help="Target tenant id")
    parser.add_argument("--tenant-name", required=True, help="Target tenant name")
    parser.add_argument("--site-code", default="", help="Optional target site code")
    parser.add_argument("--site-name", default="", help="Optional target site name")
    parser.add_argument("--default-user-password", default="ChangeMe123!", help="Default password for imported users")
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate import without committing changes")
    return parser


def main() -> int:
    from app.db import init_db
    from app.engine_db import init_engine_db
    from app.legacy_import import import_legacy_source
    from app.ops_db import init_ops_db
    from app.voice_db import init_voice_db

    args = build_parser().parse_args()
    init_db()
    init_engine_db()
    init_ops_db()
    init_voice_db()
    summary = import_legacy_source(
        source_path=args.source,
        tenant_id=args.tenant_id,
        tenant_name=args.tenant_name,
        site_code=args.site_code,
        site_name=args.site_name,
        default_user_password=args.default_user_password,
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
