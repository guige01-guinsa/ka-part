#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
STORAGE_ROOT="$ROOT_DIR/runtime/test"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "virtual environment is missing. Run ./scripts/dev-setup.sh first." >&2
  exit 1
fi

mkdir -p "$STORAGE_ROOT/data" "$STORAGE_ROOT/uploads"

export ALLOW_INSECURE_DEFAULTS=1
export KA_HSTS_ENABLED=0
export KA_STORAGE_ROOT="$STORAGE_ROOT"

"$PYTHON_BIN" -m compileall app
"$PYTHON_BIN" -m ruff check app tests
"$PYTHON_BIN" -m pytest -q tests/test_engine_routes.py
