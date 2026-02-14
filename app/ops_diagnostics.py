from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from .db import list_security_audit_logs, schema_alignment_report, write_security_audit_log

logger = logging.getLogger("ka-part.ops-diagnostics")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
STATE_PATH = DATA_DIR / "ops_diagnostics_state.json"

_SCHED_THREAD: threading.Thread | None = None
_SCHED_STOP_EVENT = threading.Event()
_STATE_LOCK = threading.Lock()


def _safe_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except Exception:
        value = int(default)
    return max(minimum, min(maximum, value))


def _scheduler_enabled() -> bool:
    raw = str(os.getenv("KA_OPS_DIAGNOSTICS_ENABLED", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _interval_sec() -> int:
    return _safe_int_env("KA_OPS_DIAGNOSTICS_INTERVAL_SEC", 300, 30, 86400)


def _lookback_minutes() -> int:
    return _safe_int_env("KA_OPS_DIAGNOSTICS_LOOKBACK_MIN", 60, 5, 1440)


def _error_threshold() -> int:
    return _safe_int_env("KA_OPS_DIAGNOSTICS_ERROR_THRESHOLD", 1, 1, 500)


def _audit_limit() -> int:
    return _safe_int_env("KA_OPS_DIAGNOSTICS_AUDIT_LIMIT", 200, 20, 500)


def _default_state() -> Dict[str, Any]:
    return {
        "enabled": _scheduler_enabled(),
        "interval_sec": _interval_sec(),
        "lookback_minutes": _lookback_minutes(),
        "error_threshold": _error_threshold(),
        "audit_limit": _audit_limit(),
        "last_run_at": "",
        "last_ok": None,
        "schema_alignment_ok": None,
        "schema_issue_count": 0,
        "schema_issue_preview": [],
        "recent_error_count": 0,
        "recent_error_preview": [],
        "alerts": [],
        "run_count": 0,
        "last_fingerprint": "",
    }


def _load_state() -> Dict[str, Any]:
    state = _default_state()
    if not STATE_PATH.exists():
        return state
    try:
        loaded = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            state.update(loaded)
    except Exception:
        logger.exception("Failed to load ops diagnostics state: %s", STATE_PATH)
    state["enabled"] = _scheduler_enabled()
    state["interval_sec"] = _interval_sec()
    state["lookback_minutes"] = _lookback_minutes()
    state["error_threshold"] = _error_threshold()
    state["audit_limit"] = _audit_limit()
    return state


def _save_state(state: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = _default_state()
    payload.update(state or {})
    payload["enabled"] = _scheduler_enabled()
    payload["interval_sec"] = _interval_sec()
    payload["lookback_minutes"] = _lookback_minutes()
    payload["error_threshold"] = _error_threshold()
    payload["audit_limit"] = _audit_limit()
    STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_created_at(value: Any) -> datetime | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00"))
    except Exception:
        return None


def _collect_recent_error_logs(*, lookback_minutes: int, limit: int) -> List[Dict[str, Any]]:
    items = list_security_audit_logs(limit=limit, outcome="error")
    if lookback_minutes <= 0:
        return items
    cutoff_epoch = time.time() - (float(lookback_minutes) * 60.0)
    out: List[Dict[str, Any]] = []
    for item in items:
        dt = _parse_created_at(item.get("created_at"))
        if dt is None:
            out.append(item)
            continue
        try:
            if dt.timestamp() >= cutoff_epoch:
                out.append(item)
        except Exception:
            out.append(item)
    return out


def _fingerprint(payload: Dict[str, Any]) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def run_ops_diagnostics(now: datetime | None = None) -> Dict[str, Any]:
    current = now or datetime.now()
    lookback = _lookback_minutes()
    threshold = _error_threshold()
    audit_limit = _audit_limit()

    schema = schema_alignment_report()
    schema_ok = bool(schema.get("ok"))
    issues = schema.get("issues") if isinstance(schema.get("issues"), list) else []
    issue_count = int(schema.get("issue_count") or len(issues))

    recent_errors = _collect_recent_error_logs(lookback_minutes=lookback, limit=audit_limit)
    recent_error_count = len(recent_errors)

    alerts: List[str] = []
    if not schema_ok:
        alerts.append(f"schema_alignment failed ({issue_count} issues)")
    if recent_error_count >= threshold:
        alerts.append(f"security audit errors in last {lookback}m: {recent_error_count}")

    ok = len(alerts) == 0
    schema_issue_preview = [str(x) for x in issues[:20]]
    recent_error_preview = [
        {
            "id": int(item.get("id") or 0),
            "event_type": str(item.get("event_type") or ""),
            "severity": str(item.get("severity") or ""),
            "created_at": str(item.get("created_at") or ""),
        }
        for item in recent_errors[:20]
    ]
    fingerprint = _fingerprint(
        {
            "ok": ok,
            "issue_count": issue_count,
            "recent_error_ids": [x.get("id") for x in recent_error_preview],
            "alerts": alerts,
        }
    )

    with _STATE_LOCK:
        prev = _load_state()
        next_state = dict(prev)
        next_state.update(
            {
                "enabled": _scheduler_enabled(),
                "interval_sec": _interval_sec(),
                "lookback_minutes": lookback,
                "error_threshold": threshold,
                "audit_limit": audit_limit,
                "last_run_at": current.replace(microsecond=0).isoformat(sep=" "),
                "last_ok": ok,
                "schema_alignment_ok": schema_ok,
                "schema_issue_count": issue_count,
                "schema_issue_preview": schema_issue_preview,
                "recent_error_count": recent_error_count,
                "recent_error_preview": recent_error_preview,
                "alerts": alerts,
                "run_count": int(prev.get("run_count") or 0) + 1,
                "last_fingerprint": fingerprint,
            }
        )
        _save_state(next_state)

    if str(prev.get("last_fingerprint") or "") != fingerprint:
        try:
            write_security_audit_log(
                event_type="ops_diagnostics",
                severity=("INFO" if ok else "WARN"),
                outcome=("ok" if ok else "error"),
                actor_login="ops-diagnostics",
                detail={
                    "alerts": alerts,
                    "schema_issue_count": issue_count,
                    "recent_error_count": recent_error_count,
                    "lookback_minutes": lookback,
                },
            )
        except Exception:
            logger.exception("Failed to write ops diagnostics audit log")

    if ok:
        logger.info("Ops diagnostics ok: schema=%s, recent_errors=%s", schema_ok, recent_error_count)
    else:
        logger.warning("Ops diagnostics alert: %s", "; ".join(alerts))
    return next_state


def get_ops_diagnostics_status() -> Dict[str, Any]:
    with _STATE_LOCK:
        state = _load_state()
    return state


def _scheduler_loop() -> None:
    logger.info("Ops diagnostics scheduler started")
    while not _SCHED_STOP_EVENT.is_set():
        try:
            run_ops_diagnostics()
        except Exception:
            logger.exception("Ops diagnostics tick failed")
        wait_sec = _interval_sec()
        if _SCHED_STOP_EVENT.wait(wait_sec):
            break
    logger.info("Ops diagnostics scheduler stopped")


def start_ops_diagnostics_scheduler() -> None:
    global _SCHED_THREAD
    if not _scheduler_enabled():
        logger.info("Ops diagnostics scheduler disabled by KA_OPS_DIAGNOSTICS_ENABLED")
        return
    if _SCHED_THREAD is not None and _SCHED_THREAD.is_alive():
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    _SCHED_STOP_EVENT.clear()
    _SCHED_THREAD = threading.Thread(target=_scheduler_loop, name="ka-ops-diagnostics", daemon=True)
    _SCHED_THREAD.start()


def stop_ops_diagnostics_scheduler() -> None:
    global _SCHED_THREAD
    if _SCHED_THREAD is None:
        return
    _SCHED_STOP_EVENT.set()
    _SCHED_THREAD.join(timeout=5)
    _SCHED_THREAD = None
