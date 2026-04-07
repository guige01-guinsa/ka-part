from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

from .db import DB_PATH, now_iso

VOICE_SESSION_STATUS_VALUES = ("ringing", "in_progress", "completed", "handoff", "failed", "no_input")
VOICE_TURN_ROLE_VALUES = ("caller", "assistant", "system", "tool", "event")


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH), timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    try:
        con.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass
    return con


def _clean_text(value: Any, *, field: str, required: bool = False, max_len: int = 4000) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_len:
        raise ValueError(f"{field} length must be <= {max_len}")
    return text


def _clean_choice(value: Any, allowed: tuple[str, ...], *, field: str, default: str = "") -> str:
    text = str(value or "").strip() or default
    if text not in allowed:
        raise ValueError(f"{field} must be one of: {', '.join(allowed)}")
    return text


def _json_dump(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_load(value: Any, default: Any) -> Any:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS voice_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          provider TEXT NOT NULL,
          provider_call_id TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'ringing',
          stage TEXT NOT NULL DEFAULT 'ask_location',
          from_phone TEXT,
          to_phone TEXT,
          complainant_phone TEXT,
          building TEXT,
          unit TEXT,
          content TEXT,
          summary TEXT,
          complaint_id INTEGER REFERENCES complaints(id) ON DELETE SET NULL,
          handoff_reason TEXT,
          handoff_target TEXT,
          state_json TEXT,
          started_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          ended_at TEXT,
          UNIQUE(provider, provider_call_id)
        );

        CREATE TABLE IF NOT EXISTS voice_turns (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          voice_session_id INTEGER NOT NULL REFERENCES voice_sessions(id) ON DELETE CASCADE,
          role TEXT NOT NULL,
          text TEXT,
          meta_json TEXT,
          created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_voice_sessions_tenant_updated
          ON voice_sessions(tenant_id, updated_at DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_voice_sessions_provider_call
          ON voice_sessions(provider, provider_call_id);
        CREATE INDEX IF NOT EXISTS idx_voice_turns_session
          ON voice_turns(voice_session_id, id ASC);
        """
    )


def init_voice_db() -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        con.commit()
    finally:
        con.close()


def _session_detail(con: sqlite3.Connection, session_id: int) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, provider, provider_call_id, status, stage, from_phone, to_phone,
          complainant_phone, building, unit, content, summary, complaint_id,
          handoff_reason, handoff_target, state_json, started_at, updated_at, ended_at
        FROM voice_sessions
        WHERE id=?
        LIMIT 1
        """,
        (int(session_id),),
    ).fetchone()
    if not row:
        raise ValueError("voice session not found")
    item = dict(row)
    item["state"] = _json_load(item.pop("state_json", None), {})
    item["turns"] = [
        {
            **dict(turn),
            "meta": _json_load(turn["meta_json"], {}),
        }
        for turn in con.execute(
            """
            SELECT id, voice_session_id, role, text, meta_json, created_at
            FROM voice_turns
            WHERE voice_session_id=?
            ORDER BY id ASC
            """,
            (int(session_id),),
        ).fetchall()
    ]
    for turn in item["turns"]:
        turn.pop("meta_json", None)
    return item


def create_or_get_voice_session(
    *,
    tenant_id: str,
    provider: str,
    provider_call_id: str,
    from_phone: str = "",
    to_phone: str = "",
    state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_provider = _clean_text(provider, field="provider", required=True, max_len=40).lower()
    clean_call_id = _clean_text(provider_call_id, field="provider_call_id", required=True, max_len=160)
    clean_from_phone = _clean_text(from_phone, field="from_phone", max_len=40)
    clean_to_phone = _clean_text(to_phone, field="to_phone", max_len=40)
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT id
            FROM voice_sessions
            WHERE provider=? AND provider_call_id=?
            LIMIT 1
            """,
            (clean_provider, clean_call_id),
        ).fetchone()
        ts = now_iso()
        if row:
            con.execute(
                """
                UPDATE voice_sessions
                SET tenant_id=?, from_phone=COALESCE(NULLIF(?, ''), from_phone), to_phone=COALESCE(NULLIF(?, ''), to_phone), updated_at=?
                WHERE id=?
                """,
                (clean_tenant_id, clean_from_phone, clean_to_phone, ts, int(row["id"])),
            )
            con.commit()
            return _session_detail(con, int(row["id"]))

        cur = con.execute(
            """
            INSERT INTO voice_sessions(
              tenant_id, provider, provider_call_id, status, stage, from_phone, to_phone,
              complainant_phone, state_json, started_at, updated_at
            )
            VALUES(?,?,?,'ringing','ask_location',?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_provider,
                clean_call_id,
                clean_from_phone or None,
                clean_to_phone or None,
                clean_from_phone or None,
                _json_dump(state or {}),
                ts,
                ts,
            ),
        )
        con.commit()
        return _session_detail(con, int(cur.lastrowid))
    finally:
        con.close()


def get_voice_session(session_id: int) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        try:
            return _session_detail(con, int(session_id))
        except ValueError:
            return None
    finally:
        con.close()


def get_voice_session_by_provider_call(*, provider: str, provider_call_id: str) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT id
            FROM voice_sessions
            WHERE provider=? AND provider_call_id=?
            LIMIT 1
            """,
            (str(provider or "").strip().lower(), str(provider_call_id or "").strip()),
        ).fetchone()
        if not row:
            return None
        return _session_detail(con, int(row["id"]))
    finally:
        con.close()


def append_voice_turn(*, session_id: int, role: str, text: str = "", meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    clean_role = _clean_choice(role, VOICE_TURN_ROLE_VALUES, field="role")
    clean_text_value = _clean_text(text, field="text", max_len=8000)
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO voice_turns(voice_session_id, role, text, meta_json, created_at)
            VALUES(?,?,?,?,?)
            """,
            (
                int(session_id),
                clean_role,
                clean_text_value or None,
                _json_dump(meta or {}),
                ts,
            ),
        )
        con.execute("UPDATE voice_sessions SET updated_at=? WHERE id=?", (ts, int(session_id)))
        con.commit()
        row = con.execute(
            """
            SELECT id, voice_session_id, role, text, meta_json, created_at
            FROM voice_turns
            WHERE id=?
            LIMIT 1
            """,
            (int(cur.lastrowid),),
        ).fetchone()
        out = dict(row) if row else {}
        out["meta"] = _json_load(out.pop("meta_json", None), {})
        return out
    finally:
        con.close()


def update_voice_session(
    session_id: int,
    *,
    status: Optional[str] = None,
    stage: Optional[str] = None,
    from_phone: Optional[str] = None,
    to_phone: Optional[str] = None,
    complainant_phone: Optional[str] = None,
    building: Optional[str] = None,
    unit: Optional[str] = None,
    content: Optional[str] = None,
    summary: Optional[str] = None,
    complaint_id: Optional[int] = None,
    handoff_reason: Optional[str] = None,
    handoff_target: Optional[str] = None,
    state: Optional[Dict[str, Any]] = None,
    ended: bool = False,
) -> Dict[str, Any]:
    fields: List[str] = []
    params: List[Any] = []

    if status is not None:
        fields.append("status=?")
        params.append(_clean_choice(status, VOICE_SESSION_STATUS_VALUES, field="status"))
    if stage is not None:
        fields.append("stage=?")
        params.append(_clean_text(stage, field="stage", required=True, max_len=40))
    if from_phone is not None:
        fields.append("from_phone=?")
        params.append(_clean_text(from_phone, field="from_phone", max_len=40) or None)
    if to_phone is not None:
        fields.append("to_phone=?")
        params.append(_clean_text(to_phone, field="to_phone", max_len=40) or None)
    if complainant_phone is not None:
        fields.append("complainant_phone=?")
        params.append(_clean_text(complainant_phone, field="complainant_phone", max_len=40) or None)
    if building is not None:
        fields.append("building=?")
        params.append(_clean_text(building, field="building", max_len=20) or None)
    if unit is not None:
        fields.append("unit=?")
        params.append(_clean_text(unit, field="unit", max_len=20) or None)
    if content is not None:
        fields.append("content=?")
        params.append(_clean_text(content, field="content", max_len=8000) or None)
    if summary is not None:
        fields.append("summary=?")
        params.append(_clean_text(summary, field="summary", max_len=200) or None)
    if complaint_id is not None:
        fields.append("complaint_id=?")
        params.append(int(complaint_id))
    if handoff_reason is not None:
        fields.append("handoff_reason=?")
        params.append(_clean_text(handoff_reason, field="handoff_reason", max_len=200) or None)
    if handoff_target is not None:
        fields.append("handoff_target=?")
        params.append(_clean_text(handoff_target, field="handoff_target", max_len=80) or None)
    if state is not None:
        fields.append("state_json=?")
        params.append(_json_dump(state))
    fields.append("updated_at=?")
    params.append(now_iso())
    if ended:
        fields.append("ended_at=?")
        params.append(now_iso())

    con = _connect()
    try:
        _ensure_schema(con)
        params.append(int(session_id))
        con.execute(f"UPDATE voice_sessions SET {', '.join(fields)} WHERE id=?", tuple(params))
        con.commit()
        return _session_detail(con, int(session_id))
    finally:
        con.close()


def list_voice_sessions(*, tenant_id: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    clean_tenant_id = str(tenant_id or "").strip().lower()
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              id, tenant_id, provider, provider_call_id, status, stage, from_phone, to_phone,
              complainant_phone, building, unit, content, summary, complaint_id,
              handoff_reason, handoff_target, state_json, started_at, updated_at, ended_at
            FROM voice_sessions
        """
        params: List[Any] = []
        if clean_tenant_id:
            sql += " WHERE tenant_id=?"
            params.append(clean_tenant_id)
        sql += " ORDER BY updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(500, int(limit))))
        rows = con.execute(sql, tuple(params)).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["state"] = _json_load(item.pop("state_json", None), {})
            items.append(item)
        return items
    finally:
        con.close()
