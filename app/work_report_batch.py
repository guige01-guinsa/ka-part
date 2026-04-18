from __future__ import annotations

import json
import shutil
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from .db import DB_PATH, STORAGE_ROOT, now_iso

WORK_REPORT_JOB_ROOT = (STORAGE_ROOT / "uploads" / "work_report_jobs").resolve()
WORK_REPORT_JOB_TOTAL_STEPS = 5
WORK_REPORT_JOB_POLL_AFTER_MS = 2000
WORK_REPORT_JOB_RETENTION_DAYS = 3
_JOB_STATUS_VALUES = {"queued", "running", "completed", "failed"}


def _connect() -> sqlite3.Connection:
    WORK_REPORT_JOB_ROOT.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH), timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    try:
        con.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass
    return con


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS work_report_jobs (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          mode TEXT NOT NULL DEFAULT 'preview',
          status TEXT NOT NULL DEFAULT 'queued',
          actor_label TEXT,
          job_dir TEXT NOT NULL,
          total_steps INTEGER NOT NULL DEFAULT 5,
          current_step INTEGER NOT NULL DEFAULT 0,
          summary TEXT,
          hint TEXT,
          source_file_count INTEGER NOT NULL DEFAULT 0,
          image_count INTEGER NOT NULL DEFAULT 0,
          reference_image_count INTEGER NOT NULL DEFAULT 0,
          attachment_count INTEGER NOT NULL DEFAULT 0,
          result_path TEXT,
          error_message TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_work_report_jobs_tenant_created
          ON work_report_jobs(tenant_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_work_report_jobs_tenant_status
          ON work_report_jobs(tenant_id, status, created_at DESC);
        """
    )


def _parse_iso(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _safe_job_dir(path: Path) -> Path:
    resolved = path.resolve()
    if not str(resolved).startswith(str(WORK_REPORT_JOB_ROOT)):
        raise ValueError("invalid work report job path")
    return resolved


def build_work_report_job_dir(tenant_id: str, job_id: str) -> Path:
    tenant_key = str(tenant_id or "").strip().lower() or "tenant"
    return _safe_job_dir(WORK_REPORT_JOB_ROOT / tenant_key / str(job_id or "").strip())


def new_work_report_job_id() -> str:
    return uuid.uuid4().hex


def _cleanup_old_jobs(con: sqlite3.Connection) -> None:
    threshold = datetime.now() - timedelta(days=WORK_REPORT_JOB_RETENTION_DAYS)
    rows = con.execute(
        """
        SELECT id, job_dir, created_at
        FROM work_report_jobs
        ORDER BY created_at ASC
        """
    ).fetchall()
    delete_ids: list[str] = []
    for row in rows:
        created_at = _parse_iso(row["created_at"])
        if not created_at or created_at >= threshold:
            continue
        delete_ids.append(str(row["id"]))
        job_dir = str(row["job_dir"] or "").strip()
        if job_dir:
            try:
                shutil.rmtree(_safe_job_dir(Path(job_dir)), ignore_errors=True)
            except Exception:
                pass
    for job_id in delete_ids:
        con.execute("DELETE FROM work_report_jobs WHERE id=?", (job_id,))


def _cleanup_job_dir_contents(job_dir: Path, *, keep_paths: set[Path] | None = None) -> None:
    target_dir = _safe_job_dir(job_dir)
    keep = {path.resolve() for path in (keep_paths or set())}
    if not target_dir.exists():
        return
    for child in target_dir.iterdir():
        resolved_child = child.resolve()
        if resolved_child in keep:
            continue
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            try:
                child.unlink(missing_ok=True)
            except Exception:
                pass


def _cleanup_finished_job_artifacts_for_record(record: Dict[str, Any]) -> None:
    status = str(record.get("status") or "").strip().lower()
    if status not in {"completed", "failed"}:
        return
    raw_job_dir = str(record.get("job_dir") or "").strip()
    if not raw_job_dir:
        return
    try:
        job_dir = _safe_job_dir(Path(raw_job_dir))
    except Exception:
        return
    if status == "failed":
        shutil.rmtree(job_dir, ignore_errors=True)
        return
    keep_paths: set[Path] = set()
    raw_result_path = str(record.get("result_path") or "").strip()
    if raw_result_path:
        try:
            keep_paths.add(_safe_job_dir(Path(raw_result_path)))
        except Exception:
            pass
    _cleanup_job_dir_contents(job_dir, keep_paths=keep_paths)


def _cleanup_finished_job_artifacts(con: sqlite3.Connection) -> None:
    rows = con.execute(
        """
        SELECT id, status, job_dir, result_path
        FROM work_report_jobs
        WHERE status IN ('completed', 'failed')
        """
    ).fetchall()
    for row in rows:
        try:
            _cleanup_finished_job_artifacts_for_record(dict(row))
        except Exception:
            pass


def _mark_interrupted_jobs_failed(con: sqlite3.Connection) -> None:
    ts = now_iso()
    con.execute(
        """
        UPDATE work_report_jobs
        SET status='failed',
            summary=?,
            hint=?,
            error_message=?,
            updated_at=?,
            finished_at=?
        WHERE status IN ('queued','running')
        """,
        (
            "서버 재시작으로 배치 작업이 중단되었습니다.",
            "같은 입력으로 미리보기를 다시 실행해 주세요.",
            "서버 재시작으로 배치 작업이 중단되었습니다.",
            ts,
            ts,
        ),
    )


def init_work_report_batch() -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        _cleanup_old_jobs(con)
        _mark_interrupted_jobs_failed(con)
        _cleanup_finished_job_artifacts(con)
        con.commit()
    finally:
        con.close()


def reclaim_work_report_job_storage() -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        _cleanup_old_jobs(con)
        _cleanup_finished_job_artifacts(con)
        con.commit()
    finally:
        con.close()


def create_work_report_job(
    *,
    job_id: str,
    tenant_id: str,
    actor_label: str,
    job_dir: Path,
    source_file_count: int = 0,
    image_count: int = 0,
    reference_image_count: int = 0,
    attachment_count: int = 0,
) -> Dict[str, Any]:
    clean_job_id = str(job_id or "").strip() or new_work_report_job_id()
    target_dir = _safe_job_dir(job_dir)
    ts = now_iso()
    con = _connect()
    try:
        _ensure_schema(con)
        con.execute(
            """
            INSERT INTO work_report_jobs(
                id, tenant_id, mode, status, actor_label, job_dir, total_steps, current_step,
              summary, hint, source_file_count, image_count, reference_image_count, attachment_count,
              created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_job_id,
                str(tenant_id or "").strip().lower(),
                "preview",
                "queued",
                str(actor_label or "").strip()[:120] or None,
                str(target_dir),
                WORK_REPORT_JOB_TOTAL_STEPS,
                0,
                "배치 작업을 등록했습니다.",
                "서버가 업로드된 원문과 사진을 순차적으로 분석합니다.",
                max(0, int(source_file_count)),
                max(0, int(image_count)),
                max(0, int(reference_image_count)),
                max(0, int(attachment_count)),
                ts,
                ts,
            ),
        )
        con.commit()
    finally:
        con.close()
    return get_work_report_job(clean_job_id) or {}


def _update_job(
    job_id: str,
    *,
    status: str | None = None,
    current_step: int | None = None,
    total_steps: int | None = None,
    summary: str | None = None,
    hint: str | None = None,
    error_message: str | None = None,
    result_path: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    updates: list[str] = ["updated_at=?"]
    params: list[Any] = [now_iso()]
    if status is not None:
        clean_status = str(status or "").strip().lower()
        if clean_status not in _JOB_STATUS_VALUES:
            raise ValueError("invalid work report job status")
        updates.append("status=?")
        params.append(clean_status)
    if current_step is not None:
        updates.append("current_step=?")
        params.append(max(0, int(current_step)))
    if total_steps is not None:
        updates.append("total_steps=?")
        params.append(max(1, int(total_steps)))
    if summary is not None:
        updates.append("summary=?")
        params.append(str(summary or "").strip() or None)
    if hint is not None:
        updates.append("hint=?")
        params.append(str(hint or "").strip() or None)
    if error_message is not None:
        updates.append("error_message=?")
        params.append(str(error_message or "").strip() or None)
    if result_path is not None:
        updates.append("result_path=?")
        params.append(str(result_path or "").strip() or None)
    if started_at is not None:
        updates.append("started_at=?")
        params.append(str(started_at or "").strip() or None)
    if finished_at is not None:
        updates.append("finished_at=?")
        params.append(str(finished_at or "").strip() or None)
    params.append(str(job_id or "").strip())

    con = _connect()
    try:
        _ensure_schema(con)
        con.execute(f"UPDATE work_report_jobs SET {', '.join(updates)} WHERE id=?", params)
        con.commit()
    finally:
        con.close()


def mark_work_report_job_running(
    job_id: str,
    *,
    current_step: int = 0,
    total_steps: int = WORK_REPORT_JOB_TOTAL_STEPS,
    summary: str = "배치 분석을 시작했습니다.",
    hint: str = "",
) -> None:
    _update_job(
        job_id,
        status="running",
        current_step=current_step,
        total_steps=total_steps,
        summary=summary,
        hint=hint,
        started_at=now_iso(),
        error_message="",
    )


def update_work_report_job_progress(
    job_id: str,
    *,
    current_step: int,
    total_steps: int = WORK_REPORT_JOB_TOTAL_STEPS,
    summary: str = "",
    hint: str = "",
) -> None:
    _update_job(
        job_id,
        status="running",
        current_step=current_step,
        total_steps=total_steps,
        summary=summary,
        hint=hint,
    )


def complete_work_report_job(
    job_id: str,
    *,
    result: Dict[str, Any],
    summary: str = "",
    hint: str = "미리보기에서 출력할 사진을 고른 뒤 PDF를 생성해 주세요.",
) -> None:
    record = get_work_report_job_record(job_id)
    if not record:
        raise ValueError("work report job not found")
    job_dir = _safe_job_dir(Path(str(record.get("job_dir") or "")))
    result_path = _safe_job_dir(job_dir / "result.json")
    result_path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    _update_job(
        job_id,
        status="completed",
        current_step=max(0, int(record.get("total_steps") or WORK_REPORT_JOB_TOTAL_STEPS) - 1),
        summary=summary or f"미리보기 작업 {int(result.get('item_count') or 0)}건 정리를 마쳤습니다.",
        hint=hint,
        result_path=str(result_path),
        finished_at=now_iso(),
        error_message="",
    )
    cleanup_record = dict(record)
    cleanup_record.update({"status": "completed", "result_path": str(result_path)})
    _cleanup_finished_job_artifacts_for_record(cleanup_record)


def fail_work_report_job(
    job_id: str,
    *,
    error_message: str,
    summary: str = "미리보기 배치 작업이 실패했습니다.",
    hint: str = "같은 입력으로 다시 시도해 주세요.",
) -> None:
    record = get_work_report_job_record(job_id)
    _update_job(
        job_id,
        status="failed",
        summary=summary,
        hint=hint,
        error_message=str(error_message or "").strip() or "미리보기 배치 작업이 실패했습니다.",
        finished_at=now_iso(),
    )
    if record:
        cleanup_record = dict(record)
        cleanup_record["status"] = "failed"
        _cleanup_finished_job_artifacts_for_record(cleanup_record)


def get_work_report_job_record(job_id: str) -> Dict[str, Any] | None:
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT
              id, tenant_id, mode, status, actor_label, job_dir, total_steps, current_step,
              summary, hint, source_file_count, image_count, reference_image_count, attachment_count,
              result_path, error_message, created_at, updated_at, started_at, finished_at
            FROM work_report_jobs
            WHERE id=?
            LIMIT 1
            """,
            (str(job_id or "").strip(),),
        ).fetchone()
    finally:
        con.close()
    return dict(row) if row else None


def _job_elapsed_seconds(record: Dict[str, Any]) -> int:
    started_at = _parse_iso(record.get("started_at") or record.get("created_at"))
    finished_at = _parse_iso(record.get("finished_at")) or datetime.now()
    if not started_at:
        return 0
    return max(0, int((finished_at - started_at).total_seconds()))


def _read_job_result(result_path: str) -> Dict[str, Any] | None:
    raw = str(result_path or "").strip()
    if not raw:
        return None
    path = _safe_job_dir(Path(raw))
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def get_work_report_job(job_id: str, *, include_result: bool = False) -> Dict[str, Any] | None:
    record = get_work_report_job_record(job_id)
    if not record:
        return None
    item = {
        "id": str(record.get("id") or ""),
        "tenant_id": str(record.get("tenant_id") or ""),
        "mode": str(record.get("mode") or "preview"),
        "status": str(record.get("status") or "queued"),
        "actor_label": str(record.get("actor_label") or ""),
        "total_steps": max(1, int(record.get("total_steps") or WORK_REPORT_JOB_TOTAL_STEPS)),
        "current_step": max(0, int(record.get("current_step") or 0)),
        "summary": str(record.get("summary") or "").strip(),
        "hint": str(record.get("hint") or "").strip(),
        "source_file_count": max(0, int(record.get("source_file_count") or 0)),
        "image_count": max(0, int(record.get("image_count") or 0)),
        "reference_image_count": max(0, int(record.get("reference_image_count") or 0)),
        "attachment_count": max(0, int(record.get("attachment_count") or 0)),
        "error_message": str(record.get("error_message") or "").strip(),
        "created_at": str(record.get("created_at") or ""),
        "updated_at": str(record.get("updated_at") or ""),
        "started_at": str(record.get("started_at") or ""),
        "finished_at": str(record.get("finished_at") or ""),
        "elapsed_sec": _job_elapsed_seconds(record),
        "poll_after_ms": WORK_REPORT_JOB_POLL_AFTER_MS if str(record.get("status") or "") in {"queued", "running"} else 0,
    }
    if include_result:
        item["result"] = _read_job_result(str(record.get("result_path") or ""))
    return item
