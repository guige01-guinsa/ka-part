import json
import os
import urllib.request
from typing import Optional

from app.db import db_conn


def _queue_notification(channel: str, recipient: str, payload: dict, error: Optional[str] = None) -> None:
    with db_conn() as db:
        db.execute(
            """
            INSERT INTO notification_queue(channel, recipient, payload_json, status, created_at, error)
            VALUES(?, ?, ?, ?, datetime('now'), ?)
            """,
            (
                channel,
                recipient,
                json.dumps(payload, ensure_ascii=False),
                "ERROR" if error else "PENDING",
                error,
            ),
        )
        db.commit()


def _post_json(url: str, payload: dict, headers: Optional[dict] = None) -> tuple[bool, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            _ = resp.read()
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _get_template(event_key: str) -> Optional[dict]:
    with db_conn() as db:
        t = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='notification_templates';"
        ).fetchone()
        if not t:
            return None
        row = db.execute(
            """
            SELECT event_key, template_code, enabled, message_format
            FROM notification_templates
            WHERE event_key=?
            """,
            (event_key,),
        ).fetchone()
    return dict(row) if row else None


def _get_recipient_phones(role_codes: list[str], vendor_id: Optional[int] = None) -> list[str]:
    if not role_codes:
        return []
    codes = [c.strip().upper() for c in role_codes if c]
    if not codes:
        return []
    with db_conn() as db:
        q = """
        SELECT DISTINCT u.phone
        FROM users u
        JOIN user_roles ur ON ur.user_id = u.id
        JOIN roles r ON r.id = ur.role_id
        WHERE upper(r.code) IN ({codes})
          AND u.is_active=1
          AND u.phone IS NOT NULL
          AND u.phone <> ''
        """
        q = q.format(codes=",".join(["?"] * len(codes)))
        params = list(codes)
        if vendor_id is not None:
            q += " AND u.vendor_id=?"
            params.append(int(vendor_id))
        rows = db.execute(q, tuple(params)).fetchall()
    return [r["phone"] for r in rows]


def _render_message(template: Optional[dict], title: str, message: str, work_id: int) -> tuple[str, Optional[str]]:
    if not template:
        return f"{title}\n{message}\n작업ID: {work_id}", None
    if int(template.get("enabled") or 0) != 1:
        return f"{title}\n{message}\n작업ID: {work_id}", None
    fmt = template.get("message_format") or "{title}\n{message}\n작업ID:{work_id}"
    try:
        text = fmt.format(title=title, message=message, work_id=work_id)
    except Exception:
        text = f"{title}\n{message}\n작업ID: {work_id}"
    return text, template.get("template_code")


def _send_kakao_bizmsg(recipient: str, template_code: str, message: str) -> tuple[bool, str]:
    api_url = os.getenv("KAKAO_API_URL", "").strip()
    auth_token = os.getenv("KAKAO_AUTH_TOKEN", "").strip()
    sender_key = os.getenv("KAKAO_SENDER_KEY", "").strip()
    if not api_url or not auth_token or not sender_key:
        return False, "KAKAO_API_URL / KAKAO_AUTH_TOKEN / KAKAO_SENDER_KEY missing"

    payload = {
        "sender_key": sender_key,
        "template_code": template_code,
        "recipient": recipient,
        "message": message,
    }
    headers = {"Authorization": f"Bearer {auth_token}"}
    return _post_json(api_url, payload, headers=headers)


def send_kakao_payload(payload: dict) -> tuple[bool, str]:
    """
    Resend helper for queued notifications.
    Payload should include: recipient, message, template_code(optional), event
    """
    webhook = os.getenv("KAKAO_WEBHOOK_URL", "").strip()
    if webhook:
        ok, err = _post_json(webhook, payload)
        return ok, err

    recipient = (payload.get("recipient") or "").strip()
    message = (payload.get("message") or "").strip()
    template_code = (payload.get("template_code") or "").strip()
    if not recipient or not message:
        return False, "payload missing recipient/message"
    if not template_code:
        return False, "payload missing template_code"
    return _send_kakao_bizmsg(recipient, template_code, message)

def notify_kakao_event(event: str, work_id: int, title: str, message: str, vendor_id: Optional[int] = None) -> None:
    """
    Kakao 알림톡 실제 연동:
    - KAKAO_API_URL / KAKAO_AUTH_TOKEN / KAKAO_SENDER_KEY 지정 시 BizMessage 방식 전송
    - KAKAO_WEBHOOK_URL 지정 시 웹훅 전송
    - 그 외에는 notification_queue 적재
    """
    event_key = (event or "").strip().upper()
    template = _get_template(event_key)
    text, template_code = _render_message(template, title, message, work_id)

    webhook = os.getenv("KAKAO_WEBHOOK_URL", "").strip()
    if webhook:
        payload = {"event": event_key, "work_id": work_id, "title": title, "message": text}
        ok, err = _post_json(webhook, payload)
        if not ok:
            _queue_notification("kakao", "", payload, error=err)
        return

    # 기본 수신자: 소장/시설과장/담당자 + (외주 지정 시) 외주업체
    phones = _get_recipient_phones(["CHIEF", "MANAGER", "FACILITY_MANAGER", "LEAD", "STAFF", "TECH"])
    if vendor_id:
        phones += _get_recipient_phones(["VENDOR"], vendor_id=vendor_id)
    phones = list(dict.fromkeys(phones))

    if not phones:
        payload = {"event": event_key, "work_id": work_id, "title": title, "message": text}
        _queue_notification("kakao", "", payload, error="no recipients")
        return

    for phone in phones:
        payload = {
            "event": event_key,
            "work_id": work_id,
            "title": title,
            "message": text,
            "template_code": template_code,
            "recipient": phone,
        }
        if template_code:
            ok, err = _send_kakao_bizmsg(phone, template_code, text)
            if not ok:
                _queue_notification("kakao", phone, payload, error=err)
        else:
            _queue_notification("kakao", phone, payload, error="template_code missing")
