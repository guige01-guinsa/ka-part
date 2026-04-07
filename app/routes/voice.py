from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import Response

from ..db import append_audit_log, ensure_service_user, get_tenant, log_usage
from ..engine_db import create_complaint
from ..voice_db import (
    append_voice_turn,
    create_or_get_voice_session,
    get_voice_session,
    get_voice_session_by_provider_call,
    list_voice_sessions,
    update_voice_session,
)
from ..voice_service import advance_voice_flow, complaint_payload_from_state, default_voice_state, handoff_number
from .core import _require_auth, _require_user_manager

router = APIRouter()
PUBLIC_BASE_URL = (
    str(os.getenv("KA_PUBLIC_BASE_URL") or os.getenv("KA_PART_PUBLIC_BASE_URL") or "").strip().rstrip("/")
)
VOICE_SAY_LANGUAGE = str(os.getenv("KA_VOICE_SAY_LANGUAGE") or "ko-KR").strip() or "ko-KR"
VOICE_GATHER_LANGUAGE = str(os.getenv("KA_VOICE_GATHER_LANGUAGE") or VOICE_SAY_LANGUAGE).strip() or VOICE_SAY_LANGUAGE
VOICE_DEFAULT_TENANT_ID = str(os.getenv("KA_VOICE_DEFAULT_TENANT_ID") or "").strip().lower()


def _xml_response(root: ET.Element) -> Response:
    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return Response(content=xml, media_type="application/xml")


def _public_url(request: Request, path: str, *, query: Optional[Dict[str, Any]] = None) -> str:
    base = PUBLIC_BASE_URL or str(request.base_url).rstrip("/")
    url = f"{base}{path}"
    filtered = {key: value for key, value in (query or {}).items() if str(value or "").strip()}
    if filtered:
        url = f"{url}?{urlencode(filtered)}"
    return url


def _twiml_gather(message: str, action_url: str) -> Response:
    root = ET.Element("Response")
    gather = ET.SubElement(
        root,
        "Gather",
        {
            "input": "speech dtmf",
            "action": action_url,
            "method": "POST",
            "language": VOICE_GATHER_LANGUAGE,
            "speechTimeout": "auto",
            "timeout": "4",
            "numDigits": "1",
        },
    )
    ET.SubElement(gather, "Say", {"language": VOICE_SAY_LANGUAGE}).text = message
    ET.SubElement(root, "Redirect", {"method": "POST"}).text = action_url
    return _xml_response(root)


def _twiml_say_hangup(message: str) -> Response:
    root = ET.Element("Response")
    ET.SubElement(root, "Say", {"language": VOICE_SAY_LANGUAGE}).text = message
    ET.SubElement(root, "Hangup")
    return _xml_response(root)


def _twiml_handoff(message: str, target_number: str) -> Response:
    root = ET.Element("Response")
    ET.SubElement(root, "Say", {"language": VOICE_SAY_LANGUAGE}).text = message
    if str(target_number or "").strip():
        ET.SubElement(root, "Dial").text = str(target_number or "").strip()
    else:
        ET.SubElement(root, "Hangup")
    return _xml_response(root)


def _resolve_tenant_id(request: Request, tenant_id: str = "") -> str:
    resolved = str(tenant_id or request.query_params.get("tenant_id") or VOICE_DEFAULT_TENANT_ID or "").strip().lower()
    if not resolved:
        raise HTTPException(status_code=400, detail="tenant_id가 필요합니다.")
    tenant = get_tenant(resolved)
    if not tenant:
        raise HTTPException(status_code=404, detail="tenant not found")
    return resolved


def _ensure_voice_session(*, tenant_id: str, call_sid: str, from_phone: str, to_phone: str) -> Dict[str, Any]:
    session = get_voice_session_by_provider_call(provider="twilio", provider_call_id=call_sid)
    if session:
        return session
    return create_or_get_voice_session(
        tenant_id=tenant_id,
        provider="twilio",
        provider_call_id=call_sid,
        from_phone=from_phone,
        to_phone=to_phone,
        state=default_voice_state(from_phone=from_phone),
    )


def _persist_created_complaint(*, tenant_id: str, session: Dict[str, Any]) -> Dict[str, Any] | None:
    payload = complaint_payload_from_state(tenant_id=tenant_id, state=session.get("state") or {})
    if not payload:
        return None
    actor = ensure_service_user(tenant_id)
    item = create_complaint(
        tenant_id=tenant_id,
        building=str(payload.get("building") or ""),
        unit=str(payload.get("unit") or ""),
        complainant_phone=str(payload.get("complainant_phone") or ""),
        channel="전화",
        content=str(payload.get("content") or ""),
        summary=str(payload.get("summary") or ""),
        complaint_type=str(payload.get("type") or "기타"),
        urgency=str(payload.get("urgency") or "일반"),
        status="접수",
        manager="",
        image_url="",
        source_text="voice_ai",
        ai_model="voice-intake",
        created_by_user_id=int(actor.get("id")) if actor and actor.get("id") else None,
        created_by_label="전화 AI",
    )
    update_voice_session(
        int(session["id"]),
        complaint_id=int(item["id"]),
        building=str(item.get("building") or ""),
        unit=str(item.get("unit") or ""),
        complainant_phone=str(item.get("complainant_phone") or ""),
        content=str(item.get("content") or ""),
        summary=str(item.get("summary") or ""),
    )
    return item


@router.post("/voice/twilio/inbound")
def voice_twilio_inbound(
    request: Request,
    tenant_id: str = Query(default=""),
    call_sid: str = Form(default=""),
    from_phone: str = Form(default="", alias="From"),
    to_phone: str = Form(default="", alias="To"),
    call_sid_alias: str = Form(default="", alias="CallSid"),
) -> Response:
    resolved_tenant_id = _resolve_tenant_id(request, tenant_id)
    sid = str(call_sid or call_sid_alias or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="CallSid is required")
    session = _ensure_voice_session(tenant_id=resolved_tenant_id, call_sid=sid, from_phone=from_phone, to_phone=to_phone)
    intro = "안녕하세요. 아파트 관리사무소 AI 민원 접수입니다. 먼저 동과 호수를 말씀해 주세요. 예를 들면 101동 1203호입니다."
    append_voice_turn(session_id=int(session["id"]), role="assistant", text=intro, meta={"event": "inbound"})
    action_url = _public_url(request, "/api/voice/twilio/gather", query={"tenant_id": resolved_tenant_id, "call_sid": sid})
    return _twiml_gather(intro, action_url)


@router.post("/voice/twilio/gather")
def voice_twilio_gather(
    request: Request,
    tenant_id: str = Query(default=""),
    call_sid: str = Query(default=""),
    speech_result: str = Form(default="", alias="SpeechResult"),
    digits: str = Form(default="", alias="Digits"),
    call_sid_form: str = Form(default="", alias="CallSid"),
    from_phone: str = Form(default="", alias="From"),
    to_phone: str = Form(default="", alias="To"),
) -> Response:
    resolved_tenant_id = _resolve_tenant_id(request, tenant_id)
    sid = str(call_sid or call_sid_form or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="CallSid is required")
    session = _ensure_voice_session(tenant_id=resolved_tenant_id, call_sid=sid, from_phone=from_phone, to_phone=to_phone)

    caller_text = str(speech_result or "").strip()
    if caller_text or str(digits or "").strip():
        append_voice_turn(
            session_id=int(session["id"]),
            role="caller",
            text=caller_text or f"[digits:{digits}]",
            meta={"digits": str(digits or "").strip()},
        )

    result = advance_voice_flow(state=session.get("state") or {}, utterance=caller_text, digits=str(digits or "").strip())
    session = update_voice_session(
        int(session["id"]),
        status=str(result.get("status") or "in_progress"),
        stage=str((result.get("state") or {}).get("stage") or "ask_location"),
        building=str((result.get("state") or {}).get("building") or ""),
        unit=str((result.get("state") or {}).get("unit") or ""),
        complainant_phone=str((result.get("state") or {}).get("complainant_phone") or ""),
        content=str((result.get("state") or {}).get("content") or ""),
        handoff_reason=str((result.get("state") or {}).get("handoff_reason") or ""),
        state=result.get("state") or {},
        ended=str(result.get("action") or "") in {"complete", "create_complaint", "handoff"},
    )

    complaint_item = None
    if str(result.get("action") or "") in {"create_complaint", "handoff"} and not session.get("complaint_id"):
        complaint_item = _persist_created_complaint(tenant_id=resolved_tenant_id, session=session)
        session = get_voice_session(int(session["id"])) or session
        if complaint_item:
            log_usage(resolved_tenant_id, "voice.complaints.create")
            append_audit_log(
                resolved_tenant_id,
                "voice_ai_create_complaint",
                "전화 AI",
                {"call_sid": sid, "complaint_id": int(complaint_item["id"])},
            )

    assistant_message = str(result.get("assistant_message") or "").strip() or "말씀을 다시 부탁드립니다."
    if complaint_item and str(result.get("action") or "") == "create_complaint":
        assistant_message = f"{assistant_message} 접수번호는 {complaint_item['id']}번입니다. 담당자가 확인 후 연락드리겠습니다."
    elif complaint_item and str(result.get("action") or "") == "handoff":
        assistant_message = f"{assistant_message} 접수번호는 {complaint_item['id']}번입니다."

    append_voice_turn(session_id=int(session["id"]), role="assistant", text=assistant_message, meta={"action": str(result.get("action") or "")})
    log_usage(resolved_tenant_id, "voice.twilio.gather")

    if str(result.get("action") or "") == "handoff":
        return _twiml_handoff(assistant_message, handoff_number())
    if str(result.get("action") or "") in {"complete", "create_complaint"}:
        return _twiml_say_hangup(assistant_message)

    action_url = _public_url(request, "/api/voice/twilio/gather", query={"tenant_id": resolved_tenant_id, "call_sid": sid})
    return _twiml_gather(assistant_message, action_url)


@router.post("/voice/twilio/status")
def voice_twilio_status(
    request: Request,
    tenant_id: str = Query(default=""),
    call_status: str = Form(default="", alias="CallStatus"),
    call_sid: str = Form(default="", alias="CallSid"),
) -> Dict[str, Any]:
    resolved_tenant_id = _resolve_tenant_id(request, tenant_id)
    sid = str(call_sid or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="CallSid is required")
    session = _ensure_voice_session(tenant_id=resolved_tenant_id, call_sid=sid, from_phone="", to_phone="")
    next_status = "completed"
    lowered = str(call_status or "").strip().lower()
    if lowered in {"busy", "failed", "no-answer", "canceled"}:
        next_status = "failed"
    session = update_voice_session(int(session["id"]), status=next_status, ended=True)
    append_voice_turn(session_id=int(session["id"]), role="event", text=lowered or "completed", meta={"event": "status_callback"})
    log_usage(resolved_tenant_id, "voice.twilio.status")
    return {"ok": True, "item": session}


@router.get("/voice/config")
def voice_config(request: Request, tenant_id: str = Query(default="")) -> Dict[str, Any]:
    user, _token = _require_user_manager(request)
    managed_tenant_id = tenant_id if int(user.get("is_admin") or 0) == 1 else str(user.get("tenant_id") or "")
    resolved_tenant_id = _resolve_tenant_id(request, managed_tenant_id)
    return {
        "ok": True,
        "item": {
            "tenant_id": resolved_tenant_id,
            "provider": "twilio_webhook",
            "inbound_url": _public_url(request, "/api/voice/twilio/inbound", query={"tenant_id": resolved_tenant_id}),
            "status_callback_url": _public_url(request, "/api/voice/twilio/status", query={"tenant_id": resolved_tenant_id}),
            "handoff_number_configured": bool(handoff_number()),
            "say_language": VOICE_SAY_LANGUAGE,
            "gather_language": VOICE_GATHER_LANGUAGE,
        },
    }


@router.get("/voice/sessions")
def voice_sessions(request: Request, tenant_id: str = Query(default=""), limit: int = Query(default=50, ge=1, le=200)) -> Dict[str, Any]:
    user, _token = _require_auth(request)
    resolved_tenant_id = _resolve_tenant_id(request, tenant_id) if int(user.get("is_admin") or 0) == 1 else str(user.get("tenant_id") or "").strip().lower()
    if not resolved_tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id가 필요합니다.")
    return {"ok": True, "items": list_voice_sessions(tenant_id=resolved_tenant_id, limit=int(limit))}


@router.get("/voice/sessions/{session_id}")
def voice_session_detail(request: Request, session_id: int) -> Dict[str, Any]:
    user, _token = _require_auth(request)
    item = get_voice_session(int(session_id))
    if not item:
        raise HTTPException(status_code=404, detail="voice session not found")
    if int(user.get("is_admin") or 0) != 1 and str(item.get("tenant_id") or "").strip().lower() != str(user.get("tenant_id") or "").strip().lower():
        raise HTTPException(status_code=403, detail="다른 테넌트 통화기록은 볼 수 없습니다.")
    return {"ok": True, "item": item}
