from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional

from .ai_service import classify_complaint_text, normalize_summary_text

VOICE_EMERGENCY_KEYWORDS = (
    "갇",
    "정전",
    "화재",
    "불이 났",
    "불났",
    "불이야",
    "연기",
    "누수",
    "물이 새",
    "물이 샌",
    "물이 쏟아",
    "물이 터",
)
VOICE_HANDOFF_KEYWORDS = ("상담원", "직원", "관리실", "사람", "연결", "긴급")
VOICE_CONFIRM_YES = ("예", "네", "맞", "맞아요", "맞습니다", "그렇습니다", "1")
VOICE_CONFIRM_NO = ("아니", "아니요", "틀렸", "다시", "수정", "2")
VOICE_MAX_RETRIES = max(1, min(3, int(os.getenv("KA_VOICE_MAX_RETRIES") or "2")))


def collapse_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u0000", " ")).strip()


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if digits.startswith("82") and len(digits) >= 11:
        digits = "0" + digits[2:]
    if digits.startswith("8210") and len(digits) == 12:
        digits = "0" + digits[2:]
    return digits[:11] if digits else ""


def speech_phone(value: Any) -> str:
    digits = normalize_phone(value)
    return " ".join(digits) if digits else ""


def handoff_number() -> str:
    return str(os.getenv("KA_VOICE_HANDOFF_NUMBER") or "").strip()


def extract_building_unit(text: str) -> tuple[str, str]:
    normalized = collapse_space(text)
    building = ""
    unit = ""
    building_match = re.search(r"(\d{2,4})\s*동", normalized)
    if building_match:
        building = building_match.group(1)
    unit_match = re.search(r"(\d{2,4})\s*호", normalized)
    if unit_match:
        unit = unit_match.group(1)
    if not building or not unit:
        joined = re.search(r"(\d{2,4})\s*[-/]\s*(\d{2,4})", normalized)
        if joined:
            building = building or joined.group(1)
            unit = unit or joined.group(2)
    return building, unit


def extract_phone(text: str) -> str:
    return normalize_phone(text)


def default_voice_state(*, from_phone: str = "") -> Dict[str, Any]:
    return {
        "stage": "ask_location",
        "retry_count": 0,
        "building": "",
        "unit": "",
        "complainant_phone": normalize_phone(from_phone),
        "content": "",
        "classification": {},
        "is_emergency": False,
        "handoff_reason": "",
    }


def is_yes(text: str, digits: str = "") -> bool:
    normalized = collapse_space(text).lower()
    if digits == "1":
        return True
    return any(token in normalized for token in VOICE_CONFIRM_YES)


def is_no(text: str, digits: str = "") -> bool:
    normalized = collapse_space(text).lower()
    if digits == "2":
        return True
    return any(token in normalized for token in VOICE_CONFIRM_NO)


def detect_handoff_request(text: str) -> str:
    normalized = collapse_space(text)
    if any(keyword in normalized for keyword in VOICE_EMERGENCY_KEYWORDS):
        return "긴급 민원 감지"
    if any(keyword in normalized for keyword in VOICE_HANDOFF_KEYWORDS):
        return "상담원 연결 요청"
    return ""


def _update_retry(state: Dict[str, Any]) -> int:
    retries = int(state.get("retry_count") or 0) + 1
    state["retry_count"] = retries
    return retries


def _reset_retry(state: Dict[str, Any]) -> None:
    state["retry_count"] = 0


def _merge_content(current: str, incoming: str) -> str:
    base = collapse_space(current)
    new_text = collapse_space(incoming)
    if not new_text:
        return base
    if not base:
        return new_text
    if new_text in base:
        return base
    return f"{base} / {new_text}"


def _classify_from_state(state: Dict[str, Any]) -> Dict[str, Any]:
    composed = " ".join(
        part
        for part in (
            f"{state.get('building')}동" if state.get("building") else "",
            f"{state.get('unit')}호" if state.get("unit") else "",
            str(state.get("content") or "").strip(),
        )
        if part
    ).strip()
    if not composed:
        return {}
    item = classify_complaint_text(composed)
    item["summary"] = normalize_summary_text(
        item.get("summary") or "",
        building=str(state.get("building") or ""),
        unit=str(state.get("unit") or ""),
        complaint_type=str(item.get("type") or ""),
    )
    return item


def _confirmation_text(state: Dict[str, Any]) -> str:
    classified = state.get("classification") or _classify_from_state(state)
    summary = str(classified.get("summary") or state.get("content") or "민원").strip()
    summary = normalize_summary_text(
        summary,
        building=str(state.get("building") or ""),
        unit=str(state.get("unit") or ""),
        complaint_type=str(classified.get("type") or ""),
    )
    parts = []
    if state.get("building"):
        parts.append(f"{state['building']}동")
    if state.get("unit"):
        parts.append(f"{state['unit']}호")
    if state.get("complainant_phone"):
        parts.append(f"연락처 {speech_phone(state['complainant_phone'])}")
    head = " ".join(parts).strip()
    if head:
        return f"접수 내용을 확인하겠습니다. {head}, 내용은 {summary} 입니다. 맞으면 1번 또는 예, 다시 말씀하시려면 2번 또는 아니오라고 말씀해 주세요."
    return f"접수 내용을 확인하겠습니다. {summary} 입니다. 맞으면 1번 또는 예, 다시 말씀하시려면 2번 또는 아니오라고 말씀해 주세요."


def complaint_payload_from_state(*, tenant_id: str, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    content = collapse_space(state.get("content") or "")
    if not content:
        return None
    classified = state.get("classification") or _classify_from_state(state)
    complaint_type = str(classified.get("type") or "기타").strip() or "기타"
    urgency = "긴급" if state.get("is_emergency") else str(classified.get("urgency") or "일반").strip() or "일반"
    summary = normalize_summary_text(
        str(classified.get("summary") or content),
        building=str(state.get("building") or ""),
        unit=str(state.get("unit") or ""),
        complaint_type=complaint_type,
    )
    return {
        "tenant_id": str(tenant_id or "").strip().lower(),
        "building": str(state.get("building") or "").strip(),
        "unit": str(state.get("unit") or "").strip(),
        "complainant_phone": normalize_phone(state.get("complainant_phone") or ""),
        "channel": "전화",
        "content": content,
        "summary": summary,
        "type": complaint_type,
        "urgency": urgency,
        "status": "접수",
    }


def advance_voice_flow(*, state: Dict[str, Any], utterance: str, digits: str = "") -> Dict[str, Any]:
    current = dict(default_voice_state(from_phone=state.get("complainant_phone") or ""))
    current.update(state or {})
    current["complainant_phone"] = normalize_phone(current.get("complainant_phone") or "")
    current["stage"] = str(current.get("stage") or "ask_location")

    text = collapse_space(utterance)
    handoff_reason = detect_handoff_request(text)

    if handoff_reason:
        building, unit = extract_building_unit(text)
        current["building"] = current.get("building") or building
        current["unit"] = current.get("unit") or unit
        current["content"] = _merge_content(str(current.get("content") or ""), text)
        current["classification"] = _classify_from_state(current)
        current["is_emergency"] = True
        current["handoff_reason"] = handoff_reason
        current["stage"] = "handoff"
        _reset_retry(current)
        return {
            "state": current,
            "assistant_message": "긴급 또는 상담원 연결 요청으로 확인되어 담당자에게 바로 연결하겠습니다.",
            "action": "handoff",
            "status": "handoff",
        }

    if current["stage"] == "confirm":
        if is_yes(text, digits):
            current["classification"] = current.get("classification") or _classify_from_state(current)
            current["stage"] = "completed"
            _reset_retry(current)
            return {
                "state": current,
                "assistant_message": "민원 접수를 완료하겠습니다.",
                "action": "create_complaint",
                "status": "completed",
            }
        if is_no(text, digits):
            current["stage"] = "ask_location"
            current["building"] = ""
            current["unit"] = ""
            current["content"] = ""
            current["classification"] = {}
            _reset_retry(current)
            return {
                "state": current,
                "assistant_message": "알겠습니다. 동과 호수를 다시 말씀해 주세요.",
                "action": "gather",
                "status": "in_progress",
            }

    if not text and not digits:
        retries = _update_retry(current)
        if retries > VOICE_MAX_RETRIES:
            current["stage"] = "handoff"
            current["handoff_reason"] = "응답 없음"
            return {
                "state": current,
                "assistant_message": "응답을 확인하지 못했습니다. 담당자 연결 또는 추후 재연락으로 넘기겠습니다.",
                "action": "handoff" if handoff_number() else "complete",
                "status": "handoff" if handoff_number() else "no_input",
            }
        prompt = {
            "ask_location": "동과 호수를 다시 말씀해 주세요. 예를 들면 101동 1203호입니다.",
            "ask_issue": "불편하신 내용을 다시 말씀해 주세요.",
            "ask_phone": "연락 가능한 전화번호를 말씀해 주세요.",
            "confirm": "접수 내용을 다시 확인하겠습니다. 맞으면 1번 또는 예, 아니면 2번 또는 아니오라고 말씀해 주세요.",
        }.get(current["stage"], "말씀을 다시 한 번 부탁드립니다.")
        return {"state": current, "assistant_message": prompt, "action": "gather", "status": "in_progress"}

    if current["stage"] in {"ask_location", "start"}:
        building, unit = extract_building_unit(text)
        if building:
            current["building"] = building
        if unit:
            current["unit"] = unit
        if current.get("building") and current.get("unit"):
            current["stage"] = "ask_issue"
            _reset_retry(current)
            return {
                "state": current,
                "assistant_message": f"{current['building']}동 {current['unit']}호로 확인했습니다. 불편하신 내용을 말씀해 주세요.",
                "action": "gather",
                "status": "in_progress",
            }
        _update_retry(current)
        return {
            "state": current,
            "assistant_message": "동과 호수를 확인하지 못했습니다. 101동 1203호처럼 다시 말씀해 주세요.",
            "action": "gather",
            "status": "in_progress",
        }

    if current["stage"] == "ask_issue":
        current["content"] = _merge_content(str(current.get("content") or ""), text)
        current["classification"] = _classify_from_state(current)
        if not current["complainant_phone"]:
            current["stage"] = "ask_phone"
            _reset_retry(current)
            return {
                "state": current,
                "assistant_message": "연락 가능한 전화번호를 말씀해 주세요.",
                "action": "gather",
                "status": "in_progress",
            }
        current["stage"] = "confirm"
        _reset_retry(current)
        return {
            "state": current,
            "assistant_message": _confirmation_text(current),
            "action": "gather",
            "status": "in_progress",
        }

    if current["stage"] == "ask_phone":
        phone = extract_phone(text)
        if phone:
            current["complainant_phone"] = phone
            current["stage"] = "confirm"
            _reset_retry(current)
            return {
                "state": current,
                "assistant_message": _confirmation_text(current),
                "action": "gather",
                "status": "in_progress",
            }
        _update_retry(current)
        return {
            "state": current,
            "assistant_message": "전화번호를 확인하지 못했습니다. 숫자만 천천히 다시 말씀해 주세요.",
            "action": "gather",
            "status": "in_progress",
        }

    current["classification"] = current.get("classification") or _classify_from_state(current)
    current["stage"] = "confirm"
    return {
        "state": current,
        "assistant_message": _confirmation_text(current),
        "action": "gather",
        "status": "in_progress",
    }
