from __future__ import annotations

import base64
import json
import logging
import os
import re
from contextvars import ContextVar
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

try:
    from PIL import Image as PILImage
except Exception:  # pragma: no cover - pillow optional at runtime
    PILImage = None

logger = logging.getLogger("ka-part.work-report")

WORK_REPORT_ACTION_KEYWORDS = (
    "교체",
    "설치",
    "보수",
    "수리",
    "청소",
    "점검",
    "조치",
    "정비",
    "복구",
    "시공",
    "철거",
    "도색",
    "도장",
    "보강",
    "세척",
    "개선",
    "정리",
    "조정",
    "변경",
    "회수",
    "급수",
    "구입",
)
WORK_REPORT_VENDOR_HINTS = ("업체", "업 체", "시공사", "협력업체", "담당", "작업자")
WORK_REPORT_FEEDBACK_STOP_TOKENS = {
    "관리실",
    "작업",
    "공사",
    "교체",
    "보수",
    "수리",
    "설치",
    "조치",
    "완료",
    "진행",
    "접수",
    "현장",
}
WORK_REPORT_STAGE_HINTS = {
    "before": ("before", "전", "작업전", "교체전", "시공전", "조치전", "보수전"),
    "during": ("during", "중", "작업중", "교체중", "시공중", "진행중"),
    "after": ("after", "후", "작업후", "교체후", "시공후", "조치후", "완료"),
}
MAX_WORK_REPORT_IMAGES = 200
MAX_WORK_REPORT_ATTACHMENTS = 30
MAX_WORK_REPORT_OPENAI_VISUAL_IMAGES = 10
WORK_REPORT_OPENAI_IMAGE_MAX_DIM = 1024
WORK_REPORT_OPENAI_IMAGE_QUALITY = 82
WORK_REPORT_OPENAI_CLUSTER_GAP_SECONDS = 180
WORK_REPORT_OPENAI_CLUSTER_CONTEXT_MINUTES = 12
WORK_REPORT_HEAVY_IMAGE_THRESHOLD = 40
DEFAULT_WORK_REPORT_OPENAI_TIMEOUT_SEC = 180.0
DEFAULT_WORK_REPORT_OPENAI_IMAGE_MATCH_TIMEOUT_SEC = 90.0
WORK_REPORT_OPENAI_CHUNK_TRIGGER_IMAGES = 12
WORK_REPORT_OPENAI_IMAGE_MATCH_MAX_CLUSTERS = 4
WORK_REPORT_OPENAI_IMAGE_MATCH_SAMPLE_PER_CLUSTER = 3
MAX_WORK_REPORT_OPENAI_REFERENCE_IMAGES = 6
WORK_REPORT_OPENAI_TEXT_MAX_CHARS = 14000
WORK_REPORT_OPENAI_TEXT_MAX_EVENTS = 160
_WORK_REPORT_OPENAI_LAST_ERROR: ContextVar[str] = ContextVar("work_report_openai_last_error", default="")
_WORK_REPORT_OPENAI_LAST_ERROR_REASON: ContextVar[str] = ContextVar("work_report_openai_last_error_reason", default="")
_WORK_REPORT_OPENAI_LAST_ERROR_DETAILS: ContextVar[str] = ContextVar("work_report_openai_last_error_details", default="")
WORK_REPORT_FILE_EXTENSIONS = (
    ".pdf",
    ".hwp",
    ".hwpx",
    ".txt",
    ".md",
    ".xlsx",
    ".xls",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".jpg",
    ".jpeg",
    ".png",
    ".zip",
)
KAKAO_DATE_HEADER_RE = re.compile(
    r"^(?:-+\s*)?(?P<y>\d{4})년\s*(?P<m>\d{1,2})월\s*(?P<d>\d{1,2})일(?:\s*[가-힣]+요일)?(?:\s*-+)?$"
)
KAKAO_INLINE_MESSAGE_RE = re.compile(
    r"^(?P<y>\d{4})년\s*(?P<m>\d{1,2})월\s*(?P<d>\d{1,2})일\s*(?:오전|오후)?\s*\d{1,2}:\d{2},\s*(?P<sender>[^:]{1,40})\s*:\s*(?P<body>.+)$"
)
KAKAO_BRACKET_MESSAGE_RE = re.compile(r"^\[(?P<sender>[^\]]+)\]\s*\[(?P<time>[^\]]+)\]\s*(?P<body>.+)$")
KAKAO_SHORT_MESSAGE_RE = re.compile(r"^(?P<time>(?:오전|오후)\s*\d{1,2}:\d{2}),?\s*(?P<sender>[^:]{1,40})\s*:\s*(?P<body>.+)$")
WorkReportProgressCallback = Callable[[Dict[str, Any]], None]


def _collapse(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u0000", " ")).strip()


def _float_env(name: str, default: float) -> float:
    try:
        return max(1.0, float(str(os.getenv(name) or "").strip() or default))
    except Exception:
        return float(default)


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(str(os.getenv(name) or "").strip() or default))
    except Exception:
        return int(default)


def _str_env(name: str, default: str = "") -> str:
    return str(os.getenv(name) or default or "").strip()


def _clear_openai_error_state() -> None:
    _WORK_REPORT_OPENAI_LAST_ERROR.set("")
    _WORK_REPORT_OPENAI_LAST_ERROR_REASON.set("")
    _WORK_REPORT_OPENAI_LAST_ERROR_DETAILS.set("")


def _classify_openai_error_message(message: str) -> str:
    lowered = _collapse(message).lower()
    if not lowered:
        return ""
    if "insufficient_quota" in lowered or "exceeded your current quota" in lowered:
        return "insufficient_quota"
    if "rate limit" in lowered or "429" in lowered:
        return "rate_limited"
    if "timeout" in lowered or "timed out" in lowered or "readtimeout" in lowered:
        return "api_timeout"
    if "api key" in lowered or "authentication" in lowered or "invalid_api_key" in lowered or "401" in lowered:
        return "auth_error"
    return "openai_error"


def _analysis_reason_notice(reason: str, *, details: str = "") -> str:
    reason_key = str(reason or "").strip().lower()
    if reason_key == "insufficient_quota":
        return "OpenAI 할당량이 부족해 AI 분석을 계속할 수 없습니다."
    if reason_key == "rate_limited":
        return "OpenAI 요청 한도에 걸려 AI 분석이 지연되거나 실패했습니다."
    if reason_key == "api_timeout":
        return "OpenAI 응답 시간이 초과되어 AI 분석이 중단됐습니다."
    if reason_key == "missing_api_key":
        return "OpenAI API 키가 설정되지 않아 AI 분석을 사용할 수 없습니다."
    if reason_key == "missing_sdk":
        return "OpenAI SDK가 준비되지 않아 AI 분석을 사용할 수 없습니다."
    if reason_key == "auth_error":
        return "OpenAI 인증 설정을 확인해야 합니다."
    if reason_key == "invalid_json":
        return "AI 응답에서 JSON 결과를 읽지 못했습니다."
    detail_text = _collapse(details)[:180]
    return f"AI 분석 호출에 실패했습니다: {detail_text}" if detail_text else "AI 분석 호출에 실패했습니다."


def _analysis_reason_label(reason: str) -> str:
    reason_key = str(reason or "").strip().lower()
    if reason_key == "api_timeout":
        return "응답 시간 초과"
    if reason_key == "insufficient_quota":
        return "할당량 부족"
    if reason_key == "rate_limited":
        return "요청 한도 초과"
    if reason_key == "missing_api_key":
        return "API 키 미설정"
    if reason_key == "missing_sdk":
        return "SDK 미설치"
    if reason_key == "auth_error":
        return "인증 설정 오류"
    if reason_key == "invalid_json":
        return "응답 형식 오류"
    if reason_key == "openai_error":
        return "OpenAI 호출 실패"
    return ""


def _analysis_mode_label(model: str, reason: str = "") -> str:
    model_name = _collapse(model)
    if model_name == "heuristic":
        return "규칙 기반"
    if model_name.startswith("gpt-"):
        return "OpenAI + 보조 분석" if _collapse(reason) else f"OpenAI ({model_name})"
    return model_name or "-"


def _set_openai_error_state(reason: str, notice: str, *, details: str = "") -> None:
    _WORK_REPORT_OPENAI_LAST_ERROR.set(_collapse(notice))
    _WORK_REPORT_OPENAI_LAST_ERROR_REASON.set(_collapse(reason))
    _WORK_REPORT_OPENAI_LAST_ERROR_DETAILS.set(_collapse(details)[:500])


def _summarize_openai_error(exc: Exception) -> str:
    message = _collapse(str(exc))
    return _analysis_reason_notice(_classify_openai_error_message(message), details=message)


def _consume_openai_error_snapshot() -> Dict[str, str]:
    snapshot = {
        "reason": _collapse(_WORK_REPORT_OPENAI_LAST_ERROR_REASON.get("")),
        "notice": _collapse(_WORK_REPORT_OPENAI_LAST_ERROR.get("")),
        "details": _collapse(_WORK_REPORT_OPENAI_LAST_ERROR_DETAILS.get("")),
    }
    _clear_openai_error_state()
    return snapshot


def _consume_openai_error_notice() -> str:
    return _consume_openai_error_snapshot().get("notice", "")


def _append_unique_note(notes: List[str], text: str) -> None:
    message = _collapse(text)
    if message and message not in notes:
        notes.append(message)


def _report_progress(
    callback: WorkReportProgressCallback | None,
    *,
    current_step: int,
    total_steps: int = 5,
    summary: str = "",
    hint: str = "",
) -> None:
    if not callback:
        return
    payload = {
        "current_step": max(0, int(current_step)),
        "total_steps": max(1, int(total_steps)),
        "summary": _collapse(summary or ""),
        "hint": _collapse(hint or ""),
    }
    callback(payload)


def _extract_json_text(raw_text: Any) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.IGNORECASE | re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return text[start : end + 1].strip()
    return text


def _normalize_ai_work_report_payload(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    items = data.get("items")
    if isinstance(items, list):
        return dict(data)
    tasks = data.get("tasks")
    if not isinstance(tasks, list):
        return dict(data)
    normalized_items: List[Dict[str, Any]] = []
    for row in tasks:
        if not isinstance(row, dict):
            continue
        description = _clean_item_title(row.get("description") or row.get("title") or "")
        if not description:
            continue
        assignee = _collapse(row.get("assignee") or "")
        status = _collapse(row.get("status") or "")
        summary_parts = [description]
        if status:
            summary_parts.append(status)
        if assignee:
            summary_parts.append(f"담당 {assignee}")
        normalized_items.append(
            {
                "title": description,
                "work_date": _collapse(row.get("date") or ""),
                "work_date_label": "",
                "vendor_name": "",
                "location_name": "",
                "summary": " / ".join(part for part in summary_parts if part),
                "before_image_indexes": [int(value) for value in row.get("before_image_indexes") or [] if str(value).isdigit()],
                "during_image_indexes": [int(value) for value in row.get("during_image_indexes") or [] if str(value).isdigit()],
                "after_image_indexes": [int(value) for value in row.get("after_image_indexes") or [] if str(value).isdigit()],
                "attachment_indexes": [int(value) for value in row.get("attachment_indexes") or [] if str(value).isdigit()],
                "confidence": _collapse(row.get("confidence") or "ai"),
            }
        )
    normalized = dict(data)
    normalized["items"] = normalized_items
    if normalized_items and not _collapse(normalized.get("analysis_notice") or ""):
        normalized["analysis_notice"] = "AI 응답 형식을 보정해 적용했습니다."
    return normalized


def _safe_date_value(year: int, month: int, day: int) -> str:
    try:
        return date(int(year), int(month), int(day)).isoformat()
    except Exception:
        return ""


def _date_label(value: str) -> str:
    raw = _collapse(value)
    if not raw:
        return ""
    try:
        parsed = date.fromisoformat(raw[:10])
    except Exception:
        return ""
    return f"{parsed.month}월 {parsed.day}일"


def _time_minutes(text: str) -> int:
    match = re.search(r"(오전|오후)\s*(\d{1,2}):(\d{2})", str(text or ""))
    if not match:
        return -1
    hour = int(match.group(2)) % 12
    if match.group(1) == "오후":
        hour += 12
    return (hour * 60) + int(match.group(3))


def _openai_client(default_model: str = "gpt-5", env_name: str = "OPENAI_MODEL") -> Tuple[Any | None, str]:
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        _set_openai_error_state("missing_api_key", _analysis_reason_notice("missing_api_key"))
        return None, ""
    try:
        from openai import OpenAI
    except Exception as exc:
        _set_openai_error_state("missing_sdk", _analysis_reason_notice("missing_sdk"), details=str(exc))
        return None, ""
    model = str(os.getenv(env_name) or os.getenv("OPENAI_MODEL") or default_model).strip() or default_model
    _clear_openai_error_state()
    return OpenAI(api_key=api_key), model


def _openai_event_excerpt_line(event: Dict[str, Any]) -> str:
    body = _collapse(event.get("text") or "")
    if not body:
        return ""
    sender = _collapse(event.get("sender") or "-")
    date_label = _collapse(event.get("date_label") or event.get("date") or "-")
    minute = int(event.get("minute_of_day") or -1)
    time_label = f"{minute // 60:02d}:{minute % 60:02d}" if minute >= 0 else "--:--"
    return f"- {date_label} {time_label} / {sender} / {body}"


def _openai_text_excerpt(text: str) -> Dict[str, Any]:
    raw = str(text or "")
    if not _collapse(raw):
        return {
            "text": "",
            "applied": False,
            "mode": "empty",
            "event_count": 0,
            "char_count": 0,
            "source_char_count": 0,
        }
    max_chars = _int_env("WORK_REPORT_OPENAI_TEXT_MAX_CHARS", WORK_REPORT_OPENAI_TEXT_MAX_CHARS, minimum=2000)
    max_events = _int_env("WORK_REPORT_OPENAI_TEXT_MAX_EVENTS", WORK_REPORT_OPENAI_TEXT_MAX_EVENTS, minimum=20)
    if len(raw) <= max_chars:
        return {
            "text": raw,
            "applied": False,
            "mode": "full",
            "event_count": 0,
            "char_count": len(raw),
            "source_char_count": len(raw),
        }
    events = _parse_kakao_events(raw)
    excerpt_lines: List[str] = []
    for event in events[:max_events]:
        line = _openai_event_excerpt_line(event)
        if line:
            excerpt_lines.append(line)
    if excerpt_lines:
        excerpt = "\n".join(excerpt_lines)
        mode = "event_excerpt"
        event_count = len(excerpt_lines)
    else:
        lines = [_collapse(line) for line in raw.splitlines() if _collapse(line)]
        excerpt = "\n".join(lines[:max_events])
        mode = "line_excerpt"
        event_count = 0
    if len(excerpt) > max_chars:
        excerpt = f"{excerpt[: max(1, max_chars - 1)].rstrip()}…"
    return {
        "text": excerpt,
        "applied": True,
        "mode": mode,
        "event_count": event_count,
        "char_count": len(excerpt),
        "source_char_count": len(raw),
    }


def _sample_heading(sample_title: str, sample_lines: Sequence[str]) -> str:
    for line in sample_lines:
        text = _collapse(line)
        if text and "보고" in text:
            return text
    return _collapse(sample_title) or "시설팀 주요 업무 보고"


def _sample_period(sample_lines: Sequence[str]) -> str:
    for line in sample_lines:
        text = _collapse(line)
        if "보고기간" in text:
            if ":" in text:
                return _collapse(text.split(":", 1)[1])
            return text.replace("보고기간", "").strip(" :")
    return ""


def _extract_date(text: str) -> Tuple[str, str]:
    raw = _collapse(text)
    if not raw:
        return "", ""
    m = re.search(r"(?P<y>\d{4})\s*년\s*(?P<m>\d{1,2})\s*월\s*(?P<d>\d{1,2})\s*일", raw)
    if m:
        value = _safe_date_value(int(m.group("y")), int(m.group("m")), int(m.group("d")))
        month = int(m.group("m"))
        day = int(m.group("d"))
        return value, f"{month}월 {day}일"
    m = re.search(r"(?P<y>\d{4})[./-]\s*(?P<m>\d{1,2})[./-]\s*(?P<d>\d{1,2})", raw)
    if m:
        value = _safe_date_value(int(m.group("y")), int(m.group("m")), int(m.group("d")))
        month = int(m.group("m"))
        day = int(m.group("d"))
        return value, f"{month}월 {day}일"
    m = re.search(r"(?P<m>\d{1,2})\s*월\s*(?P<d>\d{1,2})\s*일", raw)
    if m:
        value = _safe_date_value(datetime.now().year, int(m.group("m")), int(m.group("d")))
        month = int(m.group("m"))
        day = int(m.group("d"))
        return value, f"{month}월 {day}일"
    return "", ""


def _normalize_message_line(line: str) -> Dict[str, str]:
    raw = _collapse(line)
    if not raw:
        return {"text": "", "date": "", "date_label": "", "sender": ""}
    date_value, date_label = _extract_date(raw)
    text = raw
    sender = ""
    if ":" in raw:
        prefix, body = raw.split(":", 1)
        body = _collapse(body)
        if body:
            text = body
            sender_candidate = _collapse(prefix.split(",")[-1])
            if sender_candidate and len(sender_candidate) <= 20:
                sender = sender_candidate
    text = re.sub(r"^\d{4}년\s*\d{1,2}월\s*\d{1,2}일\s*(오전|오후)?\s*\d{1,2}:\d{2},?\s*", "", text)
    text = re.sub(r"^\d{4}[./-]\d{1,2}[./-]\d{1,2}\s*(오전|오후)?\s*\d{1,2}:\d{2},?\s*", "", text)
    text = _collapse(text)
    return {
        "text": text,
        "date": date_value,
        "date_label": date_label,
        "sender": sender,
    }


def _looks_like_work_item(text: str) -> bool:
    normalized = _collapse(text)
    if len(normalized) < 4:
        return False
    lowered = normalized.lower()
    if re.search(r"\d{2,4}-\d{3,4}\s*/\s*010-\d{4}-\d{4}", normalized):
        return False
    if lowered in {"완료", "교체완료", "작업완료", "사진", "동영상", "입고"}:
        return False
    if re.fullmatch(r"사진\s*\d+\s*장", normalized):
        return False
    if any(
        phrase in normalized
        for phrase in (
            "방문요청하심",
            "통화원하심",
            "전화드렸는데",
            "모르겠습니다",
            "주중에 방문하기로",
            "구매처 문의",
        )
    ):
        return False
    if "작업내용" in normalized:
        return True
    if any(pattern in lowered for pattern in ("as요청", "as접수", "요청함", "교체예정", "타이머조정", "변경 완료", "회수함", "입고")):
        return True
    return any(keyword in normalized for keyword in WORK_REPORT_ACTION_KEYWORDS)


def _is_question_like_line(text: str) -> bool:
    normalized = _collapse(text)
    lowered = normalized.lower()
    if "?" in normalized:
        return True
    return any(
        token in lowered
        for token in (
            "가능한지",
            "봐줄수",
            "봐줄 수",
            "전화바랍니다",
            "전화 주세요",
            "전화주세요",
            "원해요",
            "원해요 ",
            "해주시나요",
            "있는건지",
            "있는 건지",
            "문의하심",
        )
    )


def _is_generic_status_line(text: str) -> bool:
    normalized = _collapse(text)
    lowered = normalized.lower()
    if lowered in {
        "완료",
        "통화 완료",
        "통화완료",
        "교체 완료",
        "교체완료",
        "보수완료",
        "작업완료",
        "설치완료",
        "복구완료",
        "사진",
        "동영상",
        "입고",
    }:
        return True
    if any(lowered.startswith(prefix) for prefix in ("통화완료", "통화 완료", "교체 완료", "교체완료", "보수완료", "작업완료")):
        return True
    if not _guess_location(normalized) and any(
        lowered.endswith(suffix)
        for suffix in ("안내함", "안내드림", "전달요청함", "문의함", "확인함", "대기중", "대기 중")
    ):
        return True
    return False


def _looks_like_heuristic_anchor(text: str, *, image_heavy: bool = False) -> bool:
    normalized = _collapse(text)
    if not _looks_like_work_item(normalized):
        return False
    if _is_question_like_line(normalized) or _is_generic_status_line(normalized):
        return False
    has_location = bool(_guess_location(normalized))
    keyword_count = len(_title_tokens(normalized))
    if image_heavy:
        if has_location:
            return True
        if any(pattern in normalized.lower() for pattern in ("as접수", "as요청", "교체예정", "회수함", "입고", "설치", "복구")):
            return keyword_count >= 1
        return keyword_count >= 2
    return has_location or keyword_count >= 1


def _should_skip_context_line(text: str) -> bool:
    normalized = _collapse(text)
    if not normalized:
        return True
    if re.match(r"^\[[^\]]+\]\s*\[[^\]]*$", normalized):
        return True
    if re.search(r"\d{2,4}-\d{3,4}\s*/\s*010-\d{4}-\d{4}", normalized):
        return True
    return any(
        phrase in normalized
        for phrase in (
            "방문요청하심",
            "통화원하심",
            "전화드렸는데",
            "모르겠습니다",
            "주중에 방문하기로",
            "구매처 문의",
            "세대인계",
        )
    )


def _extract_tagged_pairs(text: str) -> Dict[str, str]:
    values = [_collapse(token) for token in re.findall(r"<([^<>]*)>", str(text or "")) if _collapse(token)]
    pairs: Dict[str, str] = {}
    for index in range(0, len(values) - 1, 2):
        key = re.sub(r"[\s:]+", "", values[index])
        value = values[index + 1]
        if key and value:
            pairs[key] = value
    return pairs


def _guess_vendor(text: str, sender: str = "") -> str:
    normalized = _collapse(text)
    m = re.search(r"(업체|업\s*체|시공사|협력업체|담당)\s*[:：]?\s*([가-힣A-Za-z0-9().\- ]{2,30})", normalized)
    if m:
        candidate = _collapse(m.group(2))
        if candidate and not any(token in candidate for token in ("요청", "접수", "확인", "완료", "예정")):
            return candidate
    if sender and sender not in {"회원", "관리실 알림"}:
        return sender
    return ""


def _guess_location(text: str) -> str:
    normalized = _collapse(text)
    building = re.search(r"(\d{2,4})\s*동", normalized)
    unit = re.search(r"(\d{2,4})\s*호", normalized)
    parts: List[str] = []
    if building:
        parts.append(f"{building.group(1)}동")
    if unit:
        parts.append(f"{unit.group(1)}호")
    if parts:
        return " ".join(parts)
    return ""


def _clean_item_title(text: str) -> str:
    normalized = _collapse(text)
    normalized = re.sub(r"^(작업내용|작업|내용)\s*[:：]?\s*", "", normalized)
    return normalized[:120]


def _title_key(text: str) -> str:
    return re.sub(r"[^0-9a-z가-힣]+", "", _clean_item_title(text).lower())


def _title_action_keyword(text: str) -> str:
    normalized = _clean_item_title(text)
    for keyword in WORK_REPORT_ACTION_KEYWORDS:
        if keyword in normalized:
            return keyword
    return ""


def _title_tokens(text: str) -> set[str]:
    stop_words = set(WORK_REPORT_ACTION_KEYWORDS) | {"작업", "민원", "사항", "업체", "요청", "접수", "완료", "예정"}
    tokens = {
        _collapse(token).lower()
        for token in re.findall(r"\d+동|\d+호|[가-힣a-zA-Z]{2,}", _clean_item_title(text))
        if _collapse(token)
    }
    return {token for token in tokens if token not in stop_words}


def _titles_match(left: str, right: str) -> bool:
    left_key = _title_key(left)
    right_key = _title_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    if (len(left_key) >= 8 and left_key in right_key) or (len(right_key) >= 8 and right_key in left_key):
        return True
    common_tokens = _title_tokens(left) & _title_tokens(right)
    if len(common_tokens) >= 2:
        return True
    left_action = _title_action_keyword(left)
    right_action = _title_action_keyword(right)
    if common_tokens and left_action and left_action == right_action:
        if not _guess_location(left) or not _guess_location(right):
            return True
    return False


def _tagged_value(tagged: Dict[str, str], *labels: str) -> str:
    normalized_labels = [re.sub(r"[\s:]+", "", label) for label in labels if _collapse(label)]
    for key, value in tagged.items():
        normalized_key = re.sub(r"[\s:]+", "", key)
        if any(label in normalized_key for label in normalized_labels):
            return _collapse(value)
    return ""


def _message_kind(text: str) -> str:
    normalized = _collapse(text)
    lower = normalized.lower()
    if not normalized:
        return "message"
    if "사진을 보냈" in normalized or "이미지를 보냈" in normalized:
        return "photo_notice"
    if re.fullmatch(r"(사진|이미지|원본사진)(\s*\d+\s*장)?", normalized):
        return "photo_notice"
    if re.search(r"(사진|이미지)\s*\d+\s*장$", normalized):
        return "photo_notice"
    if "파일을 보냈" in normalized or normalized.startswith("파일 ") or normalized.startswith("파일:"):
        return "file_notice"
    if any(ext in lower for ext in WORK_REPORT_FILE_EXTENSIONS):
        return "file_notice"
    if any(keyword in normalized for keyword in ("견적서", "작업내역서", "세금계산서", "확인서")):
        return "file_notice"
    return "message"


def _notice_count(text: str) -> int:
    normalized = _collapse(text)
    m = re.search(r"(\d+)\s*(장|건|개)", normalized)
    if m:
        return max(1, int(m.group(1)))
    return 1


def _parse_kakao_events(text: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    current_date = ""
    for index, line in enumerate(str(text or "").splitlines(), start=1):
        raw = _collapse(line)
        if not raw:
            continue
        header = KAKAO_DATE_HEADER_RE.match(raw)
        if header:
            current_date = _safe_date_value(header.group("y"), header.group("m"), header.group("d"))
            continue
        message_match = KAKAO_INLINE_MESSAGE_RE.match(raw)
        sender = ""
        body = raw
        date_value = ""
        if message_match:
            sender = _collapse(message_match.group("sender"))
            body = _collapse(message_match.group("body"))
            date_value = _safe_date_value(message_match.group("y"), message_match.group("m"), message_match.group("d"))
            current_date = date_value or current_date
        else:
            bracket_match = KAKAO_BRACKET_MESSAGE_RE.match(raw)
            short_match = KAKAO_SHORT_MESSAGE_RE.match(raw)
            if bracket_match:
                sender = _collapse(bracket_match.group("sender"))
                body = _collapse(bracket_match.group("body"))
                date_value = current_date
            elif short_match:
                sender = _collapse(short_match.group("sender"))
                body = _collapse(short_match.group("body"))
                date_value = current_date
        if not date_value:
            date_value, _ = _extract_date(body)
            if not date_value:
                date_value = current_date
        events.append(
            {
                "index": index,
                "date": date_value,
                "date_label": _date_label(date_value),
                "sender": sender,
                "text": body,
                "kind": _message_kind(body),
                "minute_of_day": _time_minutes(raw),
            }
        )
    return events


def _new_item(title: str, event: Dict[str, Any], *, confidence: str) -> Dict[str, Any]:
    clean_title = _clean_item_title(title)
    work_date = _collapse(event.get("date") or "")
    return {
        "index": 0,
        "title": clean_title,
        "summary": clean_title,
        "work_date": work_date,
        "work_date_label": _collapse(event.get("date_label") or _date_label(work_date)),
        "vendor_name": _guess_vendor(clean_title, sender=str(event.get("sender") or "")),
        "location_name": _guess_location(clean_title),
        "confidence": confidence,
        "images": [],
        "attachments": [],
        "_expected_attachment_count": 0,
        "_image_notices": [],
        "_attachment_notice_texts": [],
        "_attachment_notice_tokens": [],
        "_minute_of_day": int(event.get("minute_of_day") or -1),
    }


def _supplement_text_only_items(existing_items: Sequence[Dict[str, Any]], text: str, *, image_heavy: bool = False) -> List[Dict[str, Any]]:
    seeded_items = list(existing_items or [])
    added_items: List[Dict[str, Any]] = []
    seen_keys = {
        (_title_key(item.get("title") or ""), _collapse(item.get("work_date") or ""))
        for item in seeded_items
        if _title_key(item.get("title") or "")
    }
    for event in _parse_kakao_events(text):
        text_line = _collapse(event.get("text") or "")
        if not text_line or not _looks_like_heuristic_anchor(text_line, image_heavy=image_heavy):
            continue
        if any(_titles_match(text_line, item.get("title") or "") for item in seeded_items + added_items):
            continue
        title_key = _title_key(text_line)
        date_key = _collapse(event.get("date") or "")
        if not title_key or (title_key, date_key) in seen_keys:
            continue
        item = _new_item(text_line, event, confidence="heuristic-supplement")
        item["images"] = []
        item["attachments"] = []
        item.pop("_expected_attachment_count", None)
        item.pop("_image_notices", None)
        item.pop("_attachment_notice_texts", None)
        item.pop("_attachment_notice_tokens", None)
        item.pop("_minute_of_day", None)
        added_items.append(item)
        seen_keys.add((title_key, date_key))
    return added_items


def _append_summary(item: Dict[str, Any], text: str) -> None:
    addition = _collapse(text)
    if not addition:
        return
    title = _collapse(item.get("title") or "")
    current_summary = _collapse(item.get("summary") or "")
    if addition == title or addition == current_summary or addition in current_summary:
        return
    if current_summary and current_summary != title:
        item["summary"] = f"{current_summary} / {addition}"[:240]
    else:
        item["summary"] = addition[:240]


def _apply_tagged_fields(item: Dict[str, Any], tagged: Dict[str, str], event: Dict[str, Any] | None = None) -> None:
    title = _tagged_value(tagged, "작업내용", "제목", "건명")
    if title:
        clean_title = _clean_item_title(title)
        item["title"] = clean_title
        if not _collapse(item.get("summary") or "") or _collapse(item.get("summary") or "") == _collapse(item.get("title") or ""):
            item["summary"] = clean_title
    date_text = _tagged_value(tagged, "작업일자", "작업일시", "보고일자", "수리일시", "일시", "일자")
    if date_text:
        work_date, work_date_label = _extract_date(date_text)
        if work_date:
            item["work_date"] = work_date
            item["work_date_label"] = work_date_label
    elif event and not item.get("work_date"):
        event_date = _collapse(event.get("date") or "")
        if event_date:
            item["work_date"] = event_date
            item["work_date_label"] = _collapse(event.get("date_label") or _date_label(event_date))
    vendor = _tagged_value(tagged, "수리업체", "업체명", "업체", "시공사", "협력업체", "담당")
    if vendor:
        item["vendor_name"] = vendor
    location = _tagged_value(tagged, "위치", "장소", "현장", "대상")
    if location:
        item["location_name"] = location
    elif item.get("title") and not item.get("location_name"):
        item["location_name"] = _guess_location(item.get("title") or "")


def _attachment_metadata(entry: Dict[str, Any]) -> Dict[str, str]:
    preview_text = str(entry.get("preview_text") or "")
    preview = _collapse(preview_text)
    tagged = _extract_tagged_pairs(preview_text)

    def line_value(pattern: str) -> str:
        match = re.search(rf"(?mi)^\s*(?:{pattern})\s*[:：]?\s*(.+?)\s*$", preview_text)
        return _collapse(match.group(1)) if match else ""

    title = _tagged_value(tagged, "작업내용", "제목", "건명") or line_value("작업내용|제목|건명")
    vendor = _tagged_value(tagged, "수리업체", "업체명", "업체", "시공사", "협력업체") or line_value("수리업체|업체명|시공사|협력업체|업체")
    date_text = _tagged_value(tagged, "작업일자", "작업일시", "보고일자", "수리일시", "일시", "일자") or line_value("작업일자|작업일시|보고일자|수리일시|일시|일자")
    location = _tagged_value(tagged, "위치", "장소", "현장", "대상") or line_value("위치|장소|현장|대상")
    work_date, work_date_label = _extract_date(date_text)
    summary = preview[:160]
    if not title:
        title = _collapse(Path(str(entry.get("filename") or "attachment")).stem)
    if not location:
        location = _guess_location(title or preview)
    return {
        "title": _clean_item_title(title),
        "vendor_name": vendor,
        "work_date": work_date,
        "work_date_label": work_date_label,
        "location_name": location,
        "summary": summary,
    }


def _apply_attachment_metadata(item: Dict[str, Any], matches: Sequence[Dict[str, Any]]) -> None:
    for match in matches:
        metadata = match.get("metadata") or {}
        if not isinstance(metadata, dict):
            continue
        if not item.get("work_date") and _collapse(metadata.get("work_date") or ""):
            item["work_date"] = _collapse(metadata.get("work_date") or "")
            item["work_date_label"] = _collapse(metadata.get("work_date_label") or _date_label(item["work_date"]))
        if not item.get("vendor_name") and _collapse(metadata.get("vendor_name") or ""):
            item["vendor_name"] = _collapse(metadata.get("vendor_name") or "")
        if not item.get("location_name") and _collapse(metadata.get("location_name") or ""):
            item["location_name"] = _collapse(metadata.get("location_name") or "")
        if not item.get("title") and _collapse(metadata.get("title") or ""):
            item["title"] = _clean_item_title(metadata.get("title") or "")
        if (_collapse(item.get("summary") or "") == _collapse(item.get("title") or "") or not _collapse(item.get("summary") or "")) and _collapse(metadata.get("summary") or ""):
            _append_summary(item, metadata.get("summary") or "")


def _tokenize(value: Any) -> List[str]:
    text = _collapse(value).lower()
    tokens = re.findall(r"[a-z]+|\d{1,4}|[가-힣]{2,}", text)
    return [token for token in tokens if token]


def _entry_stage(filename: str) -> str:
    stem = _collapse(Path(str(filename or "")).stem).lower()
    for stage, hints in WORK_REPORT_STAGE_HINTS.items():
        if any(hint in stem for hint in hints):
            return stage
    return ""


def _stage_label(stage: str) -> str:
    normalized = _collapse(stage).lower() or "general"
    label_map = {
        "before": "작업 전",
        "during": "작업 중",
        "after": "작업 후",
        "general": "현장 이미지",
    }
    return label_map.get(normalized, "현장 이미지")


def _entry_time_fields(filename: str) -> Dict[str, int | str]:
    stem = _collapse(Path(str(filename or "")).stem)
    match = re.search(r"(?P<date>\d{8})_(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})", stem)
    if not match:
        return {"date": "", "minute_of_day": -1, "second_of_day": -1}
    raw_date = match.group("date")
    date_value = _safe_date_value(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    second = int(match.group("second"))
    return {
        "date": date_value,
        "minute_of_day": (hour * 60) + minute,
        "second_of_day": (hour * 3600) + (minute * 60) + second,
    }


def _openai_image_meta(index: int, entry: Dict[str, Any]) -> Dict[str, Any]:
    filename = _collapse(entry.get("filename") or f"image-{index}")
    time_fields = _entry_time_fields(filename)
    return {
        "index": int(index),
        "filename": filename,
        "preview_available": bool(_collapse(entry.get("preview_relative_path") or "")),
        "date": _collapse(time_fields.get("date") or ""),
        "minute_of_day": int(time_fields.get("minute_of_day") or -1),
        "second_of_day": int(time_fields.get("second_of_day") or -1),
        "stage_hint": _entry_stage(filename),
    }


def _work_report_image_entries(image_inputs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [_openai_image_meta(index, row) for index, row in enumerate(list(image_inputs or []), start=1)]


def _cluster_openai_image_meta(entries: Sequence[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    ordered = sorted(
        list(entries or []),
        key=lambda row: (
            str(row.get("date") or ""),
            int(row.get("second_of_day") or -1) if int(row.get("second_of_day") or -1) >= 0 else 10**9,
            int(row.get("index") or 0),
        ),
    )
    if not ordered:
        return []
    clusters: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    for row in ordered:
        if not current:
            current = [row]
            continue
        previous = current[-1]
        row_second = int(row.get("second_of_day") or -1)
        prev_second = int(previous.get("second_of_day") or -1)
        same_date = _collapse(row.get("date") or "") == _collapse(previous.get("date") or "")
        if row_second >= 0 and prev_second >= 0 and same_date and abs(row_second - prev_second) <= WORK_REPORT_OPENAI_CLUSTER_GAP_SECONDS:
            current.append(row)
            continue
        clusters.append(current)
        current = [row]
    if current:
        clusters.append(current)
    return clusters


def _sample_cluster_indexes(cluster: Sequence[Dict[str, Any]]) -> List[int]:
    rows = list(cluster or [])
    if not rows:
        return []
    selected_indexes: List[int] = []
    first_index = int(rows[0].get("index") or 0)
    last_index = int(rows[-1].get("index") or 0)
    if first_index > 0:
        selected_indexes.append(first_index)
    if last_index > 0 and last_index not in selected_indexes:
        selected_indexes.append(last_index)
    if len(rows) >= 5:
        middle_index = int(rows[len(rows) // 2].get("index") or 0)
        if middle_index > 0 and middle_index not in selected_indexes:
            selected_indexes.append(middle_index)
    for row in rows:
        image_index = int(row.get("index") or 0)
        if image_index <= 0:
            continue
        if not _collapse(row.get("stage_hint") or ""):
            continue
        if image_index not in selected_indexes:
            selected_indexes.append(image_index)
    return selected_indexes


def _evenly_sample_indexes(indexes: Sequence[int], limit: int) -> List[int]:
    values = [int(value) for value in list(indexes or []) if int(value) > 0]
    if limit <= 0 or not values:
        return []
    if len(values) <= limit:
        return values
    if limit == 1:
        return [values[0]]
    result: List[int] = []
    last_position = len(values) - 1
    for position in range(limit):
        source_index = round(position * last_position / max(limit - 1, 1))
        candidate = values[source_index]
        if candidate not in result:
            result.append(candidate)
    for candidate in values:
        if len(result) >= limit:
            break
        if candidate not in result:
            result.append(candidate)
    return result[:limit]


def _select_openai_visual_meta(image_inputs: Sequence[Dict[str, Any]], limit: int = MAX_WORK_REPORT_OPENAI_VISUAL_IMAGES) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    meta_rows = [_openai_image_meta(index, row) for index, row in enumerate(list(image_inputs or []), start=1)]
    if len(meta_rows) <= limit:
        return meta_rows
    clusters = _cluster_openai_image_meta(meta_rows)
    cluster_candidates = [_sample_cluster_indexes(cluster) for cluster in clusters]
    selected_indexes: List[int] = []
    pass_index = 0
    while len(selected_indexes) < limit:
        pass_candidates = [
            indexes[pass_index]
            for indexes in cluster_candidates
            if pass_index < len(indexes) and int(indexes[pass_index] or 0) > 0 and int(indexes[pass_index] or 0) not in selected_indexes
        ]
        if not pass_candidates:
            if pass_index >= max((len(indexes) for indexes in cluster_candidates), default=0):
                break
            pass_index += 1
            continue
        remaining = limit - len(selected_indexes)
        for image_index in _evenly_sample_indexes(pass_candidates, remaining):
            if image_index not in selected_indexes:
                selected_indexes.append(image_index)
        pass_index += 1
    if len(selected_indexes) < limit:
        for row in meta_rows:
            image_index = int(row.get("index") or 0)
            if image_index <= 0 or image_index in selected_indexes:
                continue
            selected_indexes.append(image_index)
            if len(selected_indexes) >= limit:
                break
    selected_lookup = {int(value) for value in selected_indexes[:limit] if int(value) > 0}
    return [row for row in meta_rows if int(row.get("index") or 0) in selected_lookup]


def _chunk_rows(rows: Sequence[Any], chunk_size: int) -> List[List[Any]]:
    values = list(rows or [])
    size = max(1, int(chunk_size or 1))
    return [values[index : index + size] for index in range(0, len(values), size)]


def _sample_cluster_rows(cluster: Sequence[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    rows = list(cluster or [])
    if not rows or limit <= 0:
        return []
    lookup = {int(row.get("index") or 0): row for row in rows if int(row.get("index") or 0) > 0}
    sampled_indexes = _sample_cluster_indexes(rows)
    sampled_rows = [lookup[index] for index in sampled_indexes if index in lookup]
    if not sampled_rows:
        sampled_rows = rows[:limit]
    selected: List[Dict[str, Any]] = []
    seen_indexes: set[int] = set()
    for row in sampled_rows:
        image_index = int(row.get("index") or 0)
        if image_index <= 0 or image_index in seen_indexes:
            continue
        seen_indexes.add(image_index)
        selected.append(row)
        if len(selected) >= limit:
            break
    if len(selected) < min(limit, len(rows)):
        for row in rows:
            image_index = int(row.get("index") or 0)
            if image_index <= 0 or image_index in seen_indexes:
                continue
            seen_indexes.add(image_index)
            selected.append(row)
            if len(selected) >= limit:
                break
    return selected


def _openai_cluster_lines(entries: Sequence[Dict[str, Any]]) -> List[str]:
    rows = list(entries or [])
    if not rows:
        return []
    result: List[str] = []
    for cluster_index, cluster in enumerate(_cluster_openai_image_meta(rows), start=1):
        indexes = [f"I{int(row.get('index') or 0)}" for row in cluster if int(row.get("index") or 0) > 0]
        if not indexes:
            continue
        first = cluster[0]
        last = cluster[-1]
        if len(cluster) == 1:
            result.append(f"- C{cluster_index}: {indexes[0]} / 단일 이미지 / {first.get('filename') or '-'}")
            continue
        start_second = int(first.get("second_of_day") or -1)
        end_second = int(last.get("second_of_day") or -1)
        time_label = "-"
        if start_second >= 0 and end_second >= 0:
            start_hour = start_second // 3600
            start_minute = (start_second % 3600) // 60
            end_hour = end_second // 3600
            end_minute = (end_second % 3600) // 60
            time_label = f"{start_hour:02d}:{start_minute:02d}~{end_hour:02d}:{end_minute:02d}"
        result.append(f"- C{cluster_index}: {', '.join(indexes)} / 연속 촬영 {len(cluster)}장 / {time_label}")
    return result


def _work_report_events(text: str) -> List[Dict[str, Any]]:
    if not _collapse(text):
        return []
    return [
        event
        for event in _parse_kakao_events(text)
        if _looks_like_work_item(_collapse(event.get("text") or ""))
    ]


def _cluster_nearby_events(
    cluster: Sequence[Dict[str, Any]],
    work_events: Sequence[Dict[str, Any]],
) -> List[Tuple[int, Dict[str, Any]]]:
    rows = list(cluster or [])
    events = list(work_events or [])
    if not rows or not events:
        return []
    cluster_date = _collapse(rows[0].get("date") or "")
    cluster_minutes = [int(row.get("minute_of_day") or -1) for row in rows if int(row.get("minute_of_day") or -1) >= 0]
    center_minute = round(sum(cluster_minutes) / len(cluster_minutes)) if cluster_minutes else -1
    nearby: List[Tuple[int, Dict[str, Any]]] = []
    for event in events:
        event_date = _collapse(event.get("date") or "")
        event_minute = int(event.get("minute_of_day") or -1)
        if cluster_date and event_date and cluster_date != event_date:
            continue
        if center_minute >= 0 and event_minute >= 0:
            gap = abs(event_minute - center_minute)
            if gap > WORK_REPORT_OPENAI_CLUSTER_CONTEXT_MINUTES:
                continue
        else:
            gap = 999
        nearby.append((gap, event))
    if not nearby and cluster_date:
        for event in events:
            if _collapse(event.get("date") or "") != cluster_date:
                continue
            event_minute = int(event.get("minute_of_day") or -1)
            gap = abs(event_minute - center_minute) if center_minute >= 0 and event_minute >= 0 else 999
            nearby.append((gap, event))
    nearby.sort(key=lambda row: (int(row[0]), int(row[1].get("index") or 0)))
    return nearby


def _openai_cluster_context_lines(entries: Sequence[Dict[str, Any]], text: str) -> List[str]:
    rows = list(entries or [])
    if not rows or not _collapse(text):
        return []
    work_events = _work_report_events(text)
    if not work_events:
        return []
    result: List[str] = []
    for cluster_index, cluster in enumerate(_cluster_openai_image_meta(rows), start=1):
        nearby = _cluster_nearby_events(cluster, work_events)
        if not nearby:
            continue
        snippets: List[str] = []
        seen_titles: set[str] = set()
        for gap, event in nearby:
            title = _clean_item_title(event.get("text") or "")
            title_key = _title_key(title)
            if not title_key or title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            minute = int(event.get("minute_of_day") or -1)
            time_label = "--:--"
            if minute >= 0:
                time_label = f"{minute // 60:02d}:{minute % 60:02d}"
            snippets.append(f"{time_label} {title}")
            if len(snippets) >= 3:
                break
        if snippets:
            result.append(f"- C{cluster_index}: " + " / ".join(snippets))
    return result


def _item_hint_keywords(item: Dict[str, Any], *, limit: int = 6) -> List[str]:
    stop_words = set(WORK_REPORT_ACTION_KEYWORDS) | {
        "작업",
        "민원",
        "사항",
        "요청",
        "접수",
        "완료",
        "예정",
        "관리실",
        "시설",
        "업체",
        "현장",
        "사진",
        "이미지",
    }
    ordered: List[str] = []
    seen: set[str] = set()
    for field in (
        item.get("title"),
        item.get("location_name"),
        item.get("summary"),
        item.get("vendor_name"),
    ):
        for token in _tokenize(field):
            clean = _collapse(token).lower()
            if len(clean) < 2 or clean in stop_words or clean in seen:
                continue
            seen.add(clean)
            ordered.append(clean)
            if len(ordered) >= limit:
                return ordered
    return ordered


def _item_tokens(item: Dict[str, Any]) -> set[str]:
    fields = [
        item.get("title"),
        item.get("summary"),
        item.get("vendor_name"),
        item.get("location_name"),
        item.get("work_date"),
        item.get("work_date_label"),
    ]
    fields.extend(item.get("_attachment_notice_texts") or [])
    tokens: set[str] = set()
    for field in fields:
        tokens.update(_tokenize(field))
    return tokens


def _feedback_title_tokens(value: Any) -> set[str]:
    tokens: set[str] = set()
    for token in _tokenize(value):
        clean = _collapse(token).lower()
        if not clean or clean in WORK_REPORT_FEEDBACK_STOP_TOKENS:
            continue
        tokens.add(clean)
    return tokens


def _feedback_few_shot_lines(feedback_profile: Optional[Dict[str, Any]], *, limit: int = 4) -> List[str]:
    profile = dict(feedback_profile or {})
    examples = [dict(row) for row in list(profile.get("few_shot_examples") or []) if isinstance(row, dict)]
    if not examples:
        return []
    lines: List[str] = []
    for index, example in enumerate(examples[: max(1, int(limit or 1))], start=1):
        label = _collapse(example.get("decision_label") or example.get("feedback_type") or f"예시 {index}")
        filename = _collapse(example.get("filename") or "")
        from_title = _collapse(example.get("from_item_title") or "")
        to_title = _collapse(example.get("to_item_title") or "")
        candidate_titles = [
            _collapse(candidate.get("title") or "")
            for candidate in list(example.get("candidate_items") or [])[:3]
            if isinstance(candidate, dict) and _collapse(candidate.get("title") or "")
        ]
        parts = [f"[E{index}] {label}"]
        if filename:
            parts.append(f"파일 {filename}")
        if candidate_titles:
            parts.append("후보 " + " | ".join(candidate_titles))
        if from_title and from_title != to_title:
            parts.append(f"기존선택 {from_title}")
        if _collapse(example.get("feedback_type") or "") == "mark_unmatched":
            parts.append("사람선택 unmatched")
        elif to_title:
            parts.append(f"사람선택 {to_title}")
        review_reason = _collapse(example.get("review_reason") or "")
        if review_reason:
            parts.append(f"사유 {review_reason}")
        lines.append(" / ".join(parts))
    return lines


def _load_work_report_feedback_profile(tenant_id: str) -> Dict[str, Any]:
    clean_tenant_id = _collapse(tenant_id).lower()
    if not clean_tenant_id:
        return {
            "tenant_id": "",
            "rows_used": 0,
            "positive_tokens": {},
            "negative_tokens": {},
            "preferred_title_keys": {},
            "rejected_title_keys": {},
            "few_shot_examples": [],
        }
    try:
        from .db import list_work_report_image_feedback
        from .work_report_learning import build_feedback_few_shot_examples

        rows = list_work_report_image_feedback(tenant_id=clean_tenant_id, limit=300)
    except Exception:
        logger.exception("failed to load work report feedback profile: tenant_id=%s", clean_tenant_id)
        return {
            "tenant_id": clean_tenant_id,
            "rows_used": 0,
            "positive_tokens": {},
            "negative_tokens": {},
            "preferred_title_keys": {},
            "rejected_title_keys": {},
            "few_shot_examples": [],
        }

    positive_tokens: Dict[str, float] = {}
    negative_tokens: Dict[str, float] = {}
    preferred_title_keys: Dict[str, float] = {}
    rejected_title_keys: Dict[str, float] = {}
    rows_used = 0
    few_shot_examples = build_feedback_few_shot_examples(rows, limit=8)
    for row in rows:
        if not isinstance(row, dict):
            continue
        feedback_type = _collapse(row.get("feedback_type") or "")
        if feedback_type == "change_stage":
            continue
        review_confidence = _collapse(row.get("review_confidence") or "")
        weight = 1.0
        if feedback_type == "reassign_item":
            weight = 3.0
        elif feedback_type == "confirm_current":
            weight = 1.8
        elif feedback_type == "mark_unmatched":
            weight = 1.4
        if review_confidence == "low":
            weight *= 1.2
        elif review_confidence == "high":
            weight *= 0.9

        to_item_index = int(row.get("to_item_index") or 0)
        chosen_title_key = _title_key(row.get("to_item_title") or "")
        from_title_key = _title_key(row.get("from_item_title") or "")
        chosen_tokens = _feedback_title_tokens(row.get("to_item_title") or "")
        from_tokens = _feedback_title_tokens(row.get("from_item_title") or "")
        candidate_tokens: List[set[str]] = []
        rejected_title_candidates: List[str] = []
        try:
            candidate_items = json.loads(str(row.get("candidate_items_json") or "[]"))
        except Exception:
            candidate_items = []
        if isinstance(candidate_items, list):
            for candidate in candidate_items:
                if not isinstance(candidate, dict):
                    continue
                candidate_item_index = int(candidate.get("item_index") or 0)
                tokens = _feedback_title_tokens(candidate.get("title") or "")
                if not tokens:
                    continue
                if candidate_item_index == to_item_index and chosen_tokens:
                    continue
                candidate_tokens.append(tokens)
                candidate_title_key = _title_key(candidate.get("title") or "")
                if candidate_title_key and candidate_title_key != chosen_title_key:
                    rejected_title_candidates.append(candidate_title_key)
        rejected_tokens: set[str] = set()
        for tokens in candidate_tokens:
            rejected_tokens.update(tokens)
        if chosen_title_key:
            preferred_title_keys[chosen_title_key] = preferred_title_keys.get(chosen_title_key, 0.0) + weight
        if feedback_type == "reassign_item" and from_tokens:
            rejected_tokens.update(from_tokens)
        if feedback_type == "reassign_item" and from_title_key and from_title_key != chosen_title_key:
            rejected_title_candidates.append(from_title_key)
        if feedback_type == "mark_unmatched":
            for tokens in candidate_tokens:
                for token in tokens:
                    negative_tokens[token] = negative_tokens.get(token, 0.0) + weight
            for rejected_title_key in rejected_title_candidates:
                rejected_title_keys[rejected_title_key] = rejected_title_keys.get(rejected_title_key, 0.0) + weight
            rows_used += 1
            continue
        positive_signal = chosen_tokens - rejected_tokens if chosen_tokens else set()
        negative_signal = rejected_tokens - chosen_tokens if rejected_tokens else set()
        if not positive_signal and chosen_tokens:
            positive_signal = set(chosen_tokens)
        if chosen_tokens:
            for token in positive_signal:
                positive_tokens[token] = positive_tokens.get(token, 0.0) + weight
        for token in negative_signal:
            negative_tokens[token] = negative_tokens.get(token, 0.0) + (weight * 0.9)
        for rejected_title_key in rejected_title_candidates:
            rejected_title_keys[rejected_title_key] = rejected_title_keys.get(rejected_title_key, 0.0) + (weight * 0.9)
        rows_used += 1
    return {
        "tenant_id": clean_tenant_id,
        "rows_used": rows_used,
        "positive_tokens": positive_tokens,
        "negative_tokens": negative_tokens,
        "preferred_title_keys": preferred_title_keys,
        "rejected_title_keys": rejected_title_keys,
        "few_shot_examples": few_shot_examples,
    }


def _tenant_feedback_bonus(item: Dict[str, Any], feedback_profile: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    profile = dict(feedback_profile or {})
    if int(profile.get("rows_used") or 0) <= 0:
        return {"bonus": 0, "positive_tokens": [], "negative_tokens": []}
    item_tokens = {token for token in _item_tokens(item) if token not in WORK_REPORT_FEEDBACK_STOP_TOKENS}
    if not item_tokens:
        return {"bonus": 0, "positive_tokens": [], "negative_tokens": []}
    positive_map = dict(profile.get("positive_tokens") or {})
    negative_map = dict(profile.get("negative_tokens") or {})
    positive_hits = sorted((token for token in item_tokens if float(positive_map.get(token) or 0.0) > 0.0), key=lambda token: (-float(positive_map.get(token) or 0.0), token))
    negative_hits = sorted((token for token in item_tokens if float(negative_map.get(token) or 0.0) > 0.0), key=lambda token: (-float(negative_map.get(token) or 0.0), token))
    raw_score = sum(float(positive_map.get(token) or 0.0) for token in positive_hits[:4]) - sum(float(negative_map.get(token) or 0.0) for token in negative_hits[:4])
    if raw_score >= 6.0:
        bonus = 4
    elif raw_score >= 3.0:
        bonus = 2
    elif raw_score <= -5.0:
        bonus = -3
    elif raw_score <= -2.0:
        bonus = -1
    else:
        bonus = 0
    return {
        "bonus": bonus,
        "positive_tokens": positive_hits[:3],
        "negative_tokens": negative_hits[:3],
    }


def _tenant_feedback_title_bonus(item: Dict[str, Any], feedback_profile: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    profile = dict(feedback_profile or {})
    if int(profile.get("rows_used") or 0) <= 0:
        return {"bonus": 0, "preferred_weight": 0.0, "rejected_weight": 0.0}
    item_title_key = _title_key(item.get("title") or "")
    if not item_title_key:
        return {"bonus": 0, "preferred_weight": 0.0, "rejected_weight": 0.0}
    preferred_map = dict(profile.get("preferred_title_keys") or {})
    rejected_map = dict(profile.get("rejected_title_keys") or {})
    preferred_weight = float(preferred_map.get(item_title_key) or 0.0)
    rejected_weight = float(rejected_map.get(item_title_key) or 0.0)
    raw_score = preferred_weight - rejected_weight
    if raw_score >= 6.0:
        bonus = 8
    elif raw_score >= 3.0:
        bonus = 5
    elif raw_score <= -5.0:
        bonus = -6
    elif raw_score <= -2.0:
        bonus = -3
    else:
        bonus = 0
    return {
        "bonus": bonus,
        "preferred_weight": preferred_weight,
        "rejected_weight": rejected_weight,
    }


def _entry_tokens(entry: Dict[str, Any]) -> set[str]:
    preview_text = _collapse(entry.get("preview_text") or "")
    metadata = entry.get("metadata") or {}
    is_image_like_entry = not preview_text and not metadata
    tokens = set()
    for token in _tokenize(entry.get("filename")):
        clean = _collapse(token).lower()
        if is_image_like_entry and clean.isdigit():
            continue
        if is_image_like_entry and clean in {"kakaotalk", "img", "image", "jpg", "jpeg", "png"}:
            continue
        tokens.add(clean)
    if preview_text:
        tokens.update(_tokenize(preview_text[:200]))
    if isinstance(metadata, dict):
        for field in (
            metadata.get("title"),
            metadata.get("vendor_name"),
            metadata.get("location_name"),
            metadata.get("work_date"),
            metadata.get("summary"),
        ):
            tokens.update(_tokenize(field))
    return tokens


def _item_reference_minutes(item: Dict[str, Any]) -> List[int]:
    minutes: List[int] = []
    item_minute = int(item.get("_minute_of_day") or -1)
    if item_minute >= 0:
        minutes.append(item_minute)
    for notice in list(item.get("_image_notices") or []):
        notice_minute = int((notice or {}).get("minute_of_day") or -1)
        if notice_minute >= 0:
            minutes.append(notice_minute)
    seen: set[int] = set()
    ordered: List[int] = []
    for minute in minutes:
        if minute in seen:
            continue
        seen.add(minute)
        ordered.append(minute)
    return ordered


def _temporal_match_details(item: Dict[str, Any], entry: Dict[str, Any]) -> Dict[str, Any]:
    item_date = _collapse(item.get("work_date") or "")
    entry_date = _collapse(entry.get("date") or "")
    if item_date and entry_date and item_date != entry_date:
        return {"score": 0, "gap_minutes": -1, "matched_date": False}
    entry_minute = int(entry.get("minute_of_day") or -1)
    if entry_minute < 0:
        return {"score": 0, "gap_minutes": -1, "matched_date": True}
    reference_minutes = _item_reference_minutes(item)
    if not reference_minutes:
        return {"score": 0, "gap_minutes": -1, "matched_date": True}
    gap = min(abs(entry_minute - minute) for minute in reference_minutes)
    if gap <= 3:
        score = 8
    elif gap <= 8:
        score = 5
    elif gap <= 15:
        score = 2
    else:
        score = 0
    return {
        "score": score,
        "gap_minutes": gap,
        "matched_date": True,
    }


def _temporal_match_score(item: Dict[str, Any], entry: Dict[str, Any]) -> int:
    return int(_temporal_match_details(item, entry).get("score") or 0)


def _match_score_breakdown(
    item: Dict[str, Any],
    entry: Dict[str, Any],
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    item_token_set = _item_tokens(item)
    entry_token_set = _entry_tokens(entry)
    token_overlap = len(item_token_set & entry_token_set)
    temporal = _temporal_match_details(item, entry)
    base_score = token_overlap + int(temporal.get("score") or 0)
    result: Dict[str, Any] = {
        "token_overlap": token_overlap,
        "temporal_score": int(temporal.get("score") or 0),
        "gap_minutes": int(temporal.get("gap_minutes") or -1),
        "date_bonus": 0,
        "month_day_bonus": 0,
        "vendor_bonus": 0,
        "feedback_bonus": 0,
        "feedback_title_bonus": 0,
        "feedback_positive_tokens": [],
        "feedback_negative_tokens": [],
        "score": 0,
    }
    if base_score <= 0:
        return result
    score = base_score
    item_date = str(item.get("work_date") or "")
    filename = str(entry.get("filename") or "")
    if item_date:
        compact = item_date.replace("-", "")
        if compact in filename:
            score += 3
            result["date_bonus"] = 3
        month_day = item_date[5:]
        if month_day and month_day.replace("-", "") in filename:
            score += 2
            result["month_day_bonus"] = 2
    vendor = _collapse(item.get("vendor_name") or "")
    if vendor and vendor in _collapse(filename):
        score += 2
        result["vendor_bonus"] = 2
    feedback = _tenant_feedback_bonus(item, feedback_profile)
    result["feedback_bonus"] = int(feedback.get("bonus") or 0)
    result["feedback_positive_tokens"] = list(feedback.get("positive_tokens") or [])
    result["feedback_negative_tokens"] = list(feedback.get("negative_tokens") or [])
    score += int(feedback.get("bonus") or 0)
    title_feedback = _tenant_feedback_title_bonus(item, feedback_profile)
    result["feedback_title_bonus"] = int(title_feedback.get("bonus") or 0)
    score += int(title_feedback.get("bonus") or 0)
    result["score"] = score
    return result


def _match_score(
    item: Dict[str, Any],
    entry: Dict[str, Any],
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> int:
    return int(_match_score_breakdown(item, entry, feedback_profile=feedback_profile).get("score") or 0)


def _image_candidate_reason_parts(breakdown: Dict[str, Any]) -> List[str]:
    parts: List[str] = []
    token_overlap = int(breakdown.get("token_overlap") or 0)
    if token_overlap > 0:
        parts.append(f"키워드 {token_overlap}개 일치")
    temporal_score = int(breakdown.get("temporal_score") or 0)
    gap_minutes = int(breakdown.get("gap_minutes") or -1)
    if temporal_score > 0 and gap_minutes >= 0:
        parts.append(f"촬영 시각 {gap_minutes}분 차이")
    if int(breakdown.get("date_bonus") or 0) > 0:
        parts.append("파일명 날짜 일치")
    elif int(breakdown.get("month_day_bonus") or 0) > 0:
        parts.append("파일명 월/일 단서 일치")
    if int(breakdown.get("vendor_bonus") or 0) > 0:
        parts.append("업체명 단서 일치")
    if int(breakdown.get("feedback_bonus") or 0) > 0:
        parts.append("누적 피드백 보정")
    if int(breakdown.get("feedback_title_bonus") or 0) > 0:
        parts.append("사람 확정 작업 재학습")
    if not parts:
        parts.append("명확한 단서는 약함")
    return parts


def _image_match_confidence(top_score: int, second_score: int) -> str:
    gap = max(0, int(top_score) - int(second_score))
    if top_score >= 10 and gap >= 4:
        return "high"
    if top_score >= 6 and gap >= 3:
        return "medium"
    return "low"


def _image_candidate_matches(
    items: Sequence[Dict[str, Any]],
    entry: Dict[str, Any],
    *,
    limit: int = 3,
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for item in list(items or []):
        item_index = int(item.get("index") or 0)
        if item_index <= 0:
            continue
        breakdown = _match_score_breakdown(item, entry, feedback_profile=feedback_profile)
        score = int(breakdown.get("score") or 0)
        if score <= 0:
            continue
        ranked.append(
            {
                "item_index": item_index,
                "title": _collapse(item.get("title") or ""),
                "location_name": _collapse(item.get("location_name") or ""),
                "work_date_label": _collapse(item.get("work_date_label") or item.get("work_date") or ""),
                "score": score,
                "reason_parts": _image_candidate_reason_parts(breakdown),
                "reason_text": " / ".join(_image_candidate_reason_parts(breakdown)),
            }
        )
    ranked.sort(key=lambda row: (-int(row.get("score") or 0), int(row.get("item_index") or 0)))
    top_rows = ranked[: max(1, int(limit or 1))]
    for position, row in enumerate(top_rows):
        row["rank"] = position + 1
        next_score = int(top_rows[position + 1].get("score") or 0) if position + 1 < len(top_rows) else 0
        row["confidence"] = _image_match_confidence(int(row.get("score") or 0), next_score)
    return top_rows


def _image_review_decision(current_item_index: int, candidates: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidates:
        return {"needed": False, "reason": "", "confidence": ""}
    top = dict(candidates[0])
    top_score = int(top.get("score") or 0)
    second_score = int(candidates[1].get("score") or 0) if len(candidates) > 1 else 0
    top_item_index = int(top.get("item_index") or 0)
    gap = top_score - second_score
    confidence = _image_match_confidence(top_score, second_score)
    if current_item_index <= 0 and top_score >= 3:
        return {"needed": True, "reason": "미매칭으로 남았지만 추천 후보가 있습니다.", "confidence": confidence}
    if current_item_index > 0 and top_item_index > 0 and top_item_index != current_item_index and top_score >= max(3, second_score):
        return {"needed": True, "reason": "현재 매칭과 추천 1순위가 다릅니다.", "confidence": confidence}
    if second_score > 0 and gap <= 2:
        return {"needed": True, "reason": "후보 1순위와 2순위 점수 차가 작습니다.", "confidence": "low"}
    if top_score <= 4:
        return {"needed": True, "reason": "매칭 점수가 낮아 확신이 낮습니다.", "confidence": "low"}
    return {"needed": False, "reason": "", "confidence": confidence}


def _best_item_index_for_cluster(
    items: Sequence[Dict[str, Any]],
    cluster: Sequence[Dict[str, Any]],
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> int:
    best_item_index = 0
    best_score = 0
    for item in list(items or []):
        item_index = int(item.get("index") or 0)
        if item_index <= 0:
            continue
        score = sum(_match_score(item, row, feedback_profile=feedback_profile) for row in list(cluster or []))
        if score > best_score:
            best_score = score
            best_item_index = item_index
    return best_item_index if best_score > 0 else 0


def _cluster_item_candidate_lines(
    cluster: Sequence[Dict[str, Any]],
    items: Sequence[Dict[str, Any]],
    work_events: Sequence[Dict[str, Any]],
    *,
    limit: int = 3,
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> List[str]:
    ranked = _cluster_ranked_item_candidates(
        cluster,
        items,
        work_events,
        feedback_profile=feedback_profile,
    )
    lines: List[str] = []
    for score, item_index, title, location, keywords in ranked[:limit]:
        parts = [f"T{item_index} {title or '-'}", f"점수 {score}"]
        if location:
            parts.append(f"위치 {location}")
        if keywords:
            parts.append("키워드 " + ", ".join(keywords[:4]))
        lines.append(" / ".join(parts))
    return lines


def _cluster_ranked_item_candidates(
    cluster: Sequence[Dict[str, Any]],
    items: Sequence[Dict[str, Any]],
    work_events: Sequence[Dict[str, Any]],
    *,
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> List[Tuple[int, int, str, str, List[str]]]:
    nearby = _cluster_nearby_events(cluster, work_events)[:4]
    ranked: List[Tuple[int, int, str, str, List[str]]] = []
    for item in list(items or []):
        item_index = int(item.get("index") or 0)
        if item_index <= 0:
            continue
        title = _collapse(item.get("title") or "")
        location = _collapse(item.get("location_name") or "")
        keywords = _item_hint_keywords(item, limit=5)
        score = sum(_match_score(item, row, feedback_profile=feedback_profile) for row in list(cluster or []))
        for _, event in nearby:
            event_text = _collapse(event.get("text") or "")
            if not event_text:
                continue
            if _titles_match(title, event_text):
                score += 8
            else:
                score += min(4, len(_title_tokens(title) & _title_tokens(event_text)) * 2)
            if location:
                score += min(4, len(set(_tokenize(location)) & set(_tokenize(event_text))) * 2)
            if keywords:
                score += min(3, len(set(keywords) & set(_tokenize(event_text))))
        if score > 0:
            ranked.append((score, item_index, title, location, keywords))
    ranked.sort(key=lambda row: (-row[0], row[1]))
    return ranked


def _batch_candidate_items(
    clusters: Sequence[Sequence[Dict[str, Any]]],
    items: Sequence[Dict[str, Any]],
    work_events: Sequence[Dict[str, Any]],
    *,
    per_cluster_limit: int = 4,
    total_limit: int = 18,
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    indexed_items: Dict[int, Dict[str, Any]] = {
        int(item.get("index") or 0): dict(item)
        for item in list(items or [])
        if int(item.get("index") or 0) > 0
    }
    if not indexed_items:
        return []
    score_map: Dict[int, int] = {}
    for cluster in list(clusters or []):
        ranked = _cluster_ranked_item_candidates(
            cluster,
            items,
            work_events,
            feedback_profile=feedback_profile,
        )
        for score, item_index, _title, _location, _keywords in ranked[: max(1, int(per_cluster_limit or 0))]:
            score_map[item_index] = score_map.get(item_index, 0) + int(score or 0)
        best_item_index = _best_item_index_for_cluster(items, cluster, feedback_profile=feedback_profile)
        if best_item_index > 0:
            score_map[best_item_index] = max(score_map.get(best_item_index, 0), 1)
    if not score_map:
        return [indexed_items[index] for index in sorted(indexed_items)]
    ranked_indexes = [
        item_index
        for item_index, _score in sorted(score_map.items(), key=lambda row: (-row[1], row[0]))[: max(1, int(total_limit or 0))]
        if item_index in indexed_items
    ]
    if not ranked_indexes:
        return [indexed_items[index] for index in sorted(indexed_items)]
    return [indexed_items[item_index] for item_index in ranked_indexes]


def _chunk_assign(entries: List[Dict[str, Any]], items: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    assigned: Dict[int, List[Dict[str, Any]]] = {int(item["index"]): [] for item in items}
    if not items:
        return assigned
    total_entries = len(entries)
    total_items = len(items)
    for position, entry in enumerate(entries):
        target_pos = min(int(position * total_items / max(total_entries, 1)), total_items - 1)
        assigned[int(items[target_pos]["index"])].append(entry)
    return assigned


def _assign_entries(
    items: List[Dict[str, Any]],
    entries: List[Dict[str, Any]],
    *,
    allow_chunk_fallback: bool = True,
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> Dict[int, List[Dict[str, Any]]]:
    assigned: Dict[int, List[Dict[str, Any]]] = {int(item["index"]): [] for item in items}
    unmatched: List[Dict[str, Any]] = []
    for entry in entries:
        best_item = None
        best_score = 0
        for item in items:
            score = _match_score(item, entry, feedback_profile=feedback_profile)
            if score > best_score:
                best_score = score
                best_item = item
        if best_item and best_score > 0:
            assigned[int(best_item["index"])].append(entry)
        else:
            unmatched.append(entry)
    if unmatched and allow_chunk_fallback:
        fallback = _chunk_assign(unmatched, items)
        for item_index, rows in fallback.items():
            assigned[item_index].extend(rows)
    return assigned


def _assign_images_by_notices(
    items: Sequence[Dict[str, Any]],
    entries: Sequence[Dict[str, Any]],
    *,
    max_gap_minutes: int = 10,
    cluster_gap_seconds: int = 180,
) -> Tuple[Dict[int, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    assigned: Dict[int, List[Dict[str, Any]]] = {int(item["index"]): [] for item in items}
    remaining = sorted(
        list(entries),
        key=lambda row: (
            int(row.get("second_of_day") or -1) if int(row.get("second_of_day") or -1) >= 0 else 10**9,
            int(row.get("index") or 0),
        ),
    )
    notice_rows: List[Dict[str, Any]] = []
    for item in items:
        for order, notice in enumerate(item.get("_image_notices") or [], start=1):
            notice_rows.append(
                {
                    "item_index": int(item["index"]),
                    "date": _collapse(notice.get("date") or item.get("work_date") or ""),
                    "minute_of_day": int(notice.get("minute_of_day") or -1),
                    "second_of_day": int(notice.get("second_of_day") or -1),
                    "count": max(1, int(notice.get("count") or 1)),
                    "order": order,
                }
            )
    notice_rows.sort(
        key=lambda row: (
            str(row.get("date") or ""),
            int(row.get("second_of_day") or -1) if int(row.get("second_of_day") or -1) >= 0 else 10**9,
            int(row.get("item_index") or 0),
            int(row.get("order") or 0),
        )
    )
    for notice in notice_rows:
        if not remaining:
            break
        requested = max(1, int(notice.get("count") or 1))
        notice_minute = int(notice.get("minute_of_day") or -1)
        notice_second = int(notice.get("second_of_day") or -1)
        notice_date = _collapse(notice.get("date") or "")
        timed_exists = any(int(row.get("second_of_day") or -1) >= 0 for row in remaining)
        if notice_minute < 0 or not timed_exists:
            take = min(requested, len(remaining))
            if take > 0:
                assigned[int(notice["item_index"])].extend(remaining[:take])
                del remaining[:take]
            continue

        candidate_positions: List[int] = []
        for position, row in enumerate(remaining):
            entry_second = int(row.get("second_of_day") or -1)
            entry_minute = int(row.get("minute_of_day") or -1)
            entry_date = _collapse(row.get("date") or "")
            if entry_second < 0 or entry_minute < 0:
                continue
            if notice_date and entry_date and entry_date != notice_date:
                continue
            if abs(entry_minute - notice_minute) <= max_gap_minutes:
                candidate_positions.append(position)
        if not candidate_positions:
            continue

        anchor_pos = min(
            candidate_positions,
            key=lambda position: (
                abs(int(remaining[position].get("second_of_day") or -1) - notice_second),
                int(remaining[position].get("index") or 0),
            ),
        )
        anchor_second = int(remaining[anchor_pos].get("second_of_day") or -1)
        anchor_date = _collapse(remaining[anchor_pos].get("date") or "")
        selected_positions = [anchor_pos]
        for position in range(anchor_pos + 1, len(remaining)):
            if len(selected_positions) >= requested:
                break
            row = remaining[position]
            entry_second = int(row.get("second_of_day") or -1)
            entry_date = _collapse(row.get("date") or "")
            if entry_second < 0:
                break
            if anchor_date and entry_date and entry_date != anchor_date:
                break
            if abs(entry_second - anchor_second) > cluster_gap_seconds:
                break
            selected_positions.append(position)
        if len(selected_positions) < requested:
            for position in range(anchor_pos - 1, -1, -1):
                if len(selected_positions) >= requested:
                    break
                row = remaining[position]
                entry_second = int(row.get("second_of_day") or -1)
                entry_date = _collapse(row.get("date") or "")
                if entry_second < 0:
                    continue
                if anchor_date and entry_date and entry_date != anchor_date:
                    continue
                if abs(entry_second - anchor_second) <= cluster_gap_seconds:
                    selected_positions.append(position)
        chosen_positions = sorted(set(selected_positions[:requested]))
        chosen_rows = [remaining[position] for position in chosen_positions]
        for position in reversed(chosen_positions):
            del remaining[position]
        assigned[int(notice["item_index"])].extend(chosen_rows)
    return assigned, remaining


def _occurrence_stage(total: int, position: int) -> str:
    if total <= 1:
        return ""
    if total == 2:
        return "before" if position == 0 else "after"
    if position == 0:
        return "before"
    if position == total - 1:
        return "after"
    return "during"


def _merge_explicit_items(
    explicit_items: Sequence[Dict[str, Any]],
    image_matches: Dict[int, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen_titles: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in explicit_items:
        key = (_collapse(item.get("title") or ""), _collapse(item.get("work_date") or ""))
        occurrence_images = sorted(image_matches.get(int(item["index"]), []), key=lambda row: int(row.get("index") or 0))
        existing = seen_titles.get(key)
        if existing:
            existing["_expected_attachment_count"] = int(existing.get("_expected_attachment_count") or 0) + int(item.get("_expected_attachment_count") or 0)
            existing["_attachment_notice_texts"].extend(list(item.get("_attachment_notice_texts") or []))
            existing["_attachment_notice_tokens"].extend(list(item.get("_attachment_notice_tokens") or []))
            existing["_occurrence_images"].append(occurrence_images)
            if not _collapse(existing.get("summary") or "") or _collapse(existing.get("summary") or "") == _collapse(existing.get("title") or ""):
                if _collapse(item.get("summary") or ""):
                    existing["summary"] = _collapse(item.get("summary") or "")
            elif _collapse(item.get("summary") or "") and _collapse(item.get("summary") or "") not in _collapse(existing.get("summary") or ""):
                existing["summary"] = f"{_collapse(existing.get('summary') or '')} / {_collapse(item.get('summary') or '')}"[:240]
            if not existing.get("vendor_name") and _collapse(item.get("vendor_name") or ""):
                existing["vendor_name"] = _collapse(item.get("vendor_name") or "")
            if not existing.get("location_name") and _collapse(item.get("location_name") or ""):
                existing["location_name"] = _collapse(item.get("location_name") or "")
            existing["_minute_of_day"] = max(int(existing.get("_minute_of_day") or -1), int(item.get("_minute_of_day") or -1))
            continue
        item_copy = dict(item)
        item_copy["index"] = len(merged) + 1
        item_copy["_occurrence_images"] = [occurrence_images]
        merged.append(item_copy)
        seen_titles[key] = item_copy

    for item in merged:
        occurrence_images = list(item.pop("_occurrence_images", []))
        if len(occurrence_images) > 1:
            for position, rows in enumerate(occurrence_images):
                inferred_stage = _occurrence_stage(len(occurrence_images), position)
                for row in rows:
                    if not row.get("stage") and inferred_stage:
                        row["stage"] = inferred_stage
        combined_images = [row for rows in occurrence_images for row in rows]
        item["images"] = _finalize_image_stages(combined_images)
    return merged


def _assign_entries_by_expected_counts(
    items: Sequence[Dict[str, Any]], entries: Sequence[Dict[str, Any]], expected_key: str
) -> Tuple[Dict[int, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    assigned: Dict[int, List[Dict[str, Any]]] = {int(item["index"]): [] for item in items}
    remaining = list(entries)
    if not items or not remaining:
        return assigned, remaining
    cursor = 0
    for item in items:
        expected = max(0, int(item.get(expected_key) or 0))
        if expected <= 0 or cursor >= len(remaining):
            continue
        take = min(expected, len(remaining) - cursor)
        if take > 0:
            assigned[int(item["index"])].extend(remaining[cursor : cursor + take])
            cursor += take
    return assigned, remaining[cursor:]


def _assign_entries_by_notice_tokens(
    items: Sequence[Dict[str, Any]], entries: Sequence[Dict[str, Any]], token_key: str
) -> Tuple[Dict[int, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    assigned: Dict[int, List[Dict[str, Any]]] = {int(item["index"]): [] for item in items}
    token_rows: List[Tuple[int, int]] = []
    for item in items:
        for token in item.get(token_key) or []:
            try:
                token_rows.append((int(token), int(item["index"])))
            except Exception:
                continue
    token_rows.sort(key=lambda row: row[0])
    remaining = list(entries)
    cursor = 0
    for _, item_index in token_rows:
        if cursor >= len(remaining):
            break
        assigned[item_index].append(remaining[cursor])
        cursor += 1
    return assigned, remaining[cursor:]


def _finalize_image_stages(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = sorted(entries, key=lambda row: int(row.get("index") or 0))
    undecided = [row for row in ordered if not row.get("stage")]
    if len(undecided) == 1:
        undecided[0]["stage"] = "general"
    elif len(undecided) == 2:
        undecided[0]["stage"] = "before"
        undecided[1]["stage"] = "after"
    elif len(undecided) >= 3:
        undecided[0]["stage"] = "before"
        undecided[-1]["stage"] = "after"
        for row in undecided[1:-1]:
            row["stage"] = "during"
    for row in ordered:
        stage = str(row.get("stage") or "general")
        row["stage"] = stage
        row["stage_label"] = _stage_label(stage)
    return ordered


def _openai_item_summary_lines(items: Sequence[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    for item in list(items or []):
        item_index = int(item.get("index") or 0)
        if item_index <= 0:
            continue
        lines.append(
            " / ".join(
                [
                    f"[T{item_index}] {item.get('title') or '-'}",
                    f"일자 {_collapse(item.get('work_date_label') or item.get('work_date') or '-')}",
                    f"위치 {_collapse(item.get('location_name') or '-')}",
                    f"업체 {_collapse(item.get('vendor_name') or '-')}",
                    f"요약 {_collapse(item.get('summary') or item.get('title') or '-')[:120]}",
                    f"키워드 {', '.join(_item_hint_keywords(item)) or '-'}",
                ]
            )
        )
    return lines


def _apply_image_matches_to_items(
    items: Sequence[Dict[str, Any]],
    assigned_rows: Dict[int, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    normalized_items: List[Dict[str, Any]] = []
    for item in list(items or []):
        clone = dict(item)
        image_rows: List[Dict[str, Any]] = []
        for row in assigned_rows.get(int(clone.get("index") or 0), []):
            image_rows.append(
                {
                    "index": int(row.get("index") or 0),
                    "filename": _collapse(row.get("filename") or ""),
                    "preview_available": bool(row.get("preview_available")),
                    "stage": _collapse(row.get("stage") or row.get("stage_hint") or ""),
                }
            )
        clone["images"] = _finalize_image_stages(image_rows)
        normalized_items.append(clone)
    return normalized_items


def _attach_image_review_metadata(
    items: Sequence[Dict[str, Any]],
    unmatched_images: Sequence[Dict[str, Any]],
    image_entries: Sequence[Dict[str, Any]],
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    entry_lookup = {int(row.get("index") or 0): dict(row) for row in list(image_entries or []) if int(row.get("index") or 0) > 0}
    image_row_lookup: Dict[int, Tuple[Dict[str, Any], int, str]] = {}
    for item in list(items or []):
        item_index = int(item.get("index") or 0)
        item_title = _collapse(item.get("title") or "")
        for image in list(item.get("images") or []):
            image_index = int(image.get("index") or 0)
            if image_index > 0:
                image_row_lookup[image_index] = (image, item_index, item_title)
    for image in list(unmatched_images or []):
        image_index = int(image.get("index") or 0)
        if image_index > 0:
            image_row_lookup[image_index] = (image, 0, "미매칭")

    review_queue: List[Dict[str, Any]] = []
    for image_index, (row, current_item_index, current_item_title) in image_row_lookup.items():
        entry = entry_lookup.get(int(image_index))
        if not entry:
            continue
        candidates = _image_candidate_matches(items, entry, limit=3, feedback_profile=feedback_profile)
        decision = _image_review_decision(current_item_index, candidates)
        row["review_candidates"] = candidates
        row["review_needed"] = bool(decision.get("needed"))
        row["review_reason"] = _collapse(decision.get("reason") or "")
        row["review_confidence"] = _collapse(decision.get("confidence") or "")
        row["review_current_item_index"] = current_item_index
        row["review_current_item_title"] = current_item_title or "미매칭"
        if candidates:
            row["review_recommended_item_index"] = int(candidates[0].get("item_index") or 0)
            row["review_recommended_item_title"] = _collapse(candidates[0].get("title") or "")
        else:
            row["review_recommended_item_index"] = 0
            row["review_recommended_item_title"] = ""
        if row["review_needed"]:
            review_queue.append(
                {
                    "image_index": image_index,
                    "filename": _collapse(row.get("filename") or entry.get("filename") or ""),
                    "current_item_index": current_item_index,
                    "current_item_title": current_item_title or "미매칭",
                    "review_reason": _collapse(row.get("review_reason") or ""),
                    "review_confidence": _collapse(row.get("review_confidence") or ""),
                    "candidate_items": candidates,
                }
            )
    review_queue.sort(key=lambda row: (0 if _collapse(row.get("review_confidence") or "") == "low" else 1, int(row.get("image_index") or 0)))
    return review_queue


def _build_text_summary(report_title: str, period_label: str, items: List[Dict[str, Any]], unmatched_images: List[Dict[str, Any]], unmatched_attachments: List[Dict[str, Any]], analysis_notice: str) -> str:
    image_items = [item for item in items if item.get("images")]
    text_only_items = [item for item in items if not item.get("images")]
    lines = [
        report_title or "시설팀 주요 업무 보고",
        f"보고기간: {period_label or '-'}",
        f"작업 항목 수: {len(items)}",
        f"사진 포함 작업: {len(image_items)}",
        f"텍스트 전용 작업: {len(text_only_items)}",
        f"미매칭 이미지: {len(unmatched_images)}",
        f"미매칭 첨부파일: {len(unmatched_attachments)}",
    ]
    if analysis_notice:
        lines.extend(["", f"안내: {analysis_notice}"])

    def append_items(section_title: str, rows: Sequence[Dict[str, Any]]) -> None:
        if not rows:
            return
        lines.extend(["", section_title])
        for item in rows:
            lines.extend(
                [
                    f"{int(item.get('index') or 0)}. {item.get('title') or '-'}",
                    f"- 작업일자: {item.get('work_date_label') or item.get('work_date') or '-'}",
                    f"- 작업자: {item.get('vendor_name') or '-'}",
                    f"- 위치: {item.get('location_name') or '-'}",
                    f"- 이미지: {len(item.get('images') or [])}장",
                    f"- 첨부파일: {len(item.get('attachments') or [])}건",
                ]
            )

    append_items("[사진 포함 작업]", image_items)
    append_items("[텍스트 전용 작업]", text_only_items)
    return "\n".join(lines)


def _normalize_existing_work_report_items(base_report: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(base_report, dict):
        return []
    rows = list(base_report.get("items") or []) if isinstance(base_report.get("items"), list) else []
    items: List[Dict[str, Any]] = []
    seen_indexes: set[int] = set()
    for position, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        item_index = int(row.get("index") or 0)
        if item_index <= 0 or item_index in seen_indexes:
            item_index = position
            while item_index in seen_indexes:
                item_index += 1
        seen_indexes.add(item_index)
        attachments: List[Dict[str, Any]] = []
        for attachment in list(row.get("attachments") or []):
            if not isinstance(attachment, dict):
                continue
            attachments.append(
                {
                    "index": int(attachment.get("index") or 0),
                    "filename": _collapse(attachment.get("filename") or ""),
                    "preview_text": _collapse(attachment.get("preview_text") or ""),
                }
            )
        items.append(
            {
                "index": item_index,
                "title": _clean_item_title(row.get("title") or row.get("summary") or ""),
                "work_date": _collapse(row.get("work_date") or ""),
                "work_date_label": _collapse(row.get("work_date_label") or ""),
                "vendor_name": _collapse(row.get("vendor_name") or ""),
                "location_name": _collapse(row.get("location_name") or ""),
                "summary": _collapse(row.get("summary") or row.get("title") or ""),
                "confidence": _collapse(row.get("confidence") or "manual"),
                "images": [],
                "attachments": attachments,
            }
        )
    return items


def _normalize_existing_unmatched_attachments(base_report: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(base_report, dict):
        return []
    rows = list(base_report.get("unmatched_attachments") or []) if isinstance(base_report.get("unmatched_attachments"), list) else []
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "index": int(row.get("index") or 0),
                "filename": _collapse(row.get("filename") or ""),
            }
        )
    return normalized


def _finalize_work_report_result(
    *,
    normalized_text: str,
    sample_title: str,
    report_title: str,
    period_label: str,
    items: List[Dict[str, Any]],
    images: Sequence[Dict[str, Any]],
    attachments: Sequence[Dict[str, Any]],
    image_entries: Sequence[Dict[str, Any]],
    unmatched_image_indexes: Sequence[int],
    unmatched_attachment_indexes: Sequence[int],
    analysis_notice: str,
    analysis_model: str,
    analysis_reason: str,
    analysis_diagnostics: Dict[str, Any],
    openai_failures: Sequence[Dict[str, str]],
    tenant_feedback_profile: Optional[Dict[str, Any]] = None,
    analysis_stage: str = "image_matched",
    selected_image_item_indexes: Sequence[int] | None = None,
    unmatched_attachments_override: Sequence[Dict[str, Any]] | None = None,
    use_chunked_image_matching: bool = False,
    reference_images: Sequence[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    dated_items = [item for item in items if item.get("work_date")]
    if dated_items:
        dates = sorted(item["work_date"] for item in dated_items)
        if len(dates) == 1:
            month = int(dates[0][5:7])
            day = int(dates[0][8:10])
            period_label = f"{month}월 {day}일"
        else:
            start_month = int(dates[0][5:7])
            start_day = int(dates[0][8:10])
            end_month = int(dates[-1][5:7])
            end_day = int(dates[-1][8:10])
            period_label = f"{start_month}월 {start_day}일 ~ {end_month}월 {end_day}일"

    unmatched_images = [
        {
            "index": index,
            "filename": images[index - 1].get("filename"),
            "preview_available": bool(_collapse(images[index - 1].get("preview_relative_path") or "")),
            "stage": _collapse(image_entries[index - 1].get("stage_hint") or "") or "general",
            "stage_label": _stage_label(_collapse(image_entries[index - 1].get("stage_hint") or "") or "general"),
        }
        for index in unmatched_image_indexes
        if 0 < int(index) <= len(images)
    ]
    if unmatched_attachments_override is None:
        unmatched_attachments = [
            {"index": index, "filename": attachments[index - 1].get("filename")}
            for index in unmatched_attachment_indexes
            if 0 < int(index) <= len(attachments)
        ]
    else:
        unmatched_attachments = [dict(row) for row in list(unmatched_attachments_override or [])]

    review_queue: List[Dict[str, Any]] = []
    if analysis_stage != "extract_only":
        review_queue = _attach_image_review_metadata(items, unmatched_images, image_entries, feedback_profile=tenant_feedback_profile)
    for item in items:
        item.pop("_minute_of_day", None)
    image_items = [item for item in items if item.get("images")]
    text_only_items = [item for item in items if not item.get("images")]
    report_text = _build_text_summary(report_title, period_label, items, unmatched_images, unmatched_attachments, analysis_notice)
    analysis_reason_label = _analysis_reason_label(analysis_reason)
    analysis_mode_label = _analysis_mode_label(analysis_model, analysis_reason)
    final_diagnostics = dict(analysis_diagnostics or {})
    final_diagnostics["openai_failures"] = list(openai_failures or [])
    final_diagnostics["final_model"] = analysis_model
    final_diagnostics["final_reason"] = analysis_reason
    final_diagnostics["review_queue_count"] = len(review_queue)
    final_diagnostics["analysis_stage"] = analysis_stage
    final_diagnostics["selected_image_item_count"] = len([value for value in list(selected_image_item_indexes or []) if int(value) > 0])
    if analysis_reason or analysis_model == "heuristic":
        logger.warning(
            "work report analysis fallback: stage=%s model=%s reason=%s items=%s images=%s ref_images=%s attachments=%s chunked=%s failures=%s",
            analysis_stage or "-",
            analysis_model or "-",
            analysis_reason or "-",
            len(items),
            len(images),
            len(list(reference_images or [])),
            len(attachments),
            use_chunked_image_matching,
            len(list(openai_failures or [])),
        )
    elif use_chunked_image_matching:
        logger.info(
            "work report analysis completed: stage=%s model=%s items=%s images=%s ref_images=%s attachments=%s chunked=%s",
            analysis_stage or "-",
            analysis_model or "-",
            len(items),
            len(images),
            len(list(reference_images or [])),
            len(attachments),
            use_chunked_image_matching,
        )
    return {
        "report_title": report_title,
        "period_label": period_label,
        "template_title": _collapse(sample_title) or report_title,
        "analysis_model": analysis_model,
        "analysis_mode_label": analysis_mode_label,
        "analysis_reason": analysis_reason,
        "analysis_reason_label": analysis_reason_label,
        "analysis_notice": analysis_notice,
        "analysis_diagnostics": final_diagnostics,
        "analysis_stage": analysis_stage,
        "image_selection_required": analysis_stage == "extract_only" and bool(images),
        "selected_image_item_indexes": sorted({int(value) for value in list(selected_image_item_indexes or []) if int(value) > 0}),
        "image_input_count": len(images),
        "item_count": len(items),
        "image_item_count": len(image_items),
        "text_only_item_count": len(text_only_items),
        "items": items,
        "image_items": image_items,
        "text_only_items": text_only_items,
        "review_queue_count": len(review_queue),
        "review_queue": review_queue,
        "unmatched_images": unmatched_images,
        "unmatched_attachments": unmatched_attachments,
        "source_text_preview": [_collapse(line) for line in normalized_text.splitlines() if _collapse(line)][:16],
        "report_text": report_text,
    }


def _openai_json_response(
    *,
    client: Any,
    model: str,
    content: Sequence[Dict[str, Any]],
    timeout_sec: float,
    reasoning_effort: str = "",
) -> Dict[str, Any] | None:
    _clear_openai_error_state()
    request_kwargs: Dict[str, Any] = {
        "model": model,
        "input": [{"role": "user", "content": list(content or [])}],
        "timeout": timeout_sec,
    }
    if reasoning_effort and model.startswith("gpt-5"):
        request_kwargs["reasoning"] = {"effort": reasoning_effort}
    try:
        try:
            response = client.responses.create(**request_kwargs)
        except Exception as exc:
            if request_kwargs.get("reasoning") and "Unsupported parameter" in str(exc):
                request_kwargs.pop("reasoning", None)
                response = client.responses.create(**request_kwargs)
            else:
                raise
        raw = _extract_json_text(getattr(response, "output_text", "") or "")
        if not raw:
            _set_openai_error_state("invalid_json", _analysis_reason_notice("invalid_json"))
            return None
        data = json.loads(raw)
        _clear_openai_error_state()
        return data if isinstance(data, dict) else None
    except Exception as exc:
        message = _collapse(str(exc))
        reason = _classify_openai_error_message(message)
        _set_openai_error_state(reason, _summarize_openai_error(exc), details=message)
        return None


def _optimized_openai_image_bytes(entry: Dict[str, Any]) -> Tuple[str, bytes]:
    raw = entry.get("bytes")
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        return "", b""
    payload = bytes(raw)
    if PILImage is None:
        mime = str(entry.get("content_type") or "image/jpeg").strip() or "image/jpeg"
        return mime, payload
    try:
        image = PILImage.open(BytesIO(payload))
        image.load()
        image = image.convert("RGB")
        image.thumbnail((WORK_REPORT_OPENAI_IMAGE_MAX_DIM, WORK_REPORT_OPENAI_IMAGE_MAX_DIM), PILImage.Resampling.LANCZOS)
        output = BytesIO()
        image.save(output, format="JPEG", quality=WORK_REPORT_OPENAI_IMAGE_QUALITY, optimize=True)
        optimized = output.getvalue()
        if optimized:
            return "image/jpeg", optimized
    except Exception:
        pass
    mime = str(entry.get("content_type") or "image/jpeg").strip() or "image/jpeg"
    return mime, payload


def _openai_image_url(entry: Dict[str, Any]) -> str:
    mime, payload = _optimized_openai_image_bytes(entry)
    if not payload:
        return ""
    return f"data:{mime};base64,{base64.b64encode(payload).decode('ascii')}"


def _openai_work_report(
    *,
    text: str,
    image_inputs: List[Dict[str, Any]],
    reference_image_inputs: List[Dict[str, Any]] | None,
    attachment_inputs: List[Dict[str, Any]],
    sample_title: str,
    sample_lines: Sequence[str],
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any] | None:
    client, model = _openai_client(default_model="gpt-5.4", env_name="WORK_REPORT_OPENAI_MODEL")
    if not client:
        return None
    timeout_sec = _float_env("WORK_REPORT_OPENAI_TIMEOUT_SEC", DEFAULT_WORK_REPORT_OPENAI_TIMEOUT_SEC)
    client = client.with_options(timeout=timeout_sec, max_retries=0)
    reasoning_effort = _str_env("WORK_REPORT_OPENAI_REASONING_EFFORT", "medium" if model.startswith("gpt-5") else "")
    candidate_lines: List[str] = []
    for position, event in enumerate(_parse_kakao_events(text), start=1):
        candidate_text = _collapse(event.get("text") or "")
        if not _looks_like_work_item(candidate_text):
            continue
        candidate_lines.append(
            f"- #{position} / {event.get('date_label') or event.get('date') or '-'} / {event.get('sender') or '-'} / {candidate_text}"
        )
        if len(candidate_lines) >= 80:
            break
    text_excerpt = _openai_text_excerpt(text)
    few_shot_lines = _feedback_few_shot_lines(feedback_profile, limit=4)
    prompt = """
너는 아파트 관리사무소 시설팀의 주요업무보고서를 만드는 도우미다.
입력으로 카카오톡 단체방 대화, 현장 사진, 첨부파일 목록, 샘플 보고서 개요가 들어온다.

해야 할 일:
1. 작업 항목을 추출한다.
2. 각 항목별 작업내용, 작업일자, 작업자(또는 업체/담당자), 위치, 요약을 정리한다.
3. 이미지와 첨부파일을 가장 알맞은 작업 항목에 매칭한다.
4. 이미지가 전/중/후 중 어디에 가까운지도 분류한다.
5. 반드시 JSON으로만 답한다.

출력 형식:
{
  "report_title": "",
  "period_label": "",
  "items": [
    {
      "title": "",
      "work_date": "",
      "work_date_label": "",
      "vendor_name": "",
      "location_name": "",
      "summary": "",
      "before_image_indexes": [1],
      "during_image_indexes": [],
      "after_image_indexes": [2],
      "attachment_indexes": [1],
      "confidence": "high"
    }
  ],
  "unmatched_image_indexes": [],
  "unmatched_attachment_indexes": [],
  "analysis_notice": ""
}

규칙:
- 이미지/첨부파일 index는 제공된 목록 번호만 사용한다.
- 이미지가 없어도 작업으로 보이면 누락하지 말고 items에 포함한다. 그 경우 before/during/after image index는 모두 빈 배열로 둔다.
- 모호하면 confidence를 low로 두고 analysis_notice에 이유를 남긴다.
- title은 보고서 표에 그대로 들어갈 짧고 구체적인 작업명으로 작성한다. 불필요한 수식어, 추측, 군더더기는 빼고 대화의 핵심 표현을 유지한다.
- vendor_name에는 업체명, 작업자명, 담당자명 중 대화에 가장 분명하게 나온 주체를 넣는다. 아무 근거가 없으면 빈 문자열로 둔다.
- location_name은 동/호/시설명/공간명을 가능한 한 원문 그대로 구체적으로 유지한다. 모르면 비우고 추측하지 않는다.
- summary는 이미지 위에 1줄 설명으로 들어갈 문장이다. title을 반복하지 말고 무엇을 확인/교체/정리/접수했는지 짧고 자연스럽게 설명한다.
- sample 보고서의 제목/보고기간 표현은 참고하되, 실제 입력이 다르면 입력을 우선한다.
- 시간 순서는 참고만 하고, 이미지의 실제 시각적 내용이 더 중요하다.
- 다만 이미지 파일명의 촬영시각, 사진 공지 직전/직후 대화, 같은 시각대의 연속 이미지도 함께 검토한다.
- 같은 촬영 군집에 대해 제공된 근접 대화 후보가 있으면 그 후보를 우선 검토한다.
- 카톡 캡처 참고 이미지(S)는 대화 순서와 설명 문구를 파악하기 위한 참고 자료다. 현장사진(I)와 같은 장면이라고 단정하지 말고, 문맥 근거로만 활용한다.
- 촬영 군집 근처에 명시적인 작업 문구가 있으면 그 문구의 제목과 위치 표현을 우선 유지한다. 비슷한 설비라고 해서 더 일반적인 다른 작업명으로 바꾸지 않는다.
- 같은 날 조명/센서등처럼 비슷한 작업이 여러 개 있으면, 시각적으로 비슷해 보여도 가장 가까운 시간대의 구체적인 대화 문구(동, 위치, 수량)를 우선한다.
- 하나의 촬영 군집은 가장 강한 단일 작업 항목에 우선 매칭한다. 근거가 약하면 다른 항목으로 퍼뜨리지 말고 unmatched로 남긴다.
- 사진 직후에 나온 첫 설명 문구가 작업/습득물의 본제목이고, 그 다음 줄이 전달요청/업체문의/통화완료/소유자 확인 같은 후속 메모라면 새 항목으로 분리하지 말고 같은 항목 summary에 흡수한다.
- CCTV 캡처, 증빙 화면, 확인 사진은 수리 사진이 아니어도 가장 가까운 확인/전달/민원 대응 항목에 매칭할 수 있다.
- 같은 작업 내용이 같은 날 두 번 이상 반복되면 첫 등장은 작업 전, 마지막 등장은 작업 후일 가능성을 우선 검토한다.
- 보고서 완성도는 작업 전/작업 후 이미지 쌍이 가장 중요하므로 가능하면 둘 다 확보되도록 매칭한다.
- 이미지가 2장인 작업은 가능하면 작업전/작업후가 되도록 배치하고, 3장 이상이면 첫 장은 작업전, 마지막 장은 작업후, 중간은 작업중 흐름이 자연스럽도록 정리한다.
- 이미지가 1장뿐이면 억지로 작업전/작업후를 단정하지 말고 가장 대표 사진으로만 둔다.
- 자전거/스케이트보드/보관소가 보이면 자전거 정리·회수 계열 작업에 우선 매칭한다.
- 음식물처리기 본체/키패드/안내문/AS 종이가 보이면 음식물처리기 고장·AS 접수 계열에 우선 매칭한다.
- 천장등/센서등/등기구/분리된 원형 조명이 보이면 조명·센서등 교체 계열에 우선 매칭한다.
- 박스나 자재 사진은 입고, 교체예정, 자재 준비와 더 가깝다.
- 이미지가 여러 장이면 가능한 한 같은 작업 내에서 작업 전/작업중/작업후 흐름이 자연스럽게 이어지도록 배치한다.
- 이미지가 많으면 대표 이미지만 직접 시각 검토하고, 나머지는 같은 촬영시각 군집과 파일명 정보를 근거로 조심스럽게 확장한다.
- 첨부파일은 매칭 근거로만 사용하고, 제목을 새로 만들기 위한 과도한 추측 근거로 사용하지 않는다.
- 잘못된 매칭보다 보수적인 매칭이 낫다. 확신이 낮으면 unmatched로 남기고 analysis_notice에 적는다.
- 최근 사람 검토 예시가 주어지면 같은 단지에서 실제로 수정·확정된 사례로 보고 참고하되, 현재 입력 근거가 더 분명하면 현재 입력을 우선한다.
""".strip()
    content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
    if sample_title or sample_lines:
        sample_excerpt = "\n".join(_collapse(line) for line in list(sample_lines)[:20] if _collapse(line))
        content.append({"type": "input_text", "text": f"샘플 제목: {sample_title or '-'}\n샘플 개요:\n{sample_excerpt or '-'}"})
    if _collapse(text_excerpt.get("text") or ""):
        text_heading = "카톡 대화 축약본" if text_excerpt.get("applied") else "카톡 대화 원문"
        content.append({"type": "input_text", "text": f"{text_heading}:\n{text_excerpt.get('text') or ''}"})
    if candidate_lines:
        content.append({"type": "input_text", "text": "대화에서 보이는 작업 후보:\n" + "\n".join(candidate_lines)})
    if few_shot_lines:
        content.append({"type": "input_text", "text": "최근 사람 검토 예시:\n" + "\n".join(few_shot_lines)})
    reference_images = list(reference_image_inputs or [])
    if reference_images:
        reference_meta = _select_openai_visual_meta(reference_images, limit=MAX_WORK_REPORT_OPENAI_REFERENCE_IMAGES)
        reference_lookup = {index: row for index, row in enumerate(reference_images, start=1)}
        reference_lines = [
            f"[S{int(row.get('index') or 0)}] {row.get('filename') or '-'} / 카톡 캡처 참고 이미지"
            for row in reference_meta
            if int(row.get("index") or 0) > 0
        ]
        if reference_lines:
            content.append(
                {
                    "type": "input_text",
                    "text": (
                        "카톡 캡처 참고 이미지 목록:\n"
                        + "\n".join(reference_lines)
                        + "\n이 이미지는 작업사진 출력용이 아니라 분석 참고용이다. 결과 JSON의 before/during/after에는 현장사진 I index만 사용하라."
                    ),
                }
            )
        for row in reference_meta:
            index = int(row.get("index") or 0)
            item = reference_lookup.get(index)
            if not item:
                continue
            data_url = _openai_image_url(item)
            content.append({"type": "input_text", "text": f"카톡 캡처 참고 이미지 S{index}: {item.get('filename') or f'source-{index}'}"})
            if data_url:
                content.append({"type": "input_image", "image_url": data_url})
    visual_meta: List[Dict[str, Any]] = []
    if image_inputs:
        image_meta_rows = [_openai_image_meta(index, item) for index, item in enumerate(image_inputs, start=1)]
        image_descriptions = []
        for row in image_meta_rows:
            index = int(row.get("index") or 0)
            time_label = "-"
            second_of_day = int(row.get("second_of_day") or -1)
            if second_of_day >= 0:
                hour = second_of_day // 3600
                minute = (second_of_day % 3600) // 60
                second = second_of_day % 60
                time_label = f"{row.get('date') or '-'} {hour:02d}:{minute:02d}:{second:02d}"
            image_descriptions.append(
                f"[I{index}] {row.get('filename') or f'image-{index}'} / 촬영시각: {time_label} / 파일단계힌트: {_collapse(row.get('stage_hint') or '-')}"
            )
        content.append(
            {
                "type": "input_text",
                "text": "이미지 목록:\n" + "\n".join(image_descriptions),
            }
        )
        cluster_lines = _openai_cluster_lines(image_meta_rows)
        if cluster_lines:
            content.append({"type": "input_text", "text": "이미지 촬영 군집:\n" + "\n".join(cluster_lines)})
        cluster_context_lines = _openai_cluster_context_lines(image_meta_rows, text)
        if cluster_context_lines:
            content.append({"type": "input_text", "text": "촬영 군집별 근접 대화 후보:\n" + "\n".join(cluster_context_lines)})
        visual_meta = _select_openai_visual_meta(image_inputs, limit=MAX_WORK_REPORT_OPENAI_VISUAL_IMAGES)
        if visual_meta and len(visual_meta) < len(image_inputs):
            representative = ", ".join(f"I{int(row.get('index') or 0)}" for row in visual_meta if int(row.get("index") or 0) > 0)
            omitted = len(image_inputs) - len(visual_meta)
            content.append(
                {
                    "type": "input_text",
                    "text": f"직접 시각 검토할 대표 이미지는 {representative} 이다. 나머지 {omitted}장은 같은 군집과 시간정보를 근거로 보수적으로 확장 매칭하라.",
                }
            )
        image_lookup = {index: row for index, row in enumerate(image_inputs, start=1)}
        for row in visual_meta:
            index = int(row.get("index") or 0)
            item = image_lookup.get(index)
            if not item:
                continue
            data_url = _openai_image_url(item)
            content.append({"type": "input_text", "text": f"이미지 I{index}: {item.get('filename')}"})
            if data_url:
                content.append({"type": "input_image", "image_url": data_url})
    if attachment_inputs:
        attachment_text = []
        for index, item in enumerate(attachment_inputs, start=1):
            preview = _collapse(item.get("preview_text") or "")
            attachment_text.append(f"[A{index}] {item.get('filename')} / {preview or '본문 미리보기 없음'}")
        content.append({"type": "input_text", "text": "\n".join(attachment_text)})
    raw_data = _openai_json_response(
        client=client,
        model=model,
        content=content,
        timeout_sec=timeout_sec,
        reasoning_effort=reasoning_effort,
    )
    if not raw_data:
        return None
    data = _normalize_ai_work_report_payload(raw_data)
    if visual_meta and len(visual_meta) < len(image_inputs):
        representative_notice = (
            f"AI가 대표 이미지 {len(visual_meta)}장과 촬영 군집 정보를 직접 검토하고, "
            f"나머지 {len(image_inputs) - len(visual_meta)}장은 시간군집 기준으로 보수적으로 확장 매칭했습니다."
        )
        merged_notice = _collapse(data.get("analysis_notice") or "")
        data["analysis_notice"] = representative_notice if not merged_notice else f"{merged_notice} {representative_notice}"
    items: List[Dict[str, Any]] = []
    for index, row in enumerate(data.get("items") or [], start=1):
        if not isinstance(row, dict):
            continue
        item = {
            "index": index,
            "title": _clean_item_title(row.get("title") or row.get("summary") or ""),
            "work_date": _collapse(row.get("work_date") or ""),
            "work_date_label": _collapse(row.get("work_date_label") or ""),
            "vendor_name": _collapse(row.get("vendor_name") or ""),
            "location_name": _collapse(row.get("location_name") or ""),
            "summary": _collapse(row.get("summary") or row.get("title") or ""),
            "confidence": _collapse(row.get("confidence") or "ai"),
            "images": [],
            "attachments": [],
            "before_image_indexes": [int(value) for value in row.get("before_image_indexes") or [] if str(value).isdigit()],
            "during_image_indexes": [int(value) for value in row.get("during_image_indexes") or [] if str(value).isdigit()],
            "after_image_indexes": [int(value) for value in row.get("after_image_indexes") or [] if str(value).isdigit()],
            "attachment_indexes": [int(value) for value in row.get("attachment_indexes") or [] if str(value).isdigit()],
        }
        if item["title"]:
            items.append(item)
    if not items:
        return None
    unmatched_image_indexes = {int(value) for value in data.get("unmatched_image_indexes") or [] if str(value).isdigit()}
    unmatched_attachment_indexes = {int(value) for value in data.get("unmatched_attachment_indexes") or [] if str(value).isdigit()}
    image_lookup = {index: row for index, row in enumerate(image_inputs, start=1)}
    attachment_lookup = {index: row for index, row in enumerate(attachment_inputs, start=1)}
    used_images: set[int] = set()
    used_attachments: set[int] = set()
    for item in items:
        image_matches: List[Dict[str, Any]] = []
        for stage, indexes in (
            ("before", item.pop("before_image_indexes")),
            ("during", item.pop("during_image_indexes")),
            ("after", item.pop("after_image_indexes")),
        ):
            for image_index in indexes:
                row = image_lookup.get(image_index)
                if not row:
                    continue
                used_images.add(image_index)
                image_matches.append(
                    {
                        "index": image_index,
                        "filename": row.get("filename"),
                        "preview_available": bool(_collapse(row.get("preview_relative_path") or "")),
                        "stage": stage,
                    }
                )
        item["images"] = _finalize_image_stages(image_matches)
        attachment_matches: List[Dict[str, Any]] = []
        for attachment_index in item.pop("attachment_indexes"):
            row = attachment_lookup.get(attachment_index)
            if not row:
                continue
            used_attachments.add(attachment_index)
            attachment_matches.append(
                {
                    "index": attachment_index,
                    "filename": row.get("filename"),
                    "preview_text": _collapse(row.get("preview_text") or ""),
                }
            )
        item["attachments"] = attachment_matches
    return {
        "report_title": _collapse(data.get("report_title") or ""),
        "period_label": _collapse(data.get("period_label") or ""),
        "items": items,
        "unmatched_image_indexes": sorted((set(range(1, len(image_inputs) + 1)) - used_images) | unmatched_image_indexes),
        "unmatched_attachment_indexes": sorted((set(range(1, len(attachment_inputs) + 1)) - used_attachments) | unmatched_attachment_indexes),
        "analysis_notice": _collapse(data.get("analysis_notice") or ""),
        "analysis_model": model,
        "analysis_reason": "",
        "analysis_diagnostics": {
            "openai_model": model,
            "text_compacted": bool(text_excerpt.get("applied")),
            "text_excerpt_mode": _collapse(text_excerpt.get("mode") or ""),
            "text_excerpt_char_count": int(text_excerpt.get("char_count") or 0),
            "text_source_char_count": int(text_excerpt.get("source_char_count") or 0),
            "text_excerpt_event_count": int(text_excerpt.get("event_count") or 0),
            "representative_image_count": len(visual_meta),
            "reference_image_count": len(reference_images),
            "few_shot_example_count": len(few_shot_lines),
        },
    }


def _openai_match_image_chunks(
    *,
    text: str,
    image_inputs: Sequence[Dict[str, Any]],
    items: Sequence[Dict[str, Any]],
    progress_callback: WorkReportProgressCallback | None = None,
    feedback_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any] | None:
    client, model = _openai_client(default_model="gpt-5.4", env_name="WORK_REPORT_OPENAI_MODEL")
    if not client or not image_inputs or not items:
        return None
    timeout_sec = _float_env("WORK_REPORT_OPENAI_IMAGE_MATCH_TIMEOUT_SEC", DEFAULT_WORK_REPORT_OPENAI_IMAGE_MATCH_TIMEOUT_SEC)
    client = client.with_options(timeout=timeout_sec, max_retries=0)
    reasoning_effort = _str_env("WORK_REPORT_OPENAI_REASONING_EFFORT", "medium" if model.startswith("gpt-5") else "")
    max_clusters = _int_env("WORK_REPORT_OPENAI_IMAGE_MATCH_MAX_CLUSTERS", WORK_REPORT_OPENAI_IMAGE_MATCH_MAX_CLUSTERS, minimum=1)
    sample_per_cluster = _int_env(
        "WORK_REPORT_OPENAI_IMAGE_MATCH_SAMPLE_PER_CLUSTER",
        WORK_REPORT_OPENAI_IMAGE_MATCH_SAMPLE_PER_CLUSTER,
        minimum=1,
    )
    if len(image_inputs) >= 24:
        sample_per_cluster = min(sample_per_cluster, 2)
    if len(image_inputs) >= 36:
        max_clusters = min(max_clusters, 3)
    if len(image_inputs) >= 40 or len(items) >= 80:
        sample_per_cluster = 1
    if len(image_inputs) >= 40 and len(items) >= 60:
        max_clusters = min(max_clusters, 2)
    image_lookup = {index: row for index, row in enumerate(list(image_inputs or []), start=1)}
    image_meta_rows = [_openai_image_meta(index, row) for index, row in image_lookup.items()]
    clusters = _cluster_openai_image_meta(image_meta_rows)
    if not clusters:
        return None

    work_events = _work_report_events(text)
    assigned_rows: Dict[int, List[Dict[str, Any]]] = {int(item.get("index") or 0): [] for item in items if int(item.get("index") or 0) > 0}
    unmatched_image_indexes: set[int] = set()
    analysis_notes: List[str] = []
    openai_failures: List[Dict[str, str]] = []
    few_shot_lines = _feedback_few_shot_lines(feedback_profile, limit=4)
    max_item_context = 18 if len(items) >= 40 else max(8, len(items))
    if len(items) >= 80:
        max_item_context = 12

    prompt = """
너는 이미 추출된 시설팀 작업 항목에 현장 사진 군집을 매칭하는 도우미다.
작업 항목 목록은 이미 확정되었다. 새 작업 항목을 만들지 말고, 각 사진 군집(Cn)을 가장 알맞은 작업 항목(Tn) 하나에만 매칭하거나 unmatched로 남겨라.
반드시 JSON으로만 답한다.

출력 형식:
{
  "cluster_matches": [
    {"cluster_index": 1, "item_index": 2, "confidence": "high"}
  ],
  "unmatched_cluster_indexes": [3],
  "analysis_notice": ""
}

규칙:
- cluster_index와 item_index는 제공된 번호만 사용한다.
- 하나의 군집은 하나의 작업 항목에만 매칭한다.
- 확신이 낮으면 unmatched_cluster_indexes로 남긴다.
- 실제 이미지 내용, 파일명의 촬영시각, 같은 시각대 연속 촬영 여부, 근접 대화 후보를 함께 본다.
- 군집 설명에 우선후보 Tn이 있으면 먼저 검토하되, 실제 시각 정보가 명백히 다르면 따르지 않아도 된다.
- 비슷한 작업명이 여러 개면 동/위치/사물 종류가 더 구체적으로 맞는 항목을 우선한다.
- title이 비슷해도 배경, 창호, 조명 종류, 자전거/습득물/키패드처럼 보이는 핵심 물체가 다르면 다른 작업으로 본다.
- 군집 안 여러 장이 같은 장소의 연속 상태를 보여주면 한 항목 안에서 before/during/after 흐름으로 이어질 수 있는 작업을 우선한다.
- 매칭이 약한데 억지로 붙이지 말고 unmatched로 둔다.
- 최근 사람 검토 예시가 주어지면 같은 단지에서 실제로 수정·확정된 사례로 보고 참고하되, 현재 군집 근거가 더 분명하면 현재 근거를 우선한다.
""".strip()

    cluster_batches = _chunk_rows(clusters, max_clusters)
    total_batches = max(1, len(cluster_batches))

    for batch_number, batch_clusters in enumerate(cluster_batches, start=1):
        _report_progress(
            progress_callback,
            current_step=3,
            summary=f"이미지 군집 {batch_number}/{total_batches}개를 매칭하고 있습니다.",
            hint="대량 이미지는 군집 단위로 나눠 작업 항목과 연결합니다.",
        )
        content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        batch_items = _batch_candidate_items(
            batch_clusters,
            items,
            work_events,
            per_cluster_limit=4,
            total_limit=max_item_context,
            feedback_profile=feedback_profile,
        )
        batch_item_lines = _openai_item_summary_lines(batch_items or items)
        content.append({"type": "input_text", "text": "작업 항목 목록:\n" + "\n".join(batch_item_lines)})
        if few_shot_lines:
            content.append({"type": "input_text", "text": "최근 사람 검토 예시:\n" + "\n".join(few_shot_lines)})

        cluster_lines: List[str] = []
        for local_index, cluster in enumerate(batch_clusters, start=1):
            indexes = [f"I{int(row.get('index') or 0)}" for row in cluster if int(row.get("index") or 0) > 0]
            if not indexes:
                continue
            file_names = ", ".join(str(row.get("filename") or "") for row in cluster[:4])
            time_line = _openai_cluster_lines(cluster)
            nearby_line = _openai_cluster_context_lines(cluster, text)
            candidate_lines = _cluster_item_candidate_lines(cluster, items, work_events, feedback_profile=feedback_profile)
            cluster_parts = [f"[C{local_index}] 전체 이미지 {', '.join(indexes)}", f"파일 {file_names or '-'}"]
            if time_line:
                cluster_parts.append(re.sub(r"^- C1:\s*", "", str(time_line[0])))
            if nearby_line:
                cluster_parts.append("근접 대화 " + re.sub(r"^- C1:\s*", "", str(nearby_line[0])))
            if candidate_lines:
                cluster_parts.append("우선후보 " + " ; ".join(candidate_lines))
            cluster_lines.append(" / ".join(part for part in cluster_parts if part))
        if cluster_lines:
            content.append({"type": "input_text", "text": "이번 배치 사진 군집:\n" + "\n".join(cluster_lines)})

        for local_index, cluster in enumerate(batch_clusters, start=1):
            for sample_order, row in enumerate(_sample_cluster_rows(cluster, sample_per_cluster), start=1):
                image_index = int(row.get("index") or 0)
                if image_index <= 0:
                    continue
                source = image_lookup.get(image_index)
                if not source:
                    continue
                content.append(
                    {
                        "type": "input_text",
                        "text": (
                            f"대표 이미지 C{local_index}-{sample_order} / global I{image_index} / "
                            f"{source.get('filename') or f'image-{image_index}'} / "
                            f"파일단계힌트 {_collapse(row.get('stage_hint') or '-')}"
                        ),
                    }
                )
                data_url = _openai_image_url(source)
                if data_url:
                    content.append({"type": "input_image", "image_url": data_url})

        raw_data = _openai_json_response(
            client=client,
            model=model,
            content=content,
            timeout_sec=timeout_sec,
            reasoning_effort=reasoning_effort,
        )

        matched_local_indexes: set[int] = set()
        unmatched_local_indexes: set[int] = set()
        if isinstance(raw_data, dict):
            for row in raw_data.get("cluster_matches") or []:
                if not isinstance(row, dict):
                    continue
                local_index = int(row.get("cluster_index") or 0)
                item_index = int(row.get("item_index") or 0)
                if local_index <= 0 or local_index > len(batch_clusters) or local_index in matched_local_indexes:
                    continue
                if item_index not in assigned_rows:
                    continue
                cluster = batch_clusters[local_index - 1]
                for cluster_row in cluster:
                    assigned_rows[item_index].append(dict(cluster_row))
                matched_local_indexes.add(local_index)
            unmatched_local_indexes = {
                int(value)
                for value in raw_data.get("unmatched_cluster_indexes") or []
                if str(value).isdigit() and 0 < int(value) <= len(batch_clusters)
            }
            notice = _collapse(raw_data.get("analysis_notice") or "")
            if notice:
                _append_unique_note(analysis_notes, notice)
        else:
            snapshot = _consume_openai_error_snapshot()
            if snapshot.get("notice"):
                _append_unique_note(analysis_notes, snapshot["notice"])
            if any(snapshot.values()):
                openai_failures.append(
                    {
                        "stage": f"image_batch_{batch_number}",
                        "reason": _collapse(snapshot.get("reason") or ""),
                        "notice": _collapse(snapshot.get("notice") or ""),
                        "details": _collapse(snapshot.get("details") or ""),
                    }
                )
            _append_unique_note(analysis_notes, f"이미지 배치 {batch_number}건은 AI 응답이 불안정해 보수적으로 재배치했습니다.")

        pending_local_indexes = set(range(1, len(batch_clusters) + 1)) - matched_local_indexes - unmatched_local_indexes
        if pending_local_indexes:
            for local_index in sorted(pending_local_indexes):
                cluster = batch_clusters[local_index - 1]
                fallback_item_index = _best_item_index_for_cluster(items, cluster, feedback_profile=feedback_profile)
                if fallback_item_index and fallback_item_index in assigned_rows:
                    for cluster_row in cluster:
                        assigned_rows[fallback_item_index].append(dict(cluster_row))
                else:
                    unmatched_local_indexes.add(local_index)
            if raw_data is None:
                _append_unique_note(analysis_notes, f"이미지 배치 {batch_number}의 남은 군집은 파일명/문맥 점수로만 보수 매칭했습니다.")
        for local_index in unmatched_local_indexes:
            for cluster_row in batch_clusters[local_index - 1]:
                image_index = int(cluster_row.get("index") or 0)
                if image_index > 0:
                    unmatched_image_indexes.add(image_index)

    assigned_indexes = {
        int(row.get("index") or 0)
        for rows in assigned_rows.values()
        for row in rows
        if int(row.get("index") or 0) > 0
    }
    for image_index in range(1, len(image_inputs) + 1):
        if image_index not in assigned_indexes and image_index not in unmatched_image_indexes:
            unmatched_image_indexes.add(image_index)

    return {
        "items": _apply_image_matches_to_items(items, assigned_rows),
        "unmatched_image_indexes": sorted(unmatched_image_indexes),
        "analysis_notice": _collapse(" ".join(note for note in analysis_notes if _collapse(note))),
        "analysis_model": model,
        "analysis_reason": next((row["reason"] for row in openai_failures if _collapse(row.get("reason") or "")), ""),
        "analysis_diagnostics": {
            "openai_model": model,
            "cluster_count": len(clusters),
            "cluster_batch_count": total_batches,
            "max_clusters_per_batch": max_clusters,
            "sample_per_cluster": sample_per_cluster,
            "few_shot_example_count": len(few_shot_lines),
            "max_item_context": max_item_context,
            "openai_failures": openai_failures,
        },
    }


def analyze_work_report(
    text: str,
    *,
    tenant_id: str = "",
    image_inputs: Sequence[Dict[str, Any]] | None = None,
    reference_image_inputs: Sequence[Dict[str, Any]] | None = None,
    attachment_inputs: Sequence[Dict[str, Any]] | None = None,
    sample_title: str = "",
    sample_lines: Sequence[str] | None = None,
    defer_image_matching: bool = False,
    base_report: Optional[Dict[str, Any]] = None,
    selected_image_item_indexes: Sequence[int] | None = None,
    progress_callback: WorkReportProgressCallback | None = None,
) -> Dict[str, Any]:
    normalized_text = str(text or "")
    images = list(image_inputs or [])[:MAX_WORK_REPORT_IMAGES]
    reference_images = list(reference_image_inputs or [])
    attachments = list(attachment_inputs or [])[:MAX_WORK_REPORT_ATTACHMENTS]
    sample_lines = list(sample_lines or [])
    image_heavy = len(images) >= WORK_REPORT_HEAVY_IMAGE_THRESHOLD
    defer_image_matching = bool(defer_image_matching)
    selected_item_index_set = {int(value) for value in list(selected_image_item_indexes or []) if int(value) > 0}
    normalized_base_items = _normalize_existing_work_report_items(base_report)
    normalized_base_unmatched_attachments = _normalize_existing_unmatched_attachments(base_report)
    if not _collapse(normalized_text) and not images and not reference_images and not attachments:
        raise ValueError("text, image, or attachment is required")

    openai_failure_notes: List[str] = []

    _report_progress(
        progress_callback,
        current_step=0,
        summary="원문과 첨부 내용을 배치 분석용으로 정리하고 있습니다.",
        hint="입력 크기에 따라 초기 정리 시간이 조금 걸릴 수 있습니다.",
    )

    chunk_trigger_images = _int_env(
        "WORK_REPORT_OPENAI_CHUNK_TRIGGER_IMAGES",
        WORK_REPORT_OPENAI_CHUNK_TRIGGER_IMAGES,
        minimum=1,
    )
    use_chunked_image_matching = bool(images) and len(images) >= chunk_trigger_images and not defer_image_matching and not normalized_base_items
    openai_failures: List[Dict[str, str]] = []
    analysis_diagnostics: Dict[str, Any] = {
        "input_image_count": len(images),
        "reference_image_count": len(reference_images),
        "attachment_count": len(attachments),
        "image_heavy": image_heavy,
        "used_chunked_image_matching": use_chunked_image_matching,
    }
    image_entries = _work_report_image_entries(images)
    tenant_feedback_profile = _load_work_report_feedback_profile(tenant_id)
    analysis_diagnostics["tenant_feedback_rows_used"] = int(tenant_feedback_profile.get("rows_used") or 0)
    analysis_diagnostics["tenant_feedback_enabled"] = bool(_collapse(tenant_feedback_profile.get("tenant_id") or ""))
    analysis_diagnostics["tenant_few_shot_example_count"] = len(list(tenant_feedback_profile.get("few_shot_examples") or []))

    def remember_openai_error(stage: str) -> None:
        snapshot = _consume_openai_error_snapshot()
        notice = _collapse(snapshot.get("notice") or "")
        if notice:
            _append_unique_note(openai_failure_notes, notice)
        if any(_collapse(value) for value in snapshot.values()):
            openai_failures.append(
                {
                    "stage": _collapse(stage),
                    "reason": _collapse(snapshot.get("reason") or ""),
                    "notice": notice,
                    "details": _collapse(snapshot.get("details") or ""),
                }
            )

    if normalized_base_items:
        _report_progress(
            progress_callback,
            current_step=1,
            summary="선택된 작업 항목에만 현장 사진을 다시 매칭하고 있습니다.",
            hint="사람이 고른 사진 포함 항목과 선택 이미지에만 매칭을 실행합니다.",
        )
        report_title = _collapse((base_report or {}).get("report_title") or _sample_heading(sample_title, sample_lines))
        period_label = _collapse((base_report or {}).get("period_label") or _sample_period(sample_lines))
        unmatched_attachment_indexes: List[int] = []
        analysis_notice = ""
        analysis_model = "manual-selection"
        analysis_reason = ""
        matched_items = [dict(item) for item in normalized_base_items]
        if selected_item_index_set and images:
            selected_items = [dict(item) for item in matched_items if int(item.get("index") or 0) in selected_item_index_set]
            chunked_images = _openai_match_image_chunks(
                text=normalized_text,
                image_inputs=images,
                items=selected_items,
                progress_callback=progress_callback,
                feedback_profile=tenant_feedback_profile,
            )
            if not chunked_images:
                remember_openai_error("selected_image_match")
                image_matches = _assign_entries(selected_items, image_entries, allow_chunk_fallback=False, feedback_profile=tenant_feedback_profile)
                matched_indexes = {
                    int(row.get("index") or 0)
                    for rows in image_matches.values()
                    for row in rows
                    if int(row.get("index") or 0) > 0
                }
                selected_lookup = {
                    int(item.get("index") or 0): item
                    for item in _apply_image_matches_to_items(selected_items, image_matches)
                }
                matched_items = []
                for item in normalized_base_items:
                    clone = dict(item)
                    selected = selected_lookup.get(int(clone.get("index") or 0))
                    clone["images"] = list(selected.get("images") or []) if selected else []
                    matched_items.append(clone)
                unmatched_image_indexes = [
                    int(row.get("index") or 0)
                    for row in image_entries
                    if int(row.get("index") or 0) > 0 and int(row.get("index") or 0) not in matched_indexes
                ]
                analysis_model = "heuristic"
                analysis_reason = next((row["reason"] for row in openai_failures if _collapse(row.get("reason") or "")), "")
                analysis_notice = "선택 항목 기준으로 규칙 기반 이미지 매칭을 적용했습니다."
            else:
                analysis_diagnostics["selected_image_match"] = dict(chunked_images.get("analysis_diagnostics") or {})
                selected_lookup = {
                    int(item.get("index") or 0): item
                    for item in list(chunked_images.get("items") or [])
                    if int(item.get("index") or 0) > 0
                }
                matched_items = []
                for item in normalized_base_items:
                    clone = dict(item)
                    selected = selected_lookup.get(int(clone.get("index") or 0))
                    clone["images"] = list(selected.get("images") or []) if selected else []
                    matched_items.append(clone)
                unmatched_image_indexes = list(chunked_images.get("unmatched_image_indexes") or [])
                analysis_notice = _collapse(chunked_images.get("analysis_notice") or "")
                analysis_model = _collapse(chunked_images.get("analysis_model") or "gpt-5.4")
                analysis_reason = _collapse(chunked_images.get("analysis_reason") or "")
        else:
            unmatched_image_indexes = [int(row.get("index") or 0) for row in image_entries if int(row.get("index") or 0) > 0]
            if images and not selected_item_index_set:
                analysis_notice = "사진 포함 작업 항목이나 사용할 이미지가 아직 정리되지 않아 이미지 매칭을 실행하지 않았습니다."
            else:
                analysis_notice = "현장 사진이 없어 이미지 매칭을 실행하지 않았습니다."
        return _finalize_work_report_result(
            normalized_text=normalized_text,
            sample_title=sample_title,
            report_title=report_title,
            period_label=period_label,
            items=matched_items,
            images=images,
            attachments=attachments,
            image_entries=image_entries,
            unmatched_image_indexes=unmatched_image_indexes,
            unmatched_attachment_indexes=unmatched_attachment_indexes,
            unmatched_attachments_override=normalized_base_unmatched_attachments,
            analysis_notice=analysis_notice,
            analysis_model=analysis_model,
            analysis_reason=analysis_reason,
            analysis_diagnostics=analysis_diagnostics,
            openai_failures=openai_failures,
            tenant_feedback_profile=tenant_feedback_profile,
            analysis_stage="selected_image_match",
            selected_image_item_indexes=sorted(selected_item_index_set),
            use_chunked_image_matching=bool(images),
            reference_images=reference_images,
        )

    ai_result: Dict[str, Any] | None = None
    if use_chunked_image_matching:
        _report_progress(
            progress_callback,
            current_step=1,
            summary="작업 항목을 먼저 추출한 뒤 대량 이미지를 군집으로 나눌 준비를 하고 있습니다.",
            hint="이미지가 많은 경우 텍스트 기반 작업 후보를 먼저 확정합니다.",
        )
        base_ai_result = _openai_work_report(
            text=normalized_text,
            image_inputs=[],
            reference_image_inputs=reference_images,
            attachment_inputs=attachments,
            sample_title=sample_title,
            sample_lines=sample_lines,
            feedback_profile=tenant_feedback_profile,
        )
        if not base_ai_result:
            remember_openai_error("chunk_seed_extract")
        else:
            analysis_diagnostics["seed_extract"] = dict(base_ai_result.get("analysis_diagnostics") or {})
        if base_ai_result:
            chunked_images = _openai_match_image_chunks(
                text=normalized_text,
                image_inputs=images,
                items=list(base_ai_result.get("items") or []),
                progress_callback=progress_callback,
                feedback_profile=tenant_feedback_profile,
            )
            if not chunked_images:
                remember_openai_error("chunked_image_match")
            else:
                analysis_diagnostics["chunked_image_match"] = dict(chunked_images.get("analysis_diagnostics") or {})
            if chunked_images:
                merged_notice = "대량 이미지는 군집 단위로 나눠 AI가 단계적으로 매칭했습니다."
                chunk_notice = _collapse(chunked_images.get("analysis_notice") or "")
                analysis_notice = merged_notice if not chunk_notice else f"{merged_notice} {chunk_notice}"
                ai_result = {
                    "report_title": _collapse(base_ai_result.get("report_title") or ""),
                    "period_label": _collapse(base_ai_result.get("period_label") or ""),
                    "items": list(chunked_images.get("items") or []),
                    "unmatched_image_indexes": list(chunked_images.get("unmatched_image_indexes") or []),
                    "unmatched_attachment_indexes": list(base_ai_result.get("unmatched_attachment_indexes") or []),
                    "analysis_notice": analysis_notice,
                    "analysis_model": _collapse(chunked_images.get("analysis_model") or base_ai_result.get("analysis_model") or "gpt-5.4"),
                    "analysis_reason": _collapse(chunked_images.get("analysis_reason") or base_ai_result.get("analysis_reason") or ""),
                    "analysis_diagnostics": {
                        "seed_extract": dict(base_ai_result.get("analysis_diagnostics") or {}),
                        "chunked_image_match": dict(chunked_images.get("analysis_diagnostics") or {}),
                    },
                }
            else:
                ai_result = base_ai_result
                ai_result["unmatched_image_indexes"] = list(range(1, len(images) + 1))
                fallback_notice = "대량 이미지 AI 매칭이 불안정해 이미지는 미매칭 상태로 유지했습니다."
                current_notice = _collapse(ai_result.get("analysis_notice") or "")
                ai_result["analysis_notice"] = fallback_notice if not current_notice else f"{current_notice} {fallback_notice}"

    if not ai_result:
        _report_progress(
            progress_callback,
            current_step=1,
            summary="원문에서 작업 항목과 문맥을 추출하고 있습니다.",
            hint="AI 분석이 늦거나 불안정하면 보수 추출 경로를 함께 시도합니다.",
        )
        ai_result = _openai_work_report(
            text=normalized_text,
            image_inputs=[] if defer_image_matching else images,
            reference_image_inputs=reference_images,
            attachment_inputs=attachments,
            sample_title=sample_title,
            sample_lines=sample_lines,
            feedback_profile=tenant_feedback_profile,
        )
        if not ai_result:
            remember_openai_error("direct_extract")
        else:
            analysis_diagnostics["direct_extract"] = dict(ai_result.get("analysis_diagnostics") or {})
    if not ai_result and images and not defer_image_matching:
        _report_progress(
            progress_callback,
            current_step=2,
            summary="통합 분석 대신 이미지 군집 매칭으로 다시 시도하고 있습니다.",
            hint="대량 이미지가 한 번에 불안정할 때는 군집 단위로 재시도합니다.",
        )
        base_ai_result = _openai_work_report(
            text=normalized_text,
            image_inputs=[],
            reference_image_inputs=reference_images,
            attachment_inputs=attachments,
            sample_title=sample_title,
            sample_lines=sample_lines,
            feedback_profile=tenant_feedback_profile,
        )
        if not base_ai_result:
            remember_openai_error("fallback_seed_extract")
        else:
            analysis_diagnostics["fallback_seed_extract"] = dict(base_ai_result.get("analysis_diagnostics") or {})
        if base_ai_result:
            chunked_images = _openai_match_image_chunks(
                text=normalized_text,
                image_inputs=images,
                items=list(base_ai_result.get("items") or []),
                progress_callback=progress_callback,
                feedback_profile=tenant_feedback_profile,
            )
            if not chunked_images:
                remember_openai_error("fallback_chunked_image_match")
            else:
                analysis_diagnostics["fallback_chunked_image_match"] = dict(chunked_images.get("analysis_diagnostics") or {})
            if chunked_images:
                merged_notice = "기본 통합 분석이 지연되어 이미지 매칭을 군집 배치로 다시 수행했습니다."
                chunk_notice = _collapse(chunked_images.get("analysis_notice") or "")
                analysis_notice = merged_notice if not chunk_notice else f"{merged_notice} {chunk_notice}"
                ai_result = {
                    "report_title": _collapse(base_ai_result.get("report_title") or ""),
                    "period_label": _collapse(base_ai_result.get("period_label") or ""),
                    "items": list(chunked_images.get("items") or []),
                    "unmatched_image_indexes": list(chunked_images.get("unmatched_image_indexes") or []),
                    "unmatched_attachment_indexes": list(base_ai_result.get("unmatched_attachment_indexes") or []),
                    "analysis_notice": analysis_notice,
                    "analysis_model": _collapse(chunked_images.get("analysis_model") or base_ai_result.get("analysis_model") or "gpt-5.4"),
                    "analysis_reason": _collapse(chunked_images.get("analysis_reason") or base_ai_result.get("analysis_reason") or ""),
                    "analysis_diagnostics": {
                        "fallback_seed_extract": dict(base_ai_result.get("analysis_diagnostics") or {}),
                        "fallback_chunked_image_match": dict(chunked_images.get("analysis_diagnostics") or {}),
                    },
                }
    analysis_reason = ""
    if ai_result:
        _report_progress(
            progress_callback,
            current_step=3,
            summary="작업 항목과 현장 사진의 대응 관계를 정리하고 있습니다." if not defer_image_matching else "작업 항목 추출을 마치고 사진 선택 단계를 준비하고 있습니다.",
            hint="사진이 적어도 단계 정보와 위치 문맥을 다시 정리합니다." if not defer_image_matching else "사람이 사진 포함 항목과 사용할 이미지를 고른 뒤 선택 매칭을 실행합니다.",
        )
        items = list(ai_result.get("items") or [])
        unmatched_image_indexes = list(ai_result.get("unmatched_image_indexes") or [])
        unmatched_attachment_indexes = list(ai_result.get("unmatched_attachment_indexes") or [])
        analysis_notice = _collapse(ai_result.get("analysis_notice") or "")
        analysis_model = _collapse(ai_result.get("analysis_model") or "gpt-5")
        analysis_reason = _collapse(ai_result.get("analysis_reason") or "")
        report_title = _collapse(ai_result.get("report_title") or _sample_heading(sample_title, sample_lines))
        period_label = _collapse(ai_result.get("period_label") or _sample_period(sample_lines))
        if openai_failure_notes:
            ai_notice = " ".join(openai_failure_notes)
            analysis_notice = ai_notice if not analysis_notice else f"{analysis_notice} {ai_notice}"
        if not analysis_reason:
            analysis_reason = next((row["reason"] for row in openai_failures if _collapse(row.get("reason") or "")), "")
        ai_result_diagnostics = ai_result.get("analysis_diagnostics")
        if isinstance(ai_result_diagnostics, dict) and ai_result_diagnostics:
            analysis_diagnostics["final_openai"] = dict(ai_result_diagnostics)
        supplemental_items = _supplement_text_only_items(items, normalized_text, image_heavy=image_heavy)
        if supplemental_items:
            for item in supplemental_items:
                item["index"] = len(items) + 1
                items.append(item)
            supplement_notice = f"텍스트 전용 작업 {len(supplemental_items)}건을 추가로 보강했습니다."
            analysis_notice = supplement_notice if not analysis_notice else f"{analysis_notice} {supplement_notice}"
        if defer_image_matching:
            for item in items:
                item["images"] = []
            unmatched_image_indexes = [int(row.get("index") or 0) for row in image_entries if int(row.get("index") or 0) > 0]
            selection_notice = "불필요한 대화를 제외한 뒤 사진 포함 항목과 사용할 이미지를 고르면, 선택된 범위에만 AI 이미지 매칭을 실행합니다."
            analysis_notice = selection_notice if not analysis_notice else f"{analysis_notice} {selection_notice}"
    else:
        _report_progress(
            progress_callback,
            current_step=1,
            summary="AI 응답이 없어 원문 기반 보수 추출로 전환했습니다.",
            hint="이미지와 대화 내용을 시간 순서대로 다시 정리하고 있습니다.",
        )
        events = _parse_kakao_events(normalized_text)
        explicit_items: List[Dict[str, Any]] = []
        current_item: Optional[Dict[str, Any]] = None
        pending_image_notices: List[Dict[str, Any]] = []
        pending_attachment_count = 0
        pending_attachment_texts: List[str] = []
        attachment_notice_cursor = 0

        def add_notice(item: Dict[str, Any], event: Dict[str, Any], count: int, *, kind: str) -> None:
            nonlocal attachment_notice_cursor
            total = max(0, int(count or 0))
            if total <= 0:
                return
            if kind == "image":
                item.setdefault("_image_notices", []).append(
                    {
                        "count": total,
                        "date": _collapse(event.get("date") or ""),
                        "minute_of_day": int(event.get("minute_of_day") or -1),
                        "second_of_day": max(0, int(event.get("minute_of_day") or -1)) * 60,
                    }
                )
                return
            tokens = item.setdefault("_attachment_notice_tokens", [])
            for _ in range(total):
                attachment_notice_cursor += 1
                tokens.append(attachment_notice_cursor)
            item["_expected_attachment_count"] = int(item.get("_expected_attachment_count") or 0) + total

        def apply_pending(item: Dict[str, Any]) -> None:
            nonlocal pending_image_notices, pending_attachment_count, pending_attachment_texts
            if pending_image_notices:
                item.setdefault("_image_notices", []).extend(pending_image_notices)
                pending_image_notices = []
            if pending_attachment_count > 0:
                add_notice(item, {"date": item.get("work_date"), "minute_of_day": item.get("_minute_of_day")}, pending_attachment_count, kind="attachment")
                pending_attachment_count = 0
            if pending_attachment_texts:
                item["_attachment_notice_texts"].extend(pending_attachment_texts)
                pending_attachment_texts = []

        def hold_notice_for_next_item(item: Dict[str, Any] | None, event: Dict[str, Any]) -> bool:
            if not item:
                return True
            item_minute = int(item.get("_minute_of_day") or -1)
            event_minute = int(event.get("minute_of_day") or -1)
            if item_minute >= 0 and event_minute >= 0 and event_minute - item_minute >= 20:
                return True
            return False

        def flush_current() -> None:
            nonlocal current_item
            if current_item and current_item.get("title"):
                current_item["index"] = len(explicit_items) + 1
                explicit_items.append(current_item)
            current_item = None

        for position, event in enumerate(events):
            text_line = _collapse(event.get("text") or "")
            if not text_line:
                continue
            tagged = _extract_tagged_pairs(text_line)
            next_event = events[position + 1] if position + 1 < len(events) else {}
            next_text = _collapse(next_event.get("text") or "")
            if _tagged_value(tagged, "작업내용", "제목", "건명"):
                flush_current()
                current_item = _new_item(_tagged_value(tagged, "작업내용", "제목", "건명"), event, confidence="structured")
                apply_pending(current_item)
                _apply_tagged_fields(current_item, tagged, event)
                continue
            if current_item and tagged:
                _apply_tagged_fields(current_item, tagged, event)
                continue
            if event.get("kind") == "photo_notice":
                should_hold_for_next = hold_notice_for_next_item(current_item, event)
                if next_text and _looks_like_heuristic_anchor(next_text, image_heavy=image_heavy):
                    current_minute = int(event.get("minute_of_day") or -1)
                    next_minute = int(next_event.get("minute_of_day") or -1)
                    if current_minute < 0 or next_minute < 0 or 0 <= next_minute - current_minute <= 5:
                        should_hold_for_next = True
                target_item = None if should_hold_for_next else current_item
                if target_item:
                    add_notice(target_item, event, _notice_count(text_line), kind="image")
                else:
                    pending_image_notices.append(
                        {
                            "count": _notice_count(text_line),
                            "date": _collapse(event.get("date") or ""),
                            "minute_of_day": int(event.get("minute_of_day") or -1),
                            "second_of_day": max(0, int(event.get("minute_of_day") or -1)) * 60,
                        }
                    )
                continue
            if event.get("kind") == "file_notice":
                should_hold_for_next = hold_notice_for_next_item(current_item, event)
                if next_text and _looks_like_heuristic_anchor(next_text, image_heavy=image_heavy):
                    current_minute = int(event.get("minute_of_day") or -1)
                    next_minute = int(next_event.get("minute_of_day") or -1)
                    if current_minute < 0 or next_minute < 0 or 0 <= next_minute - current_minute <= 5:
                        should_hold_for_next = True
                target_item = None if should_hold_for_next else current_item
                if target_item:
                    add_notice(target_item, event, _notice_count(text_line), kind="attachment")
                    target_item["_attachment_notice_texts"].append(text_line)
                else:
                    pending_attachment_count += _notice_count(text_line)
                    pending_attachment_texts.append(text_line)
                continue
            if _looks_like_heuristic_anchor(text_line, image_heavy=image_heavy):
                flush_current()
                current_item = _new_item(text_line, event, confidence="heuristic")
                apply_pending(current_item)
                continue
            if current_item:
                current_tagged = _extract_tagged_pairs(text_line)
                if current_tagged:
                    _apply_tagged_fields(current_item, current_tagged, event)
                    continue
                if not current_item.get("work_date"):
                    event_date = _collapse(event.get("date") or "")
                    if event_date:
                        current_item["work_date"] = event_date
                        current_item["work_date_label"] = _collapse(event.get("date_label") or _date_label(event_date))
                if not current_item.get("vendor_name"):
                    guessed_vendor = _guess_vendor(text_line, sender=str(event.get("sender") or ""))
                    if guessed_vendor:
                        current_item["vendor_name"] = guessed_vendor
                if not current_item.get("location_name"):
                    guessed_location = _guess_location(text_line)
                    if guessed_location:
                        current_item["location_name"] = guessed_location
                if any(keyword in text_line for keyword in ("업체", "시공사", "작업일자", "작업일시", "대상", "위치", "현장", "장소")):
                    if not _should_skip_context_line(text_line):
                        _append_summary(current_item, text_line)
                    continue
                if len(text_line) >= 8 and not _should_skip_context_line(text_line):
                    _append_summary(current_item, text_line)

        if current_item and (pending_image_notices or pending_attachment_count > 0 or pending_attachment_texts):
            apply_pending(current_item)
        flush_current()
        _report_progress(
            progress_callback,
            current_step=3,
            summary="원문에서 찾은 작업 후보와 사진/첨부를 보수적으로 매칭하고 있습니다." if not defer_image_matching else "원문에서 찾은 작업 후보를 정리하고 사진 선택 단계를 준비하고 있습니다.",
            hint="근거가 약한 사진은 억지로 붙이지 않고 미매칭으로 남깁니다." if not defer_image_matching else "사진 매칭은 아직 실행하지 않고 작업 후보만 먼저 정리합니다.",
        )
        explicit_image_matches, remaining_images = _assign_images_by_notices(explicit_items, image_entries) if not defer_image_matching else ({}, list(image_entries))
        items = _merge_explicit_items(explicit_items, explicit_image_matches)
        if not items and attachments:
            for entry in attachments:
                metadata = _attachment_metadata(entry)
                title = _collapse(metadata.get("title") or "")
                if not title:
                    continue
                items.append(
                    {
                        "index": len(items) + 1,
                        "title": title,
                        "summary": title,
                        "work_date": _collapse(metadata.get("work_date") or ""),
                        "work_date_label": _collapse(metadata.get("work_date_label") or ""),
                        "vendor_name": _collapse(metadata.get("vendor_name") or ""),
                        "location_name": _collapse(metadata.get("location_name") or ""),
                        "confidence": "attachment-fallback",
                        "images": [],
                        "attachments": [],
                        "_expected_attachment_count": 1,
                        "_attachment_notice_texts": [],
                        "_attachment_notice_tokens": [],
                    }
                )

        report_title = _sample_heading(sample_title, sample_lines)
        period_label = _sample_period(sample_lines)
        analysis_notice = " ".join(openai_failure_notes) if openai_failure_notes else ""
        analysis_model = "heuristic"
        analysis_reason = next((row["reason"] for row in openai_failures if _collapse(row.get("reason") or "")), "")

        attachment_entries = [
            {
                "index": index,
                "filename": row.get("filename"),
                "preview_text": _collapse(row.get("preview_text") or ""),
                "metadata": _attachment_metadata(row),
            }
            for index, row in enumerate(attachments, start=1)
        ]
        unmatched_image_indexes = [int(row.get("index") or 0) for row in remaining_images if int(row.get("index") or 0) > 0]
        unmatched_attachment_indexes = []
        if items:
            image_matches = {
                int(item["index"]): list(item.get("images") or [])
                for item in items
            }
            attachment_matches, remaining_attachments = _assign_entries_by_notice_tokens(items, attachment_entries, "_attachment_notice_tokens")
            if any(int(item.get("_expected_attachment_count") or 0) > 0 for item in items) and not any(
                item.get("_attachment_notice_tokens") for item in items
            ):
                attachment_matches, remaining_attachments = _assign_entries_by_expected_counts(items, attachment_entries, "_expected_attachment_count")
            if remaining_images and not defer_image_matching:
                score_matches = _assign_entries(items, remaining_images, allow_chunk_fallback=False, feedback_profile=tenant_feedback_profile)
                for item_index, rows in score_matches.items():
                    image_matches[item_index].extend(rows)
                matched_indexes = {
                    int(row.get("index") or 0)
                    for rows in score_matches.values()
                    for row in rows
                    if int(row.get("index") or 0) > 0
                }
                unmatched_image_indexes = [
                    int(row.get("index") or 0)
                    for row in remaining_images
                    if int(row.get("index") or 0) > 0 and int(row.get("index") or 0) not in matched_indexes
                ]
            if remaining_attachments:
                score_matches = _assign_entries(items, remaining_attachments, feedback_profile=tenant_feedback_profile)
                for item_index, rows in score_matches.items():
                    attachment_matches[item_index].extend(rows)
                matched_attachment_indexes = {
                    int(row.get("index") or 0)
                    for rows in score_matches.values()
                    for row in rows
                    if int(row.get("index") or 0) > 0
                }
                unmatched_attachment_indexes = [
                    int(row.get("index") or 0)
                    for row in remaining_attachments
                    if int(row.get("index") or 0) > 0 and int(row.get("index") or 0) not in matched_attachment_indexes
                ]
            for item in items:
                item["images"] = _finalize_image_stages(image_matches.get(int(item["index"]), []))
                raw_attachments = sorted(attachment_matches.get(int(item["index"]), []), key=lambda row: int(row.get("index") or 0))
                _apply_attachment_metadata(item, raw_attachments)
                item["attachments"] = [
                    {
                        "index": int(row.get("index") or 0),
                        "filename": row.get("filename"),
                        "preview_text": _collapse(row.get("preview_text") or ""),
                    }
                    for row in raw_attachments
                ]
                item.pop("_expected_attachment_count", None)
                item.pop("_image_notices", None)
                item.pop("_attachment_notice_texts", None)
                item.pop("_attachment_notice_tokens", None)
        if defer_image_matching:
            selection_notice = "불필요한 대화를 제외한 뒤 사진 포함 항목과 사용할 이미지를 고르면, 선택된 범위에만 AI 이미지 매칭을 실행합니다."
            analysis_notice = selection_notice if not analysis_notice else f"{analysis_notice} {selection_notice}"

    _report_progress(
        progress_callback,
        current_step=4,
        summary="미리보기 결과를 정리하고 있습니다.",
        hint="사진 선별 출력과 PDF 재사용에 필요한 결과를 저장합니다.",
    )

    return _finalize_work_report_result(
        normalized_text=normalized_text,
        sample_title=sample_title,
        report_title=report_title,
        period_label=period_label,
        items=items,
        images=images,
        attachments=attachments,
        image_entries=image_entries,
        unmatched_image_indexes=unmatched_image_indexes,
        unmatched_attachment_indexes=unmatched_attachment_indexes,
        analysis_notice=analysis_notice,
        analysis_model=analysis_model,
        analysis_reason=analysis_reason,
        analysis_diagnostics=analysis_diagnostics,
        openai_failures=openai_failures,
        tenant_feedback_profile=tenant_feedback_profile,
        analysis_stage="extract_only" if defer_image_matching else "image_matched",
        selected_image_item_indexes=sorted(selected_item_index_set),
        use_chunked_image_matching=use_chunked_image_matching,
        reference_images=reference_images,
    )
