from __future__ import annotations

import base64
import json
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

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
WORK_REPORT_STAGE_HINTS = {
    "before": ("before", "전", "작업전", "교체전", "시공전", "조치전", "보수전"),
    "during": ("during", "중", "작업중", "교체중", "시공중", "진행중"),
    "after": ("after", "후", "작업후", "교체후", "시공후", "조치후", "완료"),
}
MAX_WORK_REPORT_IMAGES = 24
MAX_WORK_REPORT_ATTACHMENTS = 30
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


def _collapse(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u0000", " ")).strip()


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


def _openai_client(default_model: str = "gpt-5") -> Tuple[Any | None, str]:
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None, ""
    try:
        from openai import OpenAI
    except Exception:
        return None, ""
    model = str(os.getenv("OPENAI_MODEL") or default_model).strip() or default_model
    return OpenAI(api_key=api_key), model


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
    if re.search(r"\d{2,4}-\d{3,4}\s*/\s*010-\d{4}-\d{4}", normalized):
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
    if any(pattern in normalized for pattern in ("AS요청", "요청함", "교체예정", "타이머조정", "변경 완료", "회수함")):
        return True
    return any(keyword in normalized for keyword in WORK_REPORT_ACTION_KEYWORDS)


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
        "_expected_image_count": 0,
        "_expected_attachment_count": 0,
        "_attachment_notice_texts": [],
        "_minute_of_day": int(event.get("minute_of_day") or -1),
    }


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


def _entry_tokens(entry: Dict[str, Any]) -> set[str]:
    tokens = set(_tokenize(entry.get("filename")))
    preview_text = _collapse(entry.get("preview_text") or "")
    if preview_text:
        tokens.update(_tokenize(preview_text[:200]))
    metadata = entry.get("metadata") or {}
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


def _match_score(item: Dict[str, Any], entry: Dict[str, Any]) -> int:
    item_token_set = _item_tokens(item)
    entry_token_set = _entry_tokens(entry)
    score = len(item_token_set & entry_token_set)
    item_date = str(item.get("work_date") or "")
    if item_date:
        compact = item_date.replace("-", "")
        if compact in str(entry.get("filename") or ""):
            score += 3
        month_day = item_date[5:]
        if month_day and month_day.replace("-", "") in str(entry.get("filename") or ""):
            score += 2
    vendor = _collapse(item.get("vendor_name") or "")
    if vendor and vendor in _collapse(entry.get("filename") or ""):
        score += 2
    return score


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


def _assign_entries(items: List[Dict[str, Any]], entries: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    assigned: Dict[int, List[Dict[str, Any]]] = {int(item["index"]): [] for item in items}
    unmatched: List[Dict[str, Any]] = []
    for entry in entries:
        best_item = None
        best_score = 0
        for item in items:
            score = _match_score(item, entry)
            if score > best_score:
                best_score = score
                best_item = item
        if best_item and best_score > 0:
            assigned[int(best_item["index"])].append(entry)
        else:
            unmatched.append(entry)
    if unmatched:
        fallback = _chunk_assign(unmatched, items)
        for item_index, rows in fallback.items():
            assigned[item_index].extend(rows)
    return assigned


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
        label_map = {
            "before": "작업 전",
            "during": "작업 중",
            "after": "작업 후",
            "general": "현장 이미지",
        }
        row["stage"] = stage
        row["stage_label"] = label_map.get(stage, "현장 이미지")
    return ordered


def _build_text_summary(report_title: str, period_label: str, items: List[Dict[str, Any]], unmatched_images: List[Dict[str, Any]], unmatched_attachments: List[Dict[str, Any]], analysis_notice: str) -> str:
    lines = [
        report_title or "시설팀 주요 업무 보고",
        f"보고기간: {period_label or '-'}",
        f"작업 항목 수: {len(items)}",
        f"미매칭 이미지: {len(unmatched_images)}",
        f"미매칭 첨부파일: {len(unmatched_attachments)}",
    ]
    if analysis_notice:
        lines.extend(["", f"안내: {analysis_notice}"])
    for item in items:
        lines.extend(
            [
                "",
                f"{int(item.get('index') or 0)}. {item.get('title') or '-'}",
                f"- 작업일자: {item.get('work_date_label') or item.get('work_date') or '-'}",
                f"- 업체: {item.get('vendor_name') or '-'}",
                f"- 위치: {item.get('location_name') or '-'}",
                f"- 이미지: {len(item.get('images') or [])}장",
                f"- 첨부파일: {len(item.get('attachments') or [])}건",
            ]
        )
    return "\n".join(lines)


def _openai_image_url(entry: Dict[str, Any]) -> str:
    raw = entry.get("bytes")
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        return ""
    mime = str(entry.get("content_type") or "image/jpeg").strip() or "image/jpeg"
    return f"data:{mime};base64,{base64.b64encode(bytes(raw)).decode('ascii')}"


def _openai_work_report(
    *,
    text: str,
    image_inputs: List[Dict[str, Any]],
    attachment_inputs: List[Dict[str, Any]],
    sample_title: str,
    sample_lines: Sequence[str],
) -> Dict[str, Any] | None:
    client, model = _openai_client()
    if not client:
        return None
    prompt = """
너는 아파트 관리사무소 시설팀의 주요업무보고서를 만드는 도우미다.
입력으로 카카오톡 단체방 대화, 현장 사진, 첨부파일 목록, 샘플 보고서 개요가 들어온다.

해야 할 일:
1. 작업 항목을 추출한다.
2. 각 항목별 작업내용, 작업일자, 업체, 위치, 요약을 정리한다.
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
- 모호하면 confidence를 low로 두고 analysis_notice에 이유를 남긴다.
- sample 보고서의 제목/보고기간 표현은 참고하되, 실제 입력이 다르면 입력을 우선한다.
""".strip()
    content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
    if sample_title or sample_lines:
        sample_excerpt = "\n".join(_collapse(line) for line in list(sample_lines)[:20] if _collapse(line))
        content.append({"type": "input_text", "text": f"샘플 제목: {sample_title or '-'}\n샘플 개요:\n{sample_excerpt or '-'}"})
    if _collapse(text):
        content.append({"type": "input_text", "text": f"카톡 대화 원문:\n{text}"})
    if image_inputs:
        content.append(
            {
                "type": "input_text",
                "text": "\n".join(f"[I{index}] {item.get('filename')}" for index, item in enumerate(image_inputs, start=1)),
            }
        )
        for index, item in enumerate(image_inputs[:12], start=1):
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
    try:
        response = client.responses.create(model=model, input=[{"role": "user", "content": content}])
        raw = _collapse(getattr(response, "output_text", "") or "")
        if not raw:
            return None
        data = json.loads(raw)
    except Exception:
        return None
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
    }


def analyze_work_report(
    text: str,
    *,
    image_inputs: Sequence[Dict[str, Any]] | None = None,
    attachment_inputs: Sequence[Dict[str, Any]] | None = None,
    sample_title: str = "",
    sample_lines: Sequence[str] | None = None,
) -> Dict[str, Any]:
    normalized_text = str(text or "")
    images = list(image_inputs or [])[:MAX_WORK_REPORT_IMAGES]
    attachments = list(attachment_inputs or [])[:MAX_WORK_REPORT_ATTACHMENTS]
    sample_lines = list(sample_lines or [])
    if not _collapse(normalized_text) and not images and not attachments:
        raise ValueError("text, image, or attachment is required")

    ai_result = _openai_work_report(
        text=normalized_text,
        image_inputs=images,
        attachment_inputs=attachments,
        sample_title=sample_title,
        sample_lines=sample_lines,
    )
    if ai_result:
        items = list(ai_result.get("items") or [])
        unmatched_image_indexes = list(ai_result.get("unmatched_image_indexes") or [])
        unmatched_attachment_indexes = list(ai_result.get("unmatched_attachment_indexes") or [])
        analysis_notice = _collapse(ai_result.get("analysis_notice") or "")
        analysis_model = _collapse(ai_result.get("analysis_model") or "gpt-5")
        report_title = _collapse(ai_result.get("report_title") or _sample_heading(sample_title, sample_lines))
        period_label = _collapse(ai_result.get("period_label") or _sample_period(sample_lines))
    else:
        events = _parse_kakao_events(normalized_text)
        explicit_items: List[Dict[str, Any]] = []
        current_item: Optional[Dict[str, Any]] = None
        pending_image_count = 0
        pending_attachment_count = 0
        pending_attachment_texts: List[str] = []

        def apply_pending(item: Dict[str, Any]) -> None:
            nonlocal pending_image_count, pending_attachment_count, pending_attachment_texts
            if pending_image_count > 0:
                item["_expected_image_count"] = int(item.get("_expected_image_count") or 0) + pending_image_count
                pending_image_count = 0
            if pending_attachment_count > 0:
                item["_expected_attachment_count"] = int(item.get("_expected_attachment_count") or 0) + pending_attachment_count
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

        for event in events:
            text_line = _collapse(event.get("text") or "")
            if not text_line:
                continue
            tagged = _extract_tagged_pairs(text_line)
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
                target_item = None if hold_notice_for_next_item(current_item, event) else current_item
                if target_item:
                    target_item["_expected_image_count"] = int(target_item.get("_expected_image_count") or 0) + _notice_count(text_line)
                else:
                    pending_image_count += _notice_count(text_line)
                continue
            if event.get("kind") == "file_notice":
                target_item = None if hold_notice_for_next_item(current_item, event) else current_item
                if target_item:
                    target_item["_expected_attachment_count"] = int(target_item.get("_expected_attachment_count") or 0) + _notice_count(text_line)
                    target_item["_attachment_notice_texts"].append(text_line)
                else:
                    pending_attachment_count += _notice_count(text_line)
                    pending_attachment_texts.append(text_line)
                continue
            if _looks_like_work_item(text_line):
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

        if current_item and (pending_image_count > 0 or pending_attachment_count > 0 or pending_attachment_texts):
            apply_pending(current_item)
        flush_current()

        deduped: List[Dict[str, Any]] = []
        seen_titles = set()
        for item in explicit_items:
            key = (_collapse(item.get("title") or ""), _collapse(item.get("work_date") or ""))
            if key in seen_titles:
                continue
            seen_titles.add(key)
            item["index"] = len(deduped) + 1
            deduped.append(item)

        items = deduped
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
                        "_expected_image_count": 0,
                        "_expected_attachment_count": 1,
                        "_attachment_notice_texts": [],
                    }
                )

        report_title = _sample_heading(sample_title, sample_lines)
        period_label = _sample_period(sample_lines)
        analysis_notice = ""
        analysis_model = "heuristic"

        image_entries = [
            {
                "index": index,
                "filename": row.get("filename"),
                "stage": _entry_stage(str(row.get("filename") or "")),
            }
            for index, row in enumerate(images, start=1)
        ]
        attachment_entries = [
            {
                "index": index,
                "filename": row.get("filename"),
                "preview_text": _collapse(row.get("preview_text") or ""),
                "metadata": _attachment_metadata(row),
            }
            for index, row in enumerate(attachments, start=1)
        ]

        if items:
            image_matches, remaining_images = _assign_entries_by_expected_counts(items, image_entries, "_expected_image_count")
            attachment_matches, remaining_attachments = _assign_entries_by_expected_counts(items, attachment_entries, "_expected_attachment_count")
            if remaining_images:
                score_matches = _assign_entries(items, remaining_images)
                for item_index, rows in score_matches.items():
                    image_matches[item_index].extend(rows)
            if remaining_attachments:
                score_matches = _assign_entries(items, remaining_attachments)
                for item_index, rows in score_matches.items():
                    attachment_matches[item_index].extend(rows)
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
                item.pop("_expected_image_count", None)
                item.pop("_expected_attachment_count", None)
                item.pop("_attachment_notice_texts", None)
                item.pop("_minute_of_day", None)
        unmatched_image_indexes = []
        unmatched_attachment_indexes = []

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
        {"index": index, "filename": images[index - 1].get("filename")}
        for index in unmatched_image_indexes
        if 0 < int(index) <= len(images)
    ]
    unmatched_attachments = [
        {"index": index, "filename": attachments[index - 1].get("filename")}
        for index in unmatched_attachment_indexes
        if 0 < int(index) <= len(attachments)
    ]
    report_text = _build_text_summary(report_title, period_label, items, unmatched_images, unmatched_attachments, analysis_notice)
    return {
        "report_title": report_title,
        "period_label": period_label,
        "template_title": _collapse(sample_title) or report_title,
        "analysis_model": analysis_model,
        "analysis_notice": analysis_notice,
        "item_count": len(items),
        "items": items,
        "unmatched_images": unmatched_images,
        "unmatched_attachments": unmatched_attachments,
        "source_text_preview": [_collapse(line) for line in normalized_text.splitlines() if _collapse(line)][:16],
        "report_text": report_text,
    }
