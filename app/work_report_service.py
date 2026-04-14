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
)
WORK_REPORT_VENDOR_HINTS = ("업체", "업 체", "시공사", "협력업체", "담당", "작업자")
WORK_REPORT_STAGE_HINTS = {
    "before": ("before", "전", "작업전", "교체전", "시공전", "조치전", "보수전"),
    "during": ("during", "중", "작업중", "교체중", "시공중", "진행중"),
    "after": ("after", "후", "작업후", "교체후", "시공후", "조치후", "완료"),
}
MAX_WORK_REPORT_IMAGES = 24
MAX_WORK_REPORT_ATTACHMENTS = 30


def _collapse(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u0000", " ")).strip()


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
    m = re.search(r"(?P<y>\d{4})[./-]\s*(?P<m>\d{1,2})[./-]\s*(?P<d>\d{1,2})", raw)
    if m:
        value = date(int(m.group("y")), int(m.group("m")), int(m.group("d"))).isoformat()
        month = int(m.group("m"))
        day = int(m.group("d"))
        return value, f"{month}월 {day}일"
    m = re.search(r"(?P<m>\d{1,2})\s*월\s*(?P<d>\d{1,2})\s*일", raw)
    if m:
        value = date(datetime.now().year, int(m.group("m")), int(m.group("d"))).isoformat()
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
    if "작업내용" in normalized:
        return True
    return any(keyword in normalized for keyword in WORK_REPORT_ACTION_KEYWORDS)


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
        return _collapse(m.group(2))
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
    tokens: set[str] = set()
    for field in fields:
        tokens.update(_tokenize(field))
    return tokens


def _entry_tokens(entry: Dict[str, Any]) -> set[str]:
    tokens = set(_tokenize(entry.get("filename")))
    preview_text = _collapse(entry.get("preview_text") or "")
    if preview_text:
        tokens.update(_tokenize(preview_text[:200]))
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
        messages = [_normalize_message_line(line) for line in normalized_text.splitlines()]
        explicit_items: List[Dict[str, Any]] = []
        current_item: Optional[Dict[str, Any]] = None

        for message in messages:
            text_line = _collapse(message.get("text") or "")
            if not text_line:
                continue
            tagged = _extract_tagged_pairs(text_line)
            if tagged.get("작업내용"):
                if current_item and current_item.get("title"):
                    explicit_items.append(current_item)
                current_item = {
                    "index": len(explicit_items) + 1,
                    "title": _clean_item_title(tagged.get("작업내용") or ""),
                    "summary": _clean_item_title(tagged.get("작업내용") or ""),
                    "work_date": "",
                    "work_date_label": "",
                    "vendor_name": "",
                    "location_name": _guess_location(tagged.get("작업내용") or ""),
                    "confidence": "structured",
                    "images": [],
                    "attachments": [],
                }
                continue
            if current_item and tagged:
                if tagged.get("작업일자"):
                    current_item["work_date"], current_item["work_date_label"] = _extract_date(tagged.get("작업일자") or "")
                if tagged.get("업체") or tagged.get("업체") or tagged.get("업체"):
                    current_item["vendor_name"] = _collapse(tagged.get("업체") or "")
                if tagged.get("업체") == "" and "업체" in "".join(tagged.keys()):
                    for key, value in tagged.items():
                        if "업체" in key:
                            current_item["vendor_name"] = _collapse(value)
                continue
            if _looks_like_work_item(text_line):
                work_date, work_date_label = _extract_date(text_line)
                if not work_date:
                    work_date = str(message.get("date") or "")
                    work_date_label = str(message.get("date_label") or "")
                explicit_items.append(
                    {
                        "index": len(explicit_items) + 1,
                        "title": _clean_item_title(text_line),
                        "summary": _clean_item_title(text_line),
                        "work_date": work_date,
                        "work_date_label": work_date_label,
                        "vendor_name": _guess_vendor(text_line, sender=str(message.get("sender") or "")),
                        "location_name": _guess_location(text_line),
                        "confidence": "heuristic",
                        "images": [],
                        "attachments": [],
                    }
                )

        if current_item and current_item.get("title"):
            explicit_items.append(current_item)

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
                title = _collapse(entry.get("preview_text") or Path(str(entry.get("filename") or "")).stem)
                if not title:
                    continue
                items.append(
                    {
                        "index": len(items) + 1,
                        "title": title,
                        "summary": title,
                        "work_date": "",
                        "work_date_label": "",
                        "vendor_name": "",
                        "location_name": "",
                        "confidence": "attachment-fallback",
                        "images": [],
                        "attachments": [],
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
            }
            for index, row in enumerate(attachments, start=1)
        ]

        if items:
            image_matches = _assign_entries(items, image_entries)
            attachment_matches = _assign_entries(items, attachment_entries)
            for item in items:
                item["images"] = _finalize_image_stages(image_matches.get(int(item["index"]), []))
                item["attachments"] = sorted(attachment_matches.get(int(item["index"]), []), key=lambda row: int(row.get("index") or 0))
        unmatched_image_indexes = []
        unmatched_attachment_indexes = []

    if not period_label:
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
