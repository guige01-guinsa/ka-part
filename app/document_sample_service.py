from __future__ import annotations

import os
import re
import zlib
from io import BytesIO
from typing import Any, Dict, List

import olefile


def _collapse(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _split_preview_fields(line: str) -> List[str]:
    items = [_collapse(match) for match in re.findall(r"<([^>]*)>", str(line or "")) if _collapse(match)]
    if items:
        return items
    return [_collapse(line)] if _collapse(line) else []


def _title_from_preview(lines: List[str], fallback: str) -> str:
    for line in lines:
        fields = _split_preview_fields(line)
        if not fields:
            continue
        if fields[0].replace(" ", "") in {"제목", "제목>", "제목<"} and len(fields) >= 2:
            return _collapse(fields[1]) or fallback
    return fallback


def _decode_prv_text(data: bytes) -> str:
    if not data:
        return ""
    for encoding in ("utf-16le", "utf-16"):
        try:
            return data.decode(encoding, errors="replace")
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")


def _clean_hwp_body_text(text: str) -> str:
    normalized = "".join(ch for ch in str(text or "") if ch in "\r\n\t" or ord(ch) >= 32)
    for index, ch in enumerate(normalized):
        if ch in "[<" or ("가" <= ch <= "힣") or ch.isdigit():
            tail = normalized[index : index + 24]
            printable = sum(
                1
                for value in tail
                if value == " " or value.isalnum() or ("가" <= value <= "힣") or value in "[]<>:/-(),."
            )
            if printable >= 8:
                return normalized[index:].strip()
    return normalized.strip()


def _iter_hwp_section_texts(ole: olefile.OleFileIO, compressed: bool) -> List[str]:
    section_names = []
    for entry in ole.listdir():
        if len(entry) == 2 and entry[0] == "BodyText" and entry[1].startswith("Section"):
            section_names.append("/".join(entry))
    section_names.sort(key=lambda value: int(value.split("Section", 1)[1]))

    lines: List[str] = []
    for section_name in section_names:
        raw = ole.openstream(section_name).read()
        section_data = zlib.decompress(raw, -15) if compressed else raw
        position = 0
        while position + 4 <= len(section_data):
            header = int.from_bytes(section_data[position : position + 4], "little")
            tag_id = header & 0x3FF
            size = (header >> 20) & 0xFFF
            position += 4
            if size == 0xFFF:
                if position + 4 > len(section_data):
                    break
                size = int.from_bytes(section_data[position : position + 4], "little")
                position += 4
            payload = section_data[position : position + size]
            position += size
            if tag_id != 67:
                continue
            text = _clean_hwp_body_text(payload.decode("utf-16le", errors="ignore"))
            if not text:
                continue
            lines.extend(line.rstrip() for line in text.splitlines() if _collapse(line))
    return lines


def _extract_hwp_preview(file_bytes: bytes) -> Dict[str, Any]:
    stream = BytesIO(file_bytes)
    ole = olefile.OleFileIO(stream)
    try:
        preview_text = ""
        preview_image = b""
        body_lines: List[str] = []
        if ole.exists("PrvText"):
            preview_text = _decode_prv_text(ole.openstream("PrvText").read())
        if ole.exists("PrvImage"):
            preview_image = ole.openstream("PrvImage").read()
        try:
            header = ole.openstream("FileHeader").read()
            flags = int.from_bytes(header[36:40], "little") if len(header) >= 40 else 0
            body_lines = _iter_hwp_section_texts(ole, compressed=bool(flags & 0x01))
        except Exception:
            body_lines = []
        preview_lines = [line.rstrip() for line in preview_text.splitlines() if _collapse(line)]
        lines = body_lines or preview_lines
        return {
            "kind": "hwp",
            "preview_text": preview_text,
            "lines": lines,
            "preview_lines": preview_lines,
            "body_line_count": len(body_lines),
            "preview_image_bytes": preview_image,
        }
    finally:
        ole.close()


def extract_document_sample(file_name: str, file_bytes: bytes) -> Dict[str, Any]:
    raw_name = str(file_name or "").strip() or "sample"
    ext = os.path.splitext(raw_name)[1].lower()
    fallback_title = os.path.splitext(os.path.basename(raw_name))[0] or "문서 샘플"

    if ext == ".hwp":
        item = _extract_hwp_preview(file_bytes)
        item["title"] = _title_from_preview(item.get("lines") or [], fallback_title)
        item["source_name"] = raw_name
        return item

    if ext in {".txt", ".md"}:
        text = file_bytes.decode("utf-8", errors="replace")
        lines = [line.rstrip() for line in text.splitlines() if _collapse(line)]
        return {
            "kind": "text",
            "title": fallback_title,
            "source_name": raw_name,
            "preview_text": text,
            "lines": lines,
            "preview_image_bytes": b"",
        }

    raise ValueError("지원하지 않는 샘플 문서 형식입니다. 현재는 HWP, TXT, MD만 가능합니다.")
