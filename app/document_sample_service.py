from __future__ import annotations

import os
import re
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


def _extract_hwp_preview(file_bytes: bytes) -> Dict[str, Any]:
    stream = BytesIO(file_bytes)
    ole = olefile.OleFileIO(stream)
    try:
        preview_text = ""
        preview_image = b""
        if ole.exists("PrvText"):
            preview_text = _decode_prv_text(ole.openstream("PrvText").read())
        if ole.exists("PrvImage"):
            preview_image = ole.openstream("PrvImage").read()
        lines = [line.rstrip() for line in preview_text.splitlines() if _collapse(line)]
        return {
            "kind": "hwp",
            "preview_text": preview_text,
            "lines": lines,
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
