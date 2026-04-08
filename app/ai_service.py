from __future__ import annotations

import base64
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .engine_db import COMPLAINT_TYPES, STATUS_VALUES, URGENCY_VALUES

TYPE_KEYWORDS: List[Tuple[str, Tuple[str, ...]]] = [
    ("승강기", ("엘리베이터", "승강기", "갇힘", "멈춤")),
    ("누수", ("누수", "물이 새", "물샘", "물이 샌", "물이세", "물 떨어", "새고")),
    ("전기", ("정전", "전기", "차단기", "전등", "조명", "누전", "콘센트")),
    ("수도", ("수도", "단수", "수압", "수돗물", "배수", "하수", "배관")),
    ("주차", ("주차", "차량", "이중주차", "주차장", "차단기", "주차선")),
    ("소음", ("소음", "층간", "시끄", "쿵쿵", "공사소음", "악취")),
    ("시설", ("문고리", "출입문", "현관문", "천장", "외벽", "시설", "파손", "고장", "조경")),
    ("미화", ("청소", "쓰레기", "미화", "오물", "냄새", "분리수거")),
    ("경비", ("경비", "보안", "외부인", "출입통제", "순찰")),
    ("관리비", ("관리비", "고지서", "납부", "부과", "정산")),
]
URGENT_KEYWORDS = ("긴급", "응급", "갇힘", "멈", "정전", "누수", "물이 새", "물이 샌", "화재", "불", "연기")
SAME_DAY_KEYWORDS = ("오늘", "당일", "지금", "빨리", "즉시", "빠르게")
QUESTION_KEYWORDS = ("문의", "가능한가", "가능한지", "어떻게", "알려", "확인 부탁", "확인부탁")
STATUS_KEYWORDS = {
    "완료": ("완료", "처리완료", "해결", "조치완료"),
    "처리중": ("처리중", "진행중", "출동", "확인중"),
    "이월": ("이월", "내일", "다음날"),
}
MAX_CHAT_DIGEST_IMAGES = 30


def _collapse_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u0000", " ")).strip()


def _extract_building_unit(text: str) -> Tuple[str, str]:
    normalized = _collapse_space(text)
    building = ""
    unit = ""
    m = re.search(r"(\d{2,4})\s*동", normalized)
    if m:
        building = m.group(1)
    m = re.search(r"(\d{2,4})\s*호", normalized)
    if m:
        unit = m.group(1)
    if not unit:
        m = re.search(r"(\d{2,4})[-/](\d{2,4})", normalized)
        if m:
            building = building or m.group(1)
            unit = m.group(2)
    return building, unit


def _infer_type(text: str) -> str:
    lowered = _collapse_space(text)
    for complaint_type, keywords in TYPE_KEYWORDS:
        if any(keyword in lowered for keyword in keywords):
            return complaint_type
    return "기타"


def _infer_urgency(text: str, complaint_type: str) -> str:
    lowered = _collapse_space(text)
    if complaint_type in {"승강기", "누수", "전기"} and any(keyword in lowered for keyword in URGENT_KEYWORDS):
        return "긴급"
    if any(keyword in lowered for keyword in URGENT_KEYWORDS):
        return "긴급"
    if any(keyword in lowered for keyword in QUESTION_KEYWORDS):
        return "단순문의"
    if any(keyword in lowered for keyword in SAME_DAY_KEYWORDS):
        return "당일"
    return "일반"


def _make_summary(text: str, complaint_type: str) -> str:
    normalized = _collapse_space(text)
    building, unit = _extract_building_unit(normalized)
    prefix = []
    if building:
        prefix.append(f"{building}동")
    if unit:
        prefix.append(f"{unit}호")
    headline = normalized
    if len(headline) > 44:
        headline = headline[:44].rstrip() + "..."
    if complaint_type != "기타" and complaint_type not in headline:
        headline = f"{complaint_type} / {headline}"
    if prefix and not any(part and part in headline for part in prefix):
        return f"{' '.join(prefix)} {headline}".strip()
    return headline


def normalize_summary_text(summary: str, *, building: str = "", unit: str = "", complaint_type: str = "") -> str:
    normalized = _collapse_space(summary)
    if not normalized:
        return normalized
    prefix_parts = []
    if building:
        prefix_parts.append(f"{str(building).strip()}동")
    if unit:
        prefix_parts.append(f"{str(unit).strip()}호")
    prefix = " ".join(part for part in prefix_parts if part).strip()
    if prefix:
        normalized = re.sub(rf"^(?:{re.escape(prefix)}\s+)+", f"{prefix} ", normalized).strip()
    if complaint_type and complaint_type != "기타":
        tag = f"{complaint_type} / "
        normalized = re.sub(rf"^(?:{re.escape(tag)})+", tag, normalized).strip()
    return normalized


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


def _openai_classify(text: str) -> Dict[str, str] | None:
    client, model = _openai_client()
    if not client:
        return None
    prompt = f"""
너는 아파트 관리사무소 민원 분류 시스템이다.
반드시 JSON으로만 답하라.

유형: {", ".join(COMPLAINT_TYPES)}
긴급도: {", ".join(URGENCY_VALUES)}

출력 형식:
{{"type":"","urgency":"","summary":""}}

민원:
{text}
""".strip()
    try:
        response = client.responses.create(model=model, input=prompt)
        raw = str(getattr(response, "output_text", "") or "").strip()
        if not raw:
            return None
        data = json.loads(raw)
        complaint_type = str(data.get("type") or "").strip()
        urgency = str(data.get("urgency") or "").strip()
        summary = _collapse_space(data.get("summary") or "")
        if complaint_type not in COMPLAINT_TYPES or urgency not in URGENCY_VALUES:
            return None
        return {
            "type": complaint_type,
            "urgency": urgency,
            "summary": normalize_summary_text(summary or _make_summary(text, complaint_type), complaint_type=complaint_type),
            "model": model,
            "source": "openai",
        }
    except Exception:
        return None


def classify_complaint_text(text: str) -> Dict[str, str]:
    normalized = _collapse_space(text)
    if not normalized:
        raise ValueError("text is required")
    ai_result = _openai_classify(normalized)
    if ai_result:
        return ai_result
    complaint_type = _infer_type(normalized)
    urgency = _infer_urgency(normalized, complaint_type)
    return {
        "type": complaint_type,
        "urgency": urgency,
        "summary": normalize_summary_text(_make_summary(normalized, complaint_type), complaint_type=complaint_type),
        "model": "heuristic",
        "source": "fallback",
    }


def _infer_status(text: str) -> str:
    normalized = _collapse_space(text)
    for status, keywords in STATUS_KEYWORDS.items():
        if any(keyword in normalized for keyword in keywords):
            return status
    return "접수"


def _guess_manager(text: str) -> str:
    normalized = _collapse_space(text)
    m = re.search(r"담당[:\s]+([가-힣A-Za-z]{2,10})", normalized)
    if m:
        return m.group(1)
    return ""


def _summary_for_report(row: Dict[str, Any]) -> str:
    summary = _collapse_space(row.get("summary") or row.get("content") or "")
    complaint_type = _collapse_space(row.get("type") or "")
    prefix = f"{complaint_type} / "
    if complaint_type and summary.startswith(prefix):
        return summary[len(prefix):].strip()
    return summary


def _normalize_chat_line(line: str) -> str:
    text = _collapse_space(line)
    text = re.sub(r"^\d{4}년\s*\d{1,2}월\s*\d{1,2}일\s*(오전|오후)?\s*\d{1,2}:\d{2},?\s*", "", text)
    text = re.sub(r"^\d{4}[./-]\d{1,2}[./-]\d{1,2}\s+\d{1,2}:\d{2}\s*", "", text)
    text = re.sub(r"^[^:]{1,20}\s*:\s*", "", text)
    return _collapse_space(text)


def _looks_like_issue(text: str) -> bool:
    if len(text) < 6:
        return False
    if any(keyword in text for _, keywords in TYPE_KEYWORDS for keyword in keywords):
        return True
    if any(keyword in text for keyword in URGENT_KEYWORDS):
        return True
    return "민원" in text or "고장" in text or "불편" in text


def _image_data_url(image_item: Dict[str, Any]) -> str:
    raw = image_item.get("bytes")
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        return ""
    mime = str(image_item.get("content_type") or "image/jpeg").strip() or "image/jpeg"
    encoded = base64.b64encode(bytes(raw)).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _openai_image_digest(text: str, image_inputs: List[Dict[str, Any]]) -> Tuple[List[str], List[str], str]:
    client, model = _openai_client()
    if not client or not image_inputs:
        return [], [], ""

    prompt = """
너는 아파트 관리사무소 카카오톡 대화 정리 도우미다.
입력으로 텍스트 대화와 카카오톡 캡처, 현장 사진이 함께 들어온다.

해야 할 일:
1. 이미지 안에서 민원성 텍스트나 시설 이상 징후를 읽어라.
2. 중복 제거 전 원시 민원 문장(image_lines)을 1줄씩 만들어라.
3. 운영자가 빠르게 확인할 수 있는 이미지 요약(image_notes)을 만들어라.
4. 반드시 JSON으로만 답하라.

출력 형식:
{"image_lines":[""],"image_notes":[""]}

원칙:
- image_lines는 민원 등록에 바로 쓸 수 있는 짧은 문장으로 작성한다.
- 동/호, 시설명, 장애 상태가 보이면 포함한다.
- 불명확한 경우 추정이라고 쓰지 말고 image_notes에만 적는다.
- 민원이 아니면 image_lines에는 넣지 않는다.
""".strip()
    content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
    if _collapse_space(text):
        content.append({"type": "input_text", "text": f"기존 텍스트 대화:\n{text}"})

    for index, image_item in enumerate(image_inputs, start=1):
        filename = str(image_item.get("filename") or f"image-{index}").strip()
        if filename:
            content.append({"type": "input_text", "text": f"이미지 {index} 파일명: {filename}"})
        data_url = _image_data_url(image_item)
        if data_url:
            content.append({"type": "input_image", "image_url": data_url})

    try:
        response = client.responses.create(model=model, input=[{"role": "user", "content": content}])
        raw = str(getattr(response, "output_text", "") or "").strip()
        if not raw:
            return [], [], ""
        data = json.loads(raw)
        image_lines = [_collapse_space(item) for item in data.get("image_lines") or [] if _collapse_space(item)]
        image_notes = [_collapse_space(item) for item in data.get("image_notes") or [] if _collapse_space(item)]
        return image_lines, image_notes, model
    except Exception:
        return [], [], ""


def _fallback_image_digest(image_inputs: List[Dict[str, Any]]) -> Tuple[List[str], List[str], str]:
    image_lines: List[str] = []
    image_notes: List[str] = []

    for index, image_item in enumerate(image_inputs, start=1):
        filename = str(image_item.get("filename") or f"image-{index}").strip()
        stem = _collapse_space(Path(filename).stem.replace("-", " ").replace("_", " "))
        if _looks_like_issue(stem):
            complaint_type = _infer_type(stem)
            image_lines.append(stem)
            image_notes.append(f"{filename}: {_make_summary(stem, complaint_type)}")
            continue
        image_notes.append(f"{filename}: 이미지 첨부 {index}건")

    if image_inputs and not any(_looks_like_issue(_collapse_space(Path(str(item.get('filename') or '')).stem.replace('-', ' ').replace('_', ' '))) for item in image_inputs):
        image_notes.append("이미지 상세 인식은 OPENAI_API_KEY 설정 시 더 정확해집니다.")
    return image_lines, image_notes, "filename-fallback"


def _digest_image_lines(text: str, image_inputs: List[Dict[str, Any]]) -> Tuple[List[str], List[str], str]:
    trimmed = image_inputs[:MAX_CHAT_DIGEST_IMAGES]
    image_lines, image_notes, model = _openai_image_digest(text, trimmed)
    if image_lines or image_notes:
        return image_lines, image_notes, model
    return _fallback_image_digest(trimmed)


def _build_digest_analysis_notice(
    *,
    total: int,
    input_image_count: int,
    image_model: str,
    source_text: str,
) -> str:
    normalized_text = _collapse_space(source_text)
    if input_image_count and image_model == "filename-fallback":
        if total > 0:
            return "이미지 본문 OCR 대신 파일명 기반 보조 분석으로 정리했습니다."
        return "이미지 본문을 읽지 못해 파일명 기준으로만 확인했습니다. 실제 카톡 캡처는 선명한 원본 PNG/JPG로 다시 넣어 보세요."
    if input_image_count and image_model and total == 0:
        return "이미지는 읽었지만 민원성 문장이 확인되지 않았습니다."
    if input_image_count and image_model:
        return f"이미지 OCR 분석을 사용했습니다. ({image_model})"
    if normalized_text and total == 0:
        return "텍스트를 분석했지만 민원으로 분류할 문장이 확인되지 않았습니다."
    return ""


def analyze_chat_digest(text: str, image_inputs: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    image_inputs = list(image_inputs or [])
    normalized = _collapse_space(text)
    if not normalized and not image_inputs:
        raise ValueError("text or image is required")

    image_lines, image_notes, image_model = _digest_image_lines(normalized, image_inputs)
    rows: List[Dict[str, Any]] = []
    seen = set()
    today = datetime.now().replace(microsecond=0).isoformat(sep=" ")
    raw_sources = list(str(text or "").splitlines()) + image_lines

    for raw_line in raw_sources:
        line = _normalize_chat_line(raw_line)
        if not line:
            continue
        if any(marker in line for marker in ("카카오톡 대화", "저장한 날짜", "---------------")):
            continue
        if not _looks_like_issue(line):
            continue
        classified = classify_complaint_text(line)
        building, unit = _extract_building_unit(line)
        status = _infer_status(line)
        manager = _guess_manager(line)
        row = {
            "received_at": today,
            "building": building,
            "unit": unit,
            "type": classified["type"],
            "summary": classified["summary"],
            "urgency": classified["urgency"],
            "status": status if status in STATUS_VALUES else "접수",
            "manager": manager,
            "content": line,
        }
        dedupe_key = (row["building"], row["unit"], row["type"], row["summary"])
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        rows.append(row)

    total = len(rows)
    done = sum(1 for row in rows if row["status"] == "완료")
    carry = sum(1 for row in rows if row["status"] == "이월")
    pending = total - done - carry
    urgent_rows = [row for row in rows if row["urgency"] == "긴급" and row["status"] != "완료"]
    major_rows = sorted(
        rows,
        key=lambda row: (
            0 if row["urgency"] == "긴급" else 1,
            0 if row["status"] != "완료" else 1,
            row["type"],
            row["summary"],
        ),
    )[:10]
    tomorrow_rows = [row for row in rows if row["status"] in {"접수", "처리중", "이월"}][:10]

    lines = [
        "📊 일일 요약",
        f"총 민원: {total}",
        f"완료: {done}",
        f"진행: {pending}",
        f"이월: {carry}",
        "",
        "🚨 긴급 민원",
    ]
    if urgent_rows:
        for row in urgent_rows:
            location = " ".join(x for x in (f"{row['building']}동" if row["building"] else "", f"{row['unit']}호" if row["unit"] else "") if x)
            lines.append(f"- {location or '위치미상'} / {_summary_for_report(row)}")
    else:
        lines.append("없음")
    lines.extend(["", "🔧 주요 민원"])
    if major_rows:
        for row in major_rows:
            lines.append(f"- {row['type']} / {_summary_for_report(row)} / {row['status']}")
    else:
        lines.append("없음")
    lines.extend(["", "📌 내일 처리"])
    if tomorrow_rows:
        for row in tomorrow_rows:
            lines.append(f"- {_summary_for_report(row)} / {row['status']}")
    else:
        lines.append("없음")
    if image_notes:
        lines.extend(["", "🖼 첨부 이미지 요약"])
        for note in image_notes[:10]:
            lines.append(f"- {note}")
    lines.extend(["", "📋 엑셀 입력용 리스트", "접수일시 | 동 | 호 | 민원유형 | 내용요약 | 긴급도 | 상태 | 담당자추정"])
    for row in rows:
        lines.append(
            " | ".join(
                [
                    row["received_at"],
                    row["building"] or "",
                    row["unit"] or "",
                    row["type"],
                    row["summary"],
                    row["urgency"],
                    row["status"],
                    row["manager"] or "",
                ]
            )
        )

    analysis_notice = _build_digest_analysis_notice(
        total=total,
        input_image_count=len(image_inputs),
        image_model=image_model,
        source_text=normalized,
    )

    return {
        "total": total,
        "done": done,
        "pending": pending,
        "carry": carry,
        "urgent_items": urgent_rows,
        "major_items": major_rows,
        "tomorrow_items": tomorrow_rows,
        "excel_rows": rows,
        "image_lines": image_lines,
        "image_notes": image_notes,
        "input_image_count": len(image_inputs),
        "image_analysis_model": image_model,
        "analysis_notice": analysis_notice,
        "report_text": "\n".join(lines),
    }
