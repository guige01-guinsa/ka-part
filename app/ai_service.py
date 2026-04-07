from __future__ import annotations

import json
import os
import re
from datetime import datetime
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
    if prefix:
        return f"{' '.join(prefix)} {headline}".strip()
    return headline


def _openai_classify(text: str) -> Dict[str, str] | None:
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None
    try:
        from openai import OpenAI
    except Exception:
        return None
    model = str(os.getenv("OPENAI_MODEL") or "gpt-5").strip()
    client = OpenAI(api_key=api_key)
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
            "summary": summary or _make_summary(text, complaint_type),
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
        "summary": _make_summary(normalized, complaint_type),
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


def analyze_chat_digest(text: str) -> Dict[str, Any]:
    normalized = _collapse_space(text)
    if not normalized:
        raise ValueError("text is required")

    rows: List[Dict[str, Any]] = []
    seen = set()
    today = datetime.now().replace(microsecond=0).isoformat(sep=" ")

    for raw_line in str(text or "").splitlines():
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
            lines.append(f"- {location or '위치미상'} / {row['summary']}")
    else:
        lines.append("없음")
    lines.extend(["", "🔧 주요 민원"])
    if major_rows:
        for row in major_rows:
            lines.append(f"- {row['type']} / {row['summary']} / {row['status']}")
    else:
        lines.append("없음")
    lines.extend(["", "📌 내일 처리"])
    if tomorrow_rows:
        for row in tomorrow_rows:
            lines.append(f"- {row['summary']} / {row['status']}")
    else:
        lines.append("없음")
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

    return {
        "total": total,
        "done": done,
        "pending": pending,
        "carry": carry,
        "urgent_items": urgent_rows,
        "major_items": major_rows,
        "tomorrow_items": tomorrow_rows,
        "excel_rows": rows,
        "report_text": "\n".join(lines),
    }
