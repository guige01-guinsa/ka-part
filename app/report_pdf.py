from __future__ import annotations

import re
from io import BytesIO
from typing import Any, Dict, Iterable, List

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

DEFAULT_FONT_NAME = "Helvetica"
_REGISTERED_FONT_NAME = ""


def _register_font() -> str:
    global _REGISTERED_FONT_NAME
    if _REGISTERED_FONT_NAME:
        return _REGISTERED_FONT_NAME
    for font_name in ("HYGothic-Medium", "HYSMyeongJo-Medium", "HeiseiKakuGo-W5"):
        try:
            pdfmetrics.registerFont(UnicodeCIDFont(font_name))
            _REGISTERED_FONT_NAME = font_name
            return font_name
        except Exception:
            continue
    _REGISTERED_FONT_NAME = DEFAULT_FONT_NAME
    return _REGISTERED_FONT_NAME


def _escape(value: Any) -> str:
    text = str(value or "").strip()
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _collapse(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _styles() -> Dict[str, ParagraphStyle]:
    font_name = _register_font()
    base_styles = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "KaTitle",
            parent=base_styles["Title"],
            fontName=font_name,
            fontSize=20,
            leading=26,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#17342F"),
            spaceAfter=8,
        ),
        "meta": ParagraphStyle(
            "KaMeta",
            parent=base_styles["Normal"],
            fontName=font_name,
            fontSize=9,
            leading=13,
            textColor=colors.HexColor("#5A6761"),
            wordWrap="CJK",
        ),
        "heading": ParagraphStyle(
            "KaHeading",
            parent=base_styles["Heading2"],
            fontName=font_name,
            fontSize=12,
            leading=16,
            textColor=colors.HexColor("#0D6A67"),
            spaceBefore=10,
            spaceAfter=6,
            wordWrap="CJK",
        ),
        "body": ParagraphStyle(
            "KaBody",
            parent=base_styles["Normal"],
            fontName=font_name,
            fontSize=10,
            leading=15,
            textColor=colors.HexColor("#172625"),
            wordWrap="CJK",
        ),
        "small": ParagraphStyle(
            "KaSmall",
            parent=base_styles["Normal"],
            fontName=font_name,
            fontSize=8.5,
            leading=11,
            textColor=colors.HexColor("#172625"),
            wordWrap="CJK",
        ),
        "caption": ParagraphStyle(
            "KaCaption",
            parent=base_styles["Normal"],
            fontName=font_name,
            fontSize=8,
            leading=11,
            textColor=colors.HexColor("#5A6761"),
            wordWrap="CJK",
        ),
    }


def _paragraphs(lines: Iterable[str], style: ParagraphStyle) -> List[Paragraph]:
    items: List[Paragraph] = []
    for line in lines:
        text = _collapse(line)
        if not text:
            continue
        items.append(Paragraph(_escape(text), style))
    return items


def _location_label(row: Dict[str, Any]) -> str:
    parts = []
    if row.get("building"):
        parts.append(f"{row['building']}동")
    if row.get("unit"):
        parts.append(f"{row['unit']}호")
    return " ".join(parts) or "위치미상"


def _append_list_section(story: List[Any], title: str, lines: Iterable[str], styles: Dict[str, ParagraphStyle]) -> None:
    story.append(Paragraph(_escape(title), styles["heading"]))
    items = list(lines)
    if not items:
        story.append(Paragraph("없음", styles["body"]))
        return
    for item in items:
        story.append(Paragraph(f"- {_escape(item)}", styles["body"]))
    story.append(Spacer(1, 3 * mm))


def _digest_table_rows(digest: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> List[List[Any]]:
    rows: List[List[Any]] = [
        [
            Paragraph("접수일시", styles["small"]),
            Paragraph("동/호", styles["small"]),
            Paragraph("민원유형", styles["small"]),
            Paragraph("내용요약", styles["small"]),
            Paragraph("긴급도", styles["small"]),
            Paragraph("상태", styles["small"]),
            Paragraph("담당자", styles["small"]),
        ]
    ]
    for item in digest.get("excel_rows") or []:
        rows.append(
            [
                Paragraph(_escape(item.get("received_at") or "-"), styles["small"]),
                Paragraph(_escape(_location_label(item)), styles["small"]),
                Paragraph(_escape(item.get("type") or "-"), styles["small"]),
                Paragraph(_escape(item.get("summary") or item.get("content") or "-"), styles["small"]),
                Paragraph(_escape(item.get("urgency") or "-"), styles["small"]),
                Paragraph(_escape(item.get("status") or "-"), styles["small"]),
                Paragraph(_escape(item.get("manager") or "-"), styles["small"]),
            ]
        )
    return rows


def _scaled_image(image_bytes: bytes, max_width: float, max_height: float) -> Image:
    reader = ImageReader(BytesIO(image_bytes))
    width, height = reader.getSize()
    if width <= 0 or height <= 0:
        raise ValueError("invalid image size")
    ratio = min(max_width / float(width), max_height / float(height))
    ratio = min(ratio, 1.0)
    image = Image(BytesIO(image_bytes))
    image.drawWidth = float(width) * ratio
    image.drawHeight = float(height) * ratio
    return image


def build_kakao_digest_pdf(
    *,
    digest: Dict[str, Any],
    tenant_label: str,
    source_text: str,
    image_inputs: List[Dict[str, Any]] | None = None,
) -> bytes:
    styles = _styles()
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=16 * mm,
        rightMargin=16 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
        title="카카오톡 대화 분석 보고서",
        author="KA-PART AI 민원처리 엔진",
    )
    story: List[Any] = []
    generated_label = _collapse((digest.get("excel_rows") or [{}])[0].get("received_at")) or "-"

    story.append(Paragraph("카카오톡 대화 분석 보고서", styles["title"]))
    meta_lines = [
        f"대상 단지: {tenant_label or '-'}",
        f"생성 시각: {generated_label}",
        f"텍스트 줄 수: {len([line for line in str(source_text or '').splitlines() if _collapse(line)])}",
        f"입력 이미지 수: {int(digest.get('input_image_count') or 0)}",
        f"이미지 분석 모델: {_collapse(digest.get('image_analysis_model') or '-')}",
    ]
    for line in meta_lines:
        story.append(Paragraph(_escape(line), styles["meta"]))
    story.append(Spacer(1, 4 * mm))

    summary_lines = [
        f"총 민원: {int(digest.get('total') or 0)}",
        f"완료: {int(digest.get('done') or 0)}",
        f"진행: {int(digest.get('pending') or 0)}",
        f"이월: {int(digest.get('carry') or 0)}",
    ]
    _append_list_section(story, "일일 요약", summary_lines, styles)

    urgent_lines = [
        f"{_location_label(item)} / {_collapse(item.get('summary') or item.get('content') or '-')}"
        for item in digest.get("urgent_items") or []
    ]
    _append_list_section(story, "긴급 민원", urgent_lines, styles)

    major_lines = [
        f"{_collapse(item.get('type') or '-')} / {_collapse(item.get('summary') or item.get('content') or '-')} / {_collapse(item.get('status') or '-')}"
        for item in digest.get("major_items") or []
    ]
    _append_list_section(story, "주요 민원", major_lines, styles)

    tomorrow_lines = [
        f"{_collapse(item.get('summary') or item.get('content') or '-')} / {_collapse(item.get('status') or '-')}"
        for item in digest.get("tomorrow_items") or []
    ]
    _append_list_section(story, "내일 처리", tomorrow_lines, styles)

    image_note_lines = list(digest.get("image_notes") or [])
    _append_list_section(story, "이미지 요약", image_note_lines, styles)

    source_preview = [
        _collapse(line)
        for line in str(source_text or "").splitlines()
        if _collapse(line)
    ][:12]
    _append_list_section(story, "원문 미리보기", source_preview, styles)

    story.append(Paragraph("엑셀 입력용 리스트", styles["heading"]))
    table = Table(
        _digest_table_rows(digest, styles),
        repeatRows=1,
        colWidths=[26 * mm, 22 * mm, 18 * mm, 64 * mm, 16 * mm, 16 * mm, 22 * mm],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E4EDE8")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#17342F")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#C8D3CE")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FBF9")]),
            ]
        )
    )
    story.append(table)

    images = list(image_inputs or [])
    if images:
        story.append(PageBreak())
        story.append(Paragraph("첨부 이미지", styles["heading"]))
        note_lookup = list(digest.get("image_notes") or [])
        for index, image_item in enumerate(images, start=1):
            filename = _collapse(image_item.get("filename") or f"image-{index}")
            caption = note_lookup[index - 1] if index - 1 < len(note_lookup) else filename
            raw = image_item.get("bytes")
            story.append(Paragraph(_escape(f"{index}. {filename}"), styles["body"]))
            if isinstance(raw, (bytes, bytearray)) and raw:
                try:
                    story.append(_scaled_image(bytes(raw), max_width=170 * mm, max_height=105 * mm))
                except Exception:
                    story.append(Paragraph("이미지 미리보기를 생성하지 못했습니다.", styles["caption"]))
            else:
                story.append(Paragraph("이미지 원본을 읽지 못했습니다.", styles["caption"]))
            story.append(Paragraph(_escape(caption), styles["caption"]))
            story.append(Spacer(1, 6 * mm))

    doc.build(story)
    return buffer.getvalue()
