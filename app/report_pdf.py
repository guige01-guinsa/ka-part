from __future__ import annotations

import re
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, Iterable, List, Sequence

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


def _work_report_detail_table(item: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> Table:
    stages = {str(row.get("stage") or "") for row in item.get("images") or []}
    if {"before", "after"}.issubset(stages):
        image_status = "작업 전/작업 후 이미지 확인"
    elif stages:
        image_status = "전후 이미지 중 일부만 확인"
    else:
        image_status = "현장 이미지 없음"
    rows = [
        [Paragraph("작업내용", styles["small"]), Paragraph(_escape(item.get("title") or "-"), styles["body"])],
        [Paragraph("작업일자", styles["small"]), Paragraph(_escape(item.get("work_date_label") or item.get("work_date") or "-"), styles["small"])],
        [Paragraph("업체", styles["small"]), Paragraph(_escape(item.get("vendor_name") or "-"), styles["small"])],
        [Paragraph("위치", styles["small"]), Paragraph(_escape(item.get("location_name") or "-"), styles["small"])],
        [Paragraph("이미지 상태", styles["small"]), Paragraph(_escape(image_status), styles["small"])],
    ]
    summary = _collapse(item.get("summary") or "")
    if summary and summary != _collapse(item.get("title") or ""):
        rows.append([Paragraph("비고", styles["small"]), Paragraph(_escape(summary), styles["small"])])
    table = Table(rows, colWidths=[24 * mm, 146 * mm])
    table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.55, colors.HexColor("#C8D3CE")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F5F2")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return table


def _work_report_image_lookup(image_inputs: Sequence[Dict[str, Any]] | None = None) -> Dict[int, Dict[str, Any]]:
    return {index: row for index, row in enumerate(list(image_inputs or []), start=1)}


def _work_report_image_grid(item: Dict[str, Any], image_inputs: Sequence[Dict[str, Any]] | None, styles: Dict[str, ParagraphStyle]) -> Table | None:
    image_lookup = _work_report_image_lookup(image_inputs)
    matches = list(item.get("images") or [])
    if not matches:
        return None
    cells: List[List[Any]] = []
    row_cells: List[Any] = []
    for match in matches:
        image_index = int(match.get("index") or 0)
        image_item = image_lookup.get(image_index) or {}
        raw = image_item.get("bytes")
        flowables: List[Any] = [
            Paragraph(_escape(match.get("stage_label") or "현장 이미지"), styles["small"]),
        ]
        if isinstance(raw, (bytes, bytearray)) and raw:
            try:
                flowables.append(_scaled_image(bytes(raw), max_width=78 * mm, max_height=62 * mm))
            except Exception:
                flowables.append(Paragraph("이미지를 렌더링하지 못했습니다.", styles["caption"]))
        else:
            flowables.append(Paragraph("이미지 원본 없음", styles["caption"]))
        flowables.append(Paragraph(_escape(match.get("filename") or image_item.get("filename") or f"image-{image_index}"), styles["caption"]))
        row_cells.append(flowables)
        if len(row_cells) == 2:
            cells.append(row_cells)
            row_cells = []
    if row_cells:
        row_cells.append(Paragraph("", styles["caption"]))
        cells.append(row_cells)
    table = Table(cells, colWidths=[84 * mm, 84 * mm])
    table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#D5DFDA")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FBFDFC")),
            ]
        )
    )
    return table


def build_work_report_pdf(
    *,
    report: Dict[str, Any],
    tenant_label: str,
    source_text: str,
    image_inputs: Sequence[Dict[str, Any]] | None = None,
    attachment_inputs: Sequence[Dict[str, Any]] | None = None,
    template_source_name: str = "",
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
        title=_collapse(report.get("report_title") or "주요 업무 보고"),
        author="KA-PART AI 민원처리 엔진",
    )
    story: List[Any] = []
    report_title = _collapse(report.get("report_title") or "시설팀 주요 업무 보고")
    period_label = _collapse(report.get("period_label") or "-")
    story.append(Paragraph(_escape(report_title), styles["title"]))
    story.append(Paragraph(_escape(f"대상 단지: {tenant_label or '-'}"), styles["meta"]))
    story.append(Paragraph(_escape(f"보고기간: {period_label}"), styles["meta"]))
    story.append(Paragraph(_escape(f"생성시각: {datetime.now().strftime('%Y년 %m월 %d일 %H:%M')}"), styles["meta"]))
    if template_source_name:
        story.append(Paragraph(_escape(f"참조 양식: {template_source_name}"), styles["meta"]))
    story.append(Spacer(1, 4 * mm))

    overview = Table(
        [
            [Paragraph("작업 항목", styles["small"]), Paragraph(str(int(report.get("item_count") or len(report.get("items") or []))), styles["small"]), Paragraph("미매칭 이미지", styles["small"]), Paragraph(str(len(report.get("unmatched_images") or [])), styles["small"])],
            [Paragraph("미매칭 파일", styles["small"]), Paragraph(str(len(report.get("unmatched_attachments") or [])), styles["small"]), Paragraph("분석모델", styles["small"]), Paragraph(_escape(report.get("analysis_model") or "-"), styles["small"])],
        ],
        colWidths=[24 * mm, 58 * mm, 24 * mm, 58 * mm],
    )
    overview.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.55, colors.HexColor("#C8D3CE")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F5F2")),
                ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#F0F5F2")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(overview)
    notice = _collapse(report.get("analysis_notice") or "")
    if notice:
        story.append(Spacer(1, 3 * mm))
        story.append(Paragraph(_escape(notice), styles["caption"]))

    items = list(report.get("items") or [])
    if items:
        for position, item in enumerate(items, start=1):
            story.append(Spacer(1, 6 * mm))
            story.append(Paragraph(_escape(f"{position}. 주요 작업"), styles["heading"]))
            story.append(_work_report_detail_table(item, styles))
            image_grid = _work_report_image_grid(item, image_inputs, styles)
            if image_grid:
                story.append(Spacer(1, 3 * mm))
                story.append(image_grid)
            attachments = list(item.get("attachments") or [])
            if attachments:
                story.append(Spacer(1, 2 * mm))
                story.append(Paragraph("첨부파일", styles["small"]))
                for attachment in attachments:
                    preview = _collapse(attachment.get("preview_text") or "")
                    label = _collapse(attachment.get("filename") or "-")
                    text = label if not preview else f"{label} / {preview}"
                    story.append(Paragraph(f"- {_escape(text)}", styles["caption"]))
            if position < len(items):
                story.append(Spacer(1, 4 * mm))
    else:
        story.append(Spacer(1, 5 * mm))
        story.append(Paragraph("자동 분류된 작업 항목이 없습니다.", styles["body"]))

    unmatched_images = list(report.get("unmatched_images") or [])
    unmatched_attachments = list(report.get("unmatched_attachments") or [])
    if unmatched_images or unmatched_attachments:
        story.append(PageBreak())
        story.append(Paragraph("미매칭 자료", styles["heading"]))
        if unmatched_images:
            story.append(Paragraph("미매칭 이미지", styles["small"]))
            for row in unmatched_images:
                story.append(Paragraph(f"- {_escape(row.get('filename') or '-')}", styles["caption"]))
        if unmatched_attachments:
            story.append(Spacer(1, 2 * mm))
            story.append(Paragraph("미매칭 첨부파일", styles["small"]))
            for row in unmatched_attachments:
                story.append(Paragraph(f"- {_escape(row.get('filename') or '-')}", styles["caption"]))

    source_preview = [line for line in report.get("source_text_preview") or [] if _collapse(line)]
    if source_preview:
        story.append(Spacer(1, 5 * mm))
        story.append(Paragraph("원문 미리보기", styles["heading"]))
        for line in source_preview[:16]:
            story.append(Paragraph(_escape(line), styles["caption"]))

    doc.build(story)
    return buffer.getvalue()


def build_reference_document_pdf(
    *,
    title: str,
    source_name: str,
    body_lines: List[str],
    preview_image_bytes: bytes = b"",
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
        title=title or "기안서 샘플 PDF",
        author="KA-PART AI 민원처리 엔진",
    )
    story: List[Any] = []
    story.append(Paragraph(_escape(title or "기안서 샘플 PDF"), styles["title"]))
    story.append(Paragraph(_escape(f"원본 파일: {source_name or '-'}"), styles["meta"]))
    story.append(Paragraph("샘플 문서를 참조해 PDF로 재작성한 결과입니다.", styles["meta"]))
    story.append(Spacer(1, 4 * mm))

    if preview_image_bytes:
        story.append(Paragraph("원본 미리보기", styles["heading"]))
        try:
            story.append(_scaled_image(preview_image_bytes, max_width=170 * mm, max_height=235 * mm))
        except Exception:
            story.append(Paragraph("원본 미리보기 이미지를 불러오지 못했습니다.", styles["caption"]))
        story.append(Spacer(1, 6 * mm))

    story.append(Paragraph("추출 본문", styles["heading"]))
    if body_lines:
        for line in body_lines:
            story.append(Paragraph(_escape(line), styles["body"]))
            story.append(Spacer(1, 1.8 * mm))
    else:
        story.append(Paragraph("추출된 본문이 없습니다.", styles["body"]))

    doc.build(story)
    return buffer.getvalue()


def build_ops_draft_pdf(
    *,
    tenant_label: str,
    title: str,
    summary: str,
    drafter_label: str,
    reference_no: str = "",
    category: str = "",
    owner: str = "",
    due_date: str = "",
    amount_total: Any = None,
    vendor_name: str = "",
    target_label: str = "",
    basis_date: str = "",
    period_start: str = "",
    period_end: str = "",
    pdf_heading: str = "",
    request_text: str = "",
    amount_policy: str = "",
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
        title=title or "기안서",
        author="KA-PART AI 민원처리 엔진",
    )
    report_date = datetime.now().strftime("%Y년 %m월 %d일")
    safe_title = _collapse(title) or "기안서"
    safe_summary = _collapse(summary) or "상세 내용이 입력되지 않았습니다."
    safe_reference = _collapse(reference_no) or f"기안-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    safe_owner = _collapse(owner) or drafter_label or "담당자 미지정"
    safe_category = _collapse(category) or "기안"
    safe_due_date = _collapse(due_date) or "-"
    safe_vendor_name = _collapse(vendor_name) or "-"
    safe_target_label = _collapse(target_label) or "-"
    safe_basis_date = _collapse(basis_date) or "-"
    safe_period_start = _collapse(period_start) or "-"
    safe_period_end = _collapse(period_end) or "-"
    safe_heading = _collapse(pdf_heading) or "행 정 문 서"
    safe_request_text = _collapse(request_text) or "위 사항을 보고드립니다."
    safe_amount_policy = _collapse(amount_policy)
    try:
        amount_number = float(str(amount_total or "").replace(",", "").strip()) if str(amount_total or "").strip() else None
    except Exception:
        amount_number = None
    safe_amount = f"{amount_number:,.0f}원" if amount_number is not None else "-"

    story: List[Any] = []
    story.append(Paragraph(_escape(safe_heading), styles["title"]))
    story.append(Spacer(1, 3 * mm))

    header = Table(
        [
            [Paragraph("문서번호", styles["small"]), Paragraph(_escape(safe_reference), styles["small"]), Paragraph("보고일자", styles["small"]), Paragraph(_escape(report_date), styles["small"])],
            [Paragraph("기안자", styles["small"]), Paragraph(_escape(drafter_label or "-"), styles["small"]), Paragraph("담당부서", styles["small"]), Paragraph(_escape(safe_owner), styles["small"])],
            [Paragraph("문서분류", styles["small"]), Paragraph(_escape(safe_category), styles["small"]), Paragraph("기한", styles["small"]), Paragraph(_escape(safe_due_date), styles["small"])],
        ],
        colWidths=[24 * mm, 58 * mm, 24 * mm, 58 * mm],
    )
    header.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#B9C8C0")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F5F2")),
                ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#F0F5F2")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(header)
    story.append(Spacer(1, 5 * mm))

    approval = Table(
        [
            [Paragraph("결재", styles["small"]), Paragraph("담당", styles["small"]), Paragraph("과장", styles["small"]), Paragraph("소장", styles["small"]), Paragraph("회장", styles["small"])],
            ["", "", "", "", ""],
        ],
        colWidths=[20 * mm, 34 * mm, 34 * mm, 34 * mm, 34 * mm],
    )
    approval.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#B9C8C0")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F5F2")),
                ("BACKGROUND", (1, 0), (-1, 0), colors.HexColor("#FAFCFB")),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 1), (-1, 1), 18),
                ("BOTTOMPADDING", (0, 1), (-1, 1), 18),
            ]
        )
    )
    story.append(approval)
    story.append(Spacer(1, 7 * mm))

    meta_rows = []
    if safe_target_label != "-":
        meta_rows.append([Paragraph("대상", styles["small"]), Paragraph(_escape(safe_target_label), styles["small"]), Paragraph("업체/상대처", styles["small"]), Paragraph(_escape(safe_vendor_name), styles["small"])])
    elif safe_vendor_name != "-":
        meta_rows.append([Paragraph("업체/상대처", styles["small"]), Paragraph(_escape(safe_vendor_name), styles["small"]), Paragraph("", styles["small"]), Paragraph("", styles["small"])])
    if amount_number is not None or safe_amount_policy:
        meta_rows.append([Paragraph("금액", styles["small"]), Paragraph(_escape(safe_amount), styles["small"]), Paragraph("금액기준", styles["small"]), Paragraph(_escape(safe_amount_policy or "-"), styles["small"])])
    if safe_basis_date != "-" or safe_period_start != "-" or safe_period_end != "-":
        period_label = safe_period_start if safe_period_end == "-" else f"{safe_period_start} ~ {safe_period_end}"
        meta_rows.append([Paragraph("기준일", styles["small"]), Paragraph(_escape(safe_basis_date), styles["small"]), Paragraph("기간", styles["small"]), Paragraph(_escape(period_label if period_label.strip(" -") else "-"), styles["small"])])
    if meta_rows:
        story.append(Paragraph("업무정보", styles["heading"]))
        meta_table = Table(meta_rows, colWidths=[24 * mm, 58 * mm, 24 * mm, 58 * mm])
        meta_table.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#B9C8C0")),
                    ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F5F2")),
                    ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#F0F5F2")),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        story.append(meta_table)
        story.append(Spacer(1, 5 * mm))

    story.append(Paragraph("제목", styles["heading"]))
    story.append(Paragraph(_escape(safe_title), styles["body"]))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("내용", styles["heading"]))
    for line in re.split(r"(?:\r\n|\r|\n)+", str(summary or "")):
        cleaned = _collapse(line)
        if cleaned:
            story.append(Paragraph(_escape(cleaned), styles["body"]))
            story.append(Spacer(1, 1.8 * mm))
    if not _collapse(summary):
        story.append(Paragraph(_escape(safe_summary), styles["body"]))
    story.append(Spacer(1, 5 * mm))

    request_lines = [safe_request_text, f"대상 단지: {tenant_label or '-'}"]
    story.append(Paragraph("요청사항", styles["heading"]))
    for line in request_lines:
        story.append(Paragraph(_escape(line), styles["body"]))
    story.append(Spacer(1, 12 * mm))
    story.append(Paragraph(_escape(tenant_label or "관리사무소"), styles["body"]))

    doc.build(story)
    return buffer.getvalue()
