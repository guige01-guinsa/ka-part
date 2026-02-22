from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from .schema_defs import SCHEMA_DEFS, SCHEMA_TAB_ORDER

LOG = logging.getLogger(__name__)
_WEASYPRINT_HTML_CLASS: Any | None = None
_WEASYPRINT_AVAILABLE: bool | None = None

def today_ymd() -> str:
    return dt.date.today().isoformat()

def safe_ymd(s: str) -> str:
    if not s:
        return today_ymd()
    try:
        return dt.date.fromisoformat(str(s)[:10]).isoformat()
    except Exception:
        return today_ymd()

def _flatten_tabs(tabs: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for tab_key, fields in (tabs or {}).items():
        if not isinstance(fields, dict):
            continue
        for k, v in fields.items():
            out[f"{tab_key}.{k}"] = str(v)
    return out


def _ordered_export_keys(rows: List[Dict[str, Any]], schema_defs: Dict[str, Dict[str, Any]] | None = None) -> List[str]:
    """Keep export column order stable and schema-driven."""
    source = schema_defs if isinstance(schema_defs, dict) else SCHEMA_DEFS
    present = set()
    for r in rows:
        flat = _flatten_tabs(r.get("tabs") or {})
        present.update(flat.keys())

    ordered: List[str] = []
    tab_order = [t for t in SCHEMA_TAB_ORDER if t in source] + [t for t in source.keys() if t not in SCHEMA_TAB_ORDER]
    for tab_key in tab_order:
        tab = source.get(tab_key) or {}
        for f in tab.get("fields") or []:
            key = f"{tab_key}.{f.get('k')}"
            if key in present:
                ordered.append(key)
                present.remove(key)

    ordered.extend(sorted(present))
    return ordered

def build_excel(
    site_name: str,
    date_from: str,
    date_to: str,
    rows: List[Dict[str, Any]],
    *,
    schema_defs: Dict[str, Dict[str, Any]] | None = None,
) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "entries"

    export_keys = _ordered_export_keys(rows, schema_defs=schema_defs)
    include_work_type = any(str(r.get("work_type") or "").strip() for r in rows)
    flattened = [
        (
            r.get("entry_date", ""),
            str(r.get("work_type") or "").strip(),
            _flatten_tabs(r.get("tabs") or {}),
        )
        for r in rows
    ]
    headers = ["entry_date"] + (["work_type"] if include_work_type else []) + export_keys
    ws.append(headers)

    for entry_date, work_type, flat in flattened:
        lead = [entry_date] + ([work_type] if include_work_type else [])
        ws.append(lead + [flat.get(k, "") for k in export_keys])

    # Automatic width optimization by real content length.
    for i, h in enumerate(headers, start=1):
        max_len = len(str(h))
        for row in ws.iter_rows(min_row=2, min_col=i, max_col=i):
            val = row[0].value
            if val is None:
                continue
            max_len = max(max_len, len(str(val)))
        ws.column_dimensions[get_column_letter(i)].width = max(10, min(48, max_len + 2))
    ws.freeze_panes = "A2"

    from io import BytesIO
    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()

def _fmt_value(value: Any, default: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else default


def _tab_value(
    tabs: Dict[str, Dict[str, Any]],
    tab_key: str,
    field_key: str,
    *,
    default: str = "-",
) -> str:
    tab = tabs.get(tab_key) if isinstance(tabs, dict) else {}
    if not isinstance(tab, dict):
        return default
    return _fmt_value(tab.get(field_key), default=default)


def _keep_pdf_first_page(pdf_bytes: bytes) -> bytes:
    raw = bytes(pdf_bytes or b"")
    if not raw:
        return raw
    try:
        from io import BytesIO
        from pypdf import PdfReader, PdfWriter
    except Exception:
        return raw
    try:
        reader = PdfReader(BytesIO(raw))
        if len(reader.pages) <= 1:
            return raw
        writer = PdfWriter()
        writer.add_page(reader.pages[0])
        out = BytesIO()
        writer.write(out)
        return out.getvalue()
    except Exception:
        return raw


def _date_label_ko(ymd: str) -> str:
    try:
        d = dt.date.fromisoformat(safe_ymd(ymd))
    except Exception:
        d = dt.date.today()
    weeks = ["월", "화", "수", "목", "금", "토", "일"]
    return f"{d.year}년 {d.month:02d}월 {d.day:02d}일 {weeks[d.weekday()]}요일"


def _build_pdf_context(
    site_name: str,
    date: str,
    tabs: Dict[str, Dict[str, Any]],
    *,
    worker_name: str = "",
) -> Dict[str, Any]:
    home = tabs.get("home") if isinstance(tabs, dict) else {}
    if not isinstance(home, dict):
        home = {}

    complex_code = _fmt_value(home.get("complex_code"), default="")
    if not complex_code:
        complex_code = _fmt_value(site_name, default="")

    lv_rows = ("L1", "L2", "L3")
    lv_cols = (
        ("V", "_V"),
        ("A", "_A"),
        ("KW", "_KW"),
    )

    def lv_block(prefix: str) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        for row_name in lv_rows:
            item: Dict[str, str] = {"phase": row_name}
            for col_name, suffix in lv_cols:
                key = f"{prefix}_{row_name}{suffix}"
                item[col_name.lower()] = _tab_value(tabs, "tr1" if prefix == "lv1" else "tr2", key)
            rows.append(item)
        return rows

    meter_main = _tab_value(tabs, "meter", "main_kwh")
    meter_industry = _tab_value(tabs, "meter", "industry_kwh")
    meter_street = _tab_value(tabs, "meter", "street_kwh")

    return {
        "title": "수변전 일지 (아파트)",
        "site_name": _fmt_value(site_name, default="-"),
        "site_code": _fmt_value(complex_code, default="-"),
        "entry_date": safe_ymd(date),
        "entry_date_label": _date_label_ko(date),
        "worker_name": _fmt_value(worker_name, default="-"),
        "work_type": _fmt_value(home.get("work_type"), default="일일"),
        "important_work": _fmt_value(home.get("important_work"), default="-"),
        "note": _fmt_value(home.get("note"), default="-"),
        "inspection_time": "10:00",
        "aiss_kv": _tab_value(tabs, "main_vcb", "main_vcb_kv"),
        "aiss_r_a": _tab_value(tabs, "meter", "AISS_L1_A"),
        "aiss_s_a": _tab_value(tabs, "meter", "AISS_L2_A"),
        "aiss_t_a": _tab_value(tabs, "meter", "AISS_L3_A"),
        # Current schema has no neutral current key; keep explicit placeholder.
        "aiss_n_a": "0",
        "tr1_temp": _tab_value(tabs, "temperature", "temperature_tr1"),
        "tr2_temp": _tab_value(tabs, "temperature", "temperature_tr2"),
        "lv1_rows": lv_block("lv1"),
        "lv2_rows": lv_block("lv2"),
        "meter_rows": [
            {"name": "메인(*720/4)", "today": meter_main, "prev": "#N/A", "daily": "", "monthly": ""},
            {"name": "산업용(13)", "today": meter_industry, "prev": "#N/A", "daily": "", "monthly": ""},
            {"name": "가로등(13)", "today": meter_street, "prev": "#N/A", "daily": "", "monthly": ""},
        ],
        "tank_apartment": _tab_value(tabs, "facility_check", "tank_level_1"),
        "tank_officetel": _tab_value(tabs, "facility_check", "tank_level_2"),
        "hydrant_pressure": _tab_value(tabs, "facility_check", "hydrant_pressure"),
        "sp_pump_pressure": _tab_value(tabs, "facility_check", "sp_pump_pressure"),
        "high_pressure": _tab_value(tabs, "facility_check", "high_pressure"),
        "low_pressure": _tab_value(tabs, "facility_check", "low_pressure"),
        "office_pressure": _tab_value(tabs, "facility_check", "office_pressure"),
        "shop_pressure": _tab_value(tabs, "facility_check", "shop_pressure"),
    }


def _render_pdf_html_template(
    site_name: str,
    date: str,
    tabs: Dict[str, Dict[str, Any]],
    *,
    worker_name: str = "",
) -> tuple[str, Path]:
    template_dir = Path(__file__).resolve().parent.parent / "templates" / "pdf"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template("substation_daily_a4.html")
    context = _build_pdf_context(site_name, date, tabs, worker_name=worker_name)
    html = tpl.render(**context)
    return html, template_dir


def _get_weasyprint_html_class():
    global _WEASYPRINT_HTML_CLASS, _WEASYPRINT_AVAILABLE
    if _WEASYPRINT_AVAILABLE is True and _WEASYPRINT_HTML_CLASS is not None:
        return _WEASYPRINT_HTML_CLASS
    if _WEASYPRINT_AVAILABLE is False:
        return None
    try:
        from weasyprint import HTML as html_class
    except Exception as e:
        _WEASYPRINT_AVAILABLE = False
        LOG.warning("WeasyPrint unavailable, using xhtml2pdf fallback: %s", e)
        return None
    _WEASYPRINT_HTML_CLASS = html_class
    _WEASYPRINT_AVAILABLE = True
    return _WEASYPRINT_HTML_CLASS


def _build_pdf_html_weasyprint(
    site_name: str,
    date: str,
    tabs: Dict[str, Dict[str, Any]],
    *,
    worker_name: str = "",
) -> bytes:
    html, template_dir = _render_pdf_html_template(site_name, date, tabs, worker_name=worker_name)
    html_class = _get_weasyprint_html_class()
    if html_class is None:
        raise RuntimeError("WeasyPrint is unavailable")
    pdf = html_class(string=html, base_url=str(template_dir)).write_pdf()
    return bytes(pdf)


def _build_pdf_html_xhtml2pdf(
    site_name: str,
    date: str,
    tabs: Dict[str, Dict[str, Any]],
    *,
    worker_name: str = "",
) -> bytes:
    from io import BytesIO
    from xhtml2pdf import pisa

    html, template_dir = _render_pdf_html_template(site_name, date, tabs, worker_name=worker_name)
    template_path = (template_dir / "substation_daily_a4.html").resolve()
    out = BytesIO()
    result = pisa.CreatePDF(src=html, dest=out, encoding="utf-8", path=str(template_path))
    if getattr(result, "err", 0):
        raise RuntimeError(f"xhtml2pdf render failed: err={result.err}")
    return out.getvalue()


def _build_pdf_legacy(
    site_name: str,
    date: str,
    tabs: Dict[str, Dict[str, str]],
    *,
    schema_defs: Dict[str, Dict[str, Any]] | None = None,
) -> bytes:
    from io import BytesIO
    bio = BytesIO()
    c = canvas.Canvas(bio, pagesize=A4)
    width, height = A4

    y = height - 40
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, y, f"수변전실 점검일지")
    y -= 22
    c.setFont("Helvetica", 11)
    c.drawString(40, y, f"단지: {site_name}    날짜: {date}")
    y -= 18
    c.line(40, y, width-40, y)
    y -= 18

    c.setFont("Helvetica", 10)
    source = schema_defs if isinstance(schema_defs, dict) else SCHEMA_DEFS
    order_map = {k: i for i, k in enumerate(SCHEMA_TAB_ORDER)}
    for tab_key in sorted((tabs or {}).keys(), key=lambda x: (order_map.get(x, 999), x)):
        c.setFont("Helvetica-Bold", 11)
        title = (source.get(tab_key) or {}).get("title") or tab_key
        c.drawString(40, y, f"[{title}]")
        y -= 14
        c.setFont("Helvetica", 10)
        fields = tabs.get(tab_key) or {}
        label_map = {
            str(f.get("k")): str(f.get("label"))
            for f in ((source.get(tab_key) or {}).get("fields") or [])
            if isinstance(f, dict) and f.get("k")
        }
        for k in sorted(fields.keys()):
            v = str(fields.get(k, ""))
            label = label_map.get(k, k)
            line = f"- {label}({k}): {v}"
            c.drawString(50, y, line[:120])
            y -= 12
            if y < 60:
                c.showPage()
                y = height - 40
                c.setFont("Helvetica", 10)
        y -= 6
        if y < 60:
            c.showPage()
            y = height - 40
            c.setFont("Helvetica", 10)

    c.showPage()
    c.save()
    return bio.getvalue()


def build_pdf(
    site_name: str,
    date: str,
    tabs: Dict[str, Dict[str, str]],
    *,
    worker_name: str = "",
    schema_defs: Dict[str, Dict[str, Any]] | None = None,
) -> bytes:
    if _get_weasyprint_html_class() is not None:
        try:
            pdf = _build_pdf_html_weasyprint(site_name, date, tabs or {}, worker_name=worker_name)
            return _keep_pdf_first_page(pdf)
        except Exception as e:
            LOG.warning("WeasyPrint render failed, trying xhtml2pdf fallback: %s", e)
    try:
        pdf = _build_pdf_html_xhtml2pdf(site_name, date, tabs or {}, worker_name=worker_name)
        return _keep_pdf_first_page(pdf)
    except Exception as e:
        LOG.warning("xhtml2pdf render failed, fallback to legacy reportlab: %s", e)
    return _keep_pdf_first_page(_build_pdf_legacy(site_name, date, tabs, schema_defs=schema_defs))
