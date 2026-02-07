from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from .schema_defs import SCHEMA_DEFS, SCHEMA_TAB_ORDER

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


def _ordered_export_keys(rows: List[Dict[str, Any]]) -> List[str]:
    """Keep export column order stable and schema-driven."""
    present = set()
    for r in rows:
        flat = _flatten_tabs(r.get("tabs") or {})
        present.update(flat.keys())

    ordered: List[str] = []
    for tab_key in SCHEMA_TAB_ORDER:
        tab = SCHEMA_DEFS.get(tab_key) or {}
        for f in tab.get("fields") or []:
            key = f"{tab_key}.{f.get('k')}"
            if key in present:
                ordered.append(key)
                present.remove(key)

    ordered.extend(sorted(present))
    return ordered

def build_excel(site_name: str, date_from: str, date_to: str, rows: List[Dict[str, Any]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "entries"

    export_keys = _ordered_export_keys(rows)
    flattened = [(r.get("entry_date", ""), _flatten_tabs(r.get("tabs") or {})) for r in rows]
    headers = ["entry_date"] + export_keys
    ws.append(headers)

    for entry_date, flat in flattened:
        ws.append([entry_date] + [flat.get(k, "") for k in export_keys])

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

def build_pdf(site_name: str, date: str, tabs: Dict[str, Dict[str, str]]) -> bytes:
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
    for tab_key in sorted((tabs or {}).keys()):
        c.setFont("Helvetica-Bold", 11)
        c.drawString(40, y, f"[{tab_key}]")
        y -= 14
        c.setFont("Helvetica", 10)
        fields = tabs.get(tab_key) or {}
        for k in sorted(fields.keys()):
            v = str(fields.get(k,""))
            line = f"- {k}: {v}"
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
