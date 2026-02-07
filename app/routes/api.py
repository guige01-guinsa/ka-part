from __future__ import annotations

import io
from typing import Any, Dict, List

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import StreamingResponse

from ..db import (
    delete_entry,
    ensure_site,
    list_entries,
    load_entry,
    load_entry_by_id,
    save_tab_values,
    schema_alignment_report,
    upsert_entry,
    upsert_tab_domain_data,
)
from ..schema_defs import SCHEMA_DEFS, normalize_tabs_payload
from ..utils import build_excel, build_pdf, safe_ymd, today_ymd

router = APIRouter()


@router.get("/schema")
def api_schema():
    return {"schema": SCHEMA_DEFS}


@router.get("/health")
def health():
    report = schema_alignment_report()
    return {"ok": True, "version": "2.6.0", "schema_alignment_ok": report.get("ok", False)}


@router.get("/schema_alignment")
def api_schema_alignment():
    return schema_alignment_report()


@router.post("/save")
def api_save(payload: Dict[str, Any] = Body(...)):
    """
    {
      "site_name": "단지명",
      "date": "YYYY-MM-DD",
      "tabs": { "<tab_key>": { "<field_key>": "value" } }
    }
    """
    site_name = (payload.get("site_name") or "").strip() or "미지정단지"
    entry_date = safe_ymd(payload.get("date") or "")

    raw_tabs = payload.get("tabs") or {}
    if not isinstance(raw_tabs, dict):
        raise HTTPException(status_code=400, detail="tabs must be object")

    tabs = normalize_tabs_payload(raw_tabs)
    ignored_tabs = sorted(set(str(k) for k in raw_tabs.keys()) - set(tabs.keys()))

    site_id = ensure_site(site_name)
    entry_id = upsert_entry(site_id, entry_date)

    for tab_key, fields in tabs.items():
        save_tab_values(entry_id, tab_key, fields)
        upsert_tab_domain_data(site_name, entry_date, tab_key, fields)

    return {
        "ok": True,
        "site_name": site_name,
        "date": entry_date,
        "saved_tabs": sorted(tabs.keys()),
        "ignored_tabs": ignored_tabs,
    }


@router.get("/load")
def api_load(site_name: str = Query(...), date: str = Query(...)):
    site_name = (site_name or "").strip() or "미지정단지"
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    tabs = load_entry(site_id, entry_date)
    return {"ok": True, "site_name": site_name, "date": entry_date, "tabs": tabs}


@router.delete("/delete")
def api_delete(site_name: str = Query(...), date: str = Query(...)):
    site_name = (site_name or "").strip() or "미지정단지"
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    ok = delete_entry(site_id, entry_date)
    return {"ok": ok}


@router.get("/list_range")
def api_list_range(
    site_name: str = Query(...),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
):
    site_name = (site_name or "").strip() or "미지정단지"
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df
    site_id = ensure_site(site_name)
    entries = list_entries(site_id, df, dt)
    dates = [e["entry_date"] for e in entries]
    return {"ok": True, "site_name": site_name, "date_from": df, "date_to": dt, "dates": dates}


@router.get("/export")
def api_export(
    site_name: str = Query(...),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
):
    site_name = (site_name or "").strip() or "미지정단지"
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df

    site_id = ensure_site(site_name)
    entries = list_entries(site_id, df, dt)
    rows: List[Dict[str, Any]] = []
    for e in entries:
        rows.append({"entry_date": e["entry_date"], "tabs": load_entry_by_id(int(e["id"]))})

    xbytes = build_excel(site_name, df, dt, rows)
    filename = f"점검일지_{site_name}_{df}~{dt}.xlsx"

    from urllib.parse import quote

    ascii_fallback = "export.xlsx"
    cd = f"attachment; filename={ascii_fallback}; filename*=UTF-8''{quote(filename)}"
    return StreamingResponse(
        io.BytesIO(xbytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": cd},
    )


@router.get("/pdf")
def api_pdf(site_name: str = Query(...), date: str = Query(...)):
    site_name = (site_name or "").strip() or "미지정단지"
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    tabs = load_entry(site_id, entry_date)

    pbytes = build_pdf(site_name, entry_date, tabs)
    filename = f"점검일지_{site_name}_{entry_date}.pdf"

    from urllib.parse import quote

    ascii_fallback = "report.pdf"
    cd = f"attachment; filename={ascii_fallback}; filename*=UTF-8''{quote(filename)}"
    return StreamingResponse(
        io.BytesIO(pbytes),
        media_type="application/pdf",
        headers={"Content-Disposition": cd},
    )
