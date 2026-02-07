# -*- coding: utf-8 -*-
"""Single source of truth for form schema and DB storage mapping."""

from __future__ import annotations

from typing import Any, Dict, List

WORK_TYPES = ["일일", "주간", "월간", "정기", "기타일상"]
TASK_STATUS = ["완료", "진행중"]
SUBTASK_STATUS = ["정상", "점검필요", "장애", "조치중"]
SUBTASK_CRITICALITY = ["낮음", "중간", "높음", "긴급"]

SCHEMA_TAB_ORDER = [
    "home",
    "tr450",
    "tr400",
    "meter",
    "facility",
    "facility_check",
    "facility_fire",
    "facility_mechanical",
    "facility_telecom",
]

SCHEMA_DEFS = {
    "home": {
        "title": "홈",
        "fields": [
            {"k": "complex_name", "label": "단지명(표시)", "type": "text", "placeholder": "예: OO아파트"},
            {"k": "work_type", "label": "업무구분", "type": "select", "options": WORK_TYPES},
            {"k": "important_work", "label": "중요작업(요약)", "type": "textarea", "placeholder": "핵심 작업 요약"},
            {"k": "note", "label": "비고", "type": "textarea", "placeholder": "특이사항"},
        ],
    },
    "tr450": {
        "title": "변압기450",
        "fields": [
            {"k": "lv1_L1_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
            {"k": "lv1_L1_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv1_L1_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv1_L2_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
            {"k": "lv1_L2_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv1_L2_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv1_L3_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
            {"k": "lv1_L3_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv1_L3_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv1_temp", "label": "온도(℃)", "type": "number", "step": "0.01", "warn_min": -10, "warn_max": 120},
        ],
        "rows": [
            ["lv1_L1_V", "lv1_L1_A", "lv1_L1_KW"],
            ["lv1_L2_V", "lv1_L2_A", "lv1_L2_KW"],
            ["lv1_L3_V", "lv1_L3_A", "lv1_L3_KW"],
            ["lv1_temp"],
        ],
    },
    "tr400": {
        "title": "변압기400",
        "fields": [
            {"k": "lv2_L1_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
            {"k": "lv2_L1_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv2_L1_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv2_L2_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
            {"k": "lv2_L2_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv2_L2_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv2_L3_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
            {"k": "lv2_L3_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv2_L3_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
            {"k": "lv2_temp", "label": "온도(℃)", "type": "number", "step": "0.01", "warn_min": -10, "warn_max": 120},
        ],
        "rows": [
            ["lv2_L1_V", "lv2_L1_A", "lv2_L1_KW"],
            ["lv2_L2_V", "lv2_L2_A", "lv2_L2_KW"],
            ["lv2_L3_V", "lv2_L3_A", "lv2_L3_KW"],
            ["lv2_temp"],
        ],
    },
    "meter": {
        "title": "전력량계",
        "fields": [
            {"k": "AISS_L1_A", "label": "L1(A)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 800},
            {"k": "AISS_L2_A", "label": "L2(A)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 800},
            {"k": "AISS_L3_A", "label": "L3(A)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 800},
            {"k": "main_kwh", "label": "메인(kWh)", "type": "number", "step": "0.01"},
            {"k": "industry_kwh", "label": "산업용(kWh)", "type": "number", "step": "0.01"},
            {"k": "street_kwh", "label": "가로등(kWh)", "type": "number", "step": "0.01"},
        ],
        "rows": [
            ["AISS_L1_A", "AISS_L2_A", "AISS_L3_A"],
            ["main_kwh", "industry_kwh", "street_kwh"],
        ],
    },
    "facility": {
        "title": "시설관리",
        "fields": [
            {"k": "title", "label": "작업/민원 제목", "type": "text", "placeholder": "예: 펌프실 누수 조치"},
            {"k": "status", "label": "상태", "type": "select", "options": TASK_STATUS},
            {"k": "content", "label": "작업내용", "type": "textarea", "placeholder": "작업 내용 상세"},
            {"k": "note", "label": "비고", "type": "textarea", "placeholder": "특이사항"},
        ],
    },
    "facility_check": {
        "title": "시설검침",
        "fields": [
            {"k": "tank_level_1", "label": "저수조1(%)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 100},
            {"k": "tank_level_2", "label": "저수조2(%)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 100},
            {"k": "hydrant_pressure", "label": "소화전(bar)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 12},
            {"k": "sp_pump_pressure", "label": "SP펌프(bar)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 12},
            {"k": "high_pressure", "label": "고층(bar)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 12},
            {"k": "low_pressure", "label": "저층(bar)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 12},
            {"k": "office_pressure", "label": "오피스(bar)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 12},
            {"k": "shop_pressure", "label": "상가(bar)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 12},
        ],
        "rows": [
            ["tank_level_1", "tank_level_2"],
            ["hydrant_pressure", "sp_pump_pressure"],
            ["high_pressure", "low_pressure"],
            ["office_pressure", "shop_pressure"],
        ],
    },
    "facility_fire": {
        "title": "소방시설관리",
        "fields": [
            {"k": "task_title", "label": "점검대상", "type": "text", "placeholder": "예: 소화펌프, 수신반"},
            {"k": "status", "label": "상태", "type": "select", "options": SUBTASK_STATUS},
            {"k": "criticality", "label": "중요도", "type": "select", "options": SUBTASK_CRITICALITY},
            {"k": "detail", "label": "점검내용", "type": "textarea", "placeholder": "이상 여부와 조치 사항"},
            {"k": "next_due", "label": "다음 점검일", "type": "date"},
        ],
        "rows": [
            ["task_title", "status"],
            ["criticality", "next_due"],
            ["detail"],
        ],
    },
    "facility_mechanical": {
        "title": "기계설비시설관리",
        "fields": [
            {"k": "task_title", "label": "점검대상", "type": "text", "placeholder": "예: 급수펌프, 열교환기"},
            {"k": "status", "label": "상태", "type": "select", "options": SUBTASK_STATUS},
            {"k": "criticality", "label": "중요도", "type": "select", "options": SUBTASK_CRITICALITY},
            {"k": "detail", "label": "점검내용", "type": "textarea", "placeholder": "진동/소음/누수 등"},
            {"k": "next_due", "label": "다음 점검일", "type": "date"},
        ],
        "rows": [
            ["task_title", "status"],
            ["criticality", "next_due"],
            ["detail"],
        ],
    },
    "facility_telecom": {
        "title": "통신시설관리",
        "fields": [
            {"k": "task_title", "label": "점검대상", "type": "text", "placeholder": "예: 통신실, 네트워크 장비"},
            {"k": "status", "label": "상태", "type": "select", "options": SUBTASK_STATUS},
            {"k": "criticality", "label": "중요도", "type": "select", "options": SUBTASK_CRITICALITY},
            {"k": "detail", "label": "점검내용", "type": "textarea", "placeholder": "장애/복구/예방조치"},
            {"k": "next_due", "label": "다음 점검일", "type": "date"},
        ],
        "rows": [
            ["task_title", "status"],
            ["criticality", "next_due"],
            ["detail"],
        ],
    },
}

# Legacy form key aliases -> canonical form keys.
LEGACY_FIELD_ALIASES: Dict[str, Dict[str, str]] = {
    "meter": {
        "kwh_main": "main_kwh",
        "kwh_industry": "industry_kwh",
        "kwh_street": "street_kwh",
    },
    "facility_check": {
        "hydrant_bar": "hydrant_pressure",
        "sp_pump_bar": "sp_pump_pressure",
        "water_high_bar": "high_pressure",
        "water_low_bar": "low_pressure",
        "water_office_bar": "office_pressure",
        "water_shop_bar": "shop_pressure",
    },
}

TAB_STORAGE_SPECS: Dict[str, Dict[str, Any]] = {
    "tr450": {
        "table": "transformer_450_reads",
        "key_cols": ["site_name", "entry_date"],
        "column_map": {
            "lv1_L1_V": "lv1_l1_v",
            "lv1_L1_A": "lv1_l1_a",
            "lv1_L1_KW": "lv1_l1_kw",
            "lv1_L2_V": "lv1_l2_v",
            "lv1_L2_A": "lv1_l2_a",
            "lv1_L2_KW": "lv1_l2_kw",
            "lv1_L3_V": "lv1_l3_v",
            "lv1_L3_A": "lv1_l3_a",
            "lv1_L3_KW": "lv1_l3_kw",
            "lv1_temp": "lv1_temp",
        },
    },
    "tr400": {
        "table": "transformer_400_reads",
        "key_cols": ["site_name", "entry_date"],
        "column_map": {
            "lv2_L1_V": "lv2_l1_v",
            "lv2_L1_A": "lv2_l1_a",
            "lv2_L1_KW": "lv2_l1_kw",
            "lv2_L2_V": "lv2_l2_v",
            "lv2_L2_A": "lv2_l2_a",
            "lv2_L2_KW": "lv2_l2_kw",
            "lv2_L3_V": "lv2_l3_v",
            "lv2_L3_A": "lv2_l3_a",
            "lv2_L3_KW": "lv2_l3_kw",
            "lv2_temp": "lv2_temp",
        },
    },
    "meter": {
        "table": "power_meter_reads",
        "key_cols": ["site_name", "entry_date"],
        "column_map": {
            "AISS_L1_A": "aiss_l1_a",
            "AISS_L2_A": "aiss_l2_a",
            "AISS_L3_A": "aiss_l3_a",
            "main_kwh": "main_kwh",
            "industry_kwh": "industry_kwh",
            "street_kwh": "street_kwh",
        },
    },
    "facility_check": {
        "table": "facility_checks",
        "key_cols": ["site_name", "entry_date"],
        "column_map": {
            "tank_level_1": "tank_level_1",
            "tank_level_2": "tank_level_2",
            "hydrant_pressure": "hydrant_pressure",
            "sp_pump_pressure": "sp_pump_pressure",
            "high_pressure": "high_pressure",
            "low_pressure": "low_pressure",
            "office_pressure": "office_pressure",
            "shop_pressure": "shop_pressure",
        },
    },
    "facility_fire": {
        "table": "facility_subtasks",
        "key_cols": ["site_name", "entry_date", "domain_key"],
        "fixed": {"domain_key": "fire"},
        "column_map": {
            "task_title": "task_title",
            "status": "status",
            "criticality": "criticality",
            "detail": "detail",
            "next_due": "next_due",
        },
    },
    "facility_mechanical": {
        "table": "facility_subtasks",
        "key_cols": ["site_name", "entry_date", "domain_key"],
        "fixed": {"domain_key": "mechanical"},
        "column_map": {
            "task_title": "task_title",
            "status": "status",
            "criticality": "criticality",
            "detail": "detail",
            "next_due": "next_due",
        },
    },
    "facility_telecom": {
        "table": "facility_subtasks",
        "key_cols": ["site_name", "entry_date", "domain_key"],
        "fixed": {"domain_key": "telecom"},
        "column_map": {
            "task_title": "task_title",
            "status": "status",
            "criticality": "criticality",
            "detail": "detail",
            "next_due": "next_due",
        },
    },
}


def schema_fields(tab_key: str) -> List[Dict[str, Any]]:
    return list((SCHEMA_DEFS.get(tab_key) or {}).get("fields") or [])


def schema_field_keys(tab_key: str) -> List[str]:
    return [str(f.get("k")) for f in schema_fields(tab_key) if f.get("k")]


def canonicalize_tab_fields(tab_key: str, values: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(values, dict):
        return {}
    aliases = LEGACY_FIELD_ALIASES.get(tab_key, {})
    allowed = set(schema_field_keys(tab_key))
    out: Dict[str, Any] = {}
    for raw_key, raw_val in values.items():
        key = aliases.get(str(raw_key), str(raw_key))
        if key in allowed:
            out[key] = raw_val
    return out


def normalize_tabs_payload(tabs: Dict[str, Any] | None) -> Dict[str, Dict[str, Any]]:
    if not isinstance(tabs, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for tab_key, tab_values in tabs.items():
        key = str(tab_key)
        if key not in SCHEMA_DEFS:
            continue
        out[key] = canonicalize_tab_fields(key, tab_values if isinstance(tab_values, dict) else {})
    return out
