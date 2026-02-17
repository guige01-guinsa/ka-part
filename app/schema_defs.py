# -*- coding: utf-8 -*-
"""Single source of truth for form schema and DB storage mapping."""

from __future__ import annotations

import copy
from typing import Any, Dict, List

WORK_TYPES = ["일일", "주간", "월간", "일상"]
TASK_STATUS = ["완료", "진행중"]
SUBTASK_STATUS = ["정상", "점검필요", "장애", "조치중"]
SUBTASK_CRITICALITY = ["낮음", "중간", "높음", "긴급"]

SCHEMA_TAB_ORDER = [
    "home",
    "tr1",
    "tr2",
    "tr3",
    "tr4",
    "tr5",
    "tr6",
    "main_vcb",
    "dc_panel",
    "temperature",
    "meter",
    "facility",
    "facility_check",
    "facility_fire",
    "facility_mechanical",
    "facility_telecom",
]

DEFAULT_PRIMARY_TAB_KEYS = ["tr1", "tr2", "tr3", "tr4", "tr5", "tr6", "main_vcb", "dc_panel", "temperature", "meter", "facility_check"]
DEFAULT_INITIAL_VISIBLE_TAB_KEYS = ["home"]
DEFAULT_HIDDEN_TAB_KEYS = [tab for tab in SCHEMA_TAB_ORDER if tab not in DEFAULT_INITIAL_VISIBLE_TAB_KEYS]

FIELD_TYPE_SET = {"text", "number", "textarea", "select", "date"}

LEGACY_TAB_ALIASES: Dict[str, str] = {
    "tr450": "tr1",
    "tr400": "tr2",
    "lv3": "tr3",
    "lv4": "tr4",
    "lv5": "tr5",
    "lv6": "tr6",
}


def canonical_tab_key(value: Any) -> str:
    raw = str(value or "").strip()
    return LEGACY_TAB_ALIASES.get(raw, raw)


def _lv_fields(prefix: str) -> List[Dict[str, Any]]:
    p = str(prefix or "").strip()
    return [
        {"k": f"{p}_L1_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
        {"k": f"{p}_L1_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
        {"k": f"{p}_L1_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
        {"k": f"{p}_L2_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
        {"k": f"{p}_L2_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
        {"k": f"{p}_L2_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
        {"k": f"{p}_L3_V", "label": "V", "type": "number", "step": "0.01", "warn_min": 180, "warn_max": 260},
        {"k": f"{p}_L3_A", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
        {"k": f"{p}_L3_KW", "label": "kW", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
        {"k": f"{p}_temp", "label": "온도(℃)", "type": "number", "step": "0.01", "warn_min": -10, "warn_max": 120},
    ]


def _lv_rows(prefix: str) -> List[List[str]]:
    p = str(prefix or "").strip()
    return [
        [f"{p}_L1_V", f"{p}_L1_A", f"{p}_L1_KW"],
        [f"{p}_L2_V", f"{p}_L2_A", f"{p}_L2_KW"],
        [f"{p}_L3_V", f"{p}_L3_A", f"{p}_L3_KW"],
        [f"{p}_temp"],
    ]

SCHEMA_DEFS = {
    "home": {
        "title": "홈",
        "fields": [
            {"k": "complex_code", "label": "단지코드(표시)", "type": "text", "placeholder": "예: APT00012", "readonly": True},
            {"k": "complex_name", "label": "단지명(표시)", "type": "text", "placeholder": "예: OO아파트", "readonly": True},
            {"k": "work_type", "label": "업무구분", "type": "select", "options": WORK_TYPES},
            {"k": "important_work", "label": "중요작업(요약)", "type": "textarea", "placeholder": "핵심 작업 요약"},
            {"k": "note", "label": "비고", "type": "textarea", "placeholder": "특이사항"},
        ],
    },
    "tr1": {
        "title": "LV1",
        "fields": _lv_fields("lv1"),
        "rows": _lv_rows("lv1"),
    },
    "tr2": {
        "title": "LV2",
        "fields": _lv_fields("lv2"),
        "rows": _lv_rows("lv2"),
    },
    "tr3": {
        "title": "LV3",
        "fields": _lv_fields("lv3"),
        "rows": _lv_rows("lv3"),
    },
    "tr4": {
        "title": "LV4",
        "fields": _lv_fields("lv4"),
        "rows": _lv_rows("lv4"),
    },
    "tr5": {
        "title": "LV5",
        "fields": _lv_fields("lv5"),
        "rows": _lv_rows("lv5"),
    },
    "tr6": {
        "title": "LV6",
        "fields": _lv_fields("lv6"),
        "rows": _lv_rows("lv6"),
    },
    "main_vcb": {
        "title": "특고(Main)",
        "fields": [
            {"k": "main_vcb_kv", "label": "KV", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 300},
            {"k": "main_vcb_l1_a", "label": "L1-A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 4000},
            {"k": "main_vcb_l2_a", "label": "L2-A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 4000},
            {"k": "main_vcb_l3_a", "label": "L3-A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 4000},
        ],
        "rows": [
            ["main_vcb_kv", "main_vcb_l1_a", "main_vcb_l2_a", "main_vcb_l3_a"],
        ],
    },
    "dc_panel": {
        "title": "정류반(DC)",
        "fields": [
            {"k": "dc_panel_v", "label": "V(DC)", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 1000},
            {"k": "dc_panel_a", "label": "A", "type": "number", "step": "0.01", "warn_min": 0, "warn_max": 2000},
        ],
        "rows": [
            ["dc_panel_v", "dc_panel_a"],
        ],
    },
    "temperature": {
        "title": "온도",
        "fields": [
            {"k": "temperature_tr1", "label": "TR1", "type": "number", "step": "0.01", "warn_min": -30, "warn_max": 120},
            {"k": "temperature_tr2", "label": "TR2", "type": "number", "step": "0.01", "warn_min": -30, "warn_max": 120},
            {"k": "temperature_tr3", "label": "TR3", "type": "number", "step": "0.01", "warn_min": -30, "warn_max": 120},
            {"k": "temperature_tr4", "label": "TR4", "type": "number", "step": "0.01", "warn_min": -30, "warn_max": 120},
            {"k": "temperature_indoor", "label": "실내", "type": "number", "step": "0.01", "warn_min": -30, "warn_max": 120},
        ],
        "rows": [
            ["temperature_tr1", "temperature_tr2", "temperature_tr3", "temperature_tr4", "temperature_indoor"],
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

DEFAULT_SITE_ENV_CONFIG: Dict[str, Any] = {
    "hide_tabs": list(DEFAULT_HIDDEN_TAB_KEYS),
}

SITE_ENV_TEMPLATE: Dict[str, Any] = {
    "hide_tabs": [],
    "tabs": {
        "tr1": {
            "title": "LV1",
            "hide_fields": [],
            "field_labels": {"lv1_temp": "LV1 온도(℃)"},
            "field_overrides": {"lv1_temp": {"warn_max": 110}},
            "add_fields": [
                {
                    "k": "lv1_oil_level",
                    "label": "유면(%)",
                    "type": "number",
                    "step": "0.01",
                    "warn_min": 0,
                    "warn_max": 100,
                }
            ],
            "rows": [
                ["lv1_L1_V", "lv1_L1_A", "lv1_L1_KW"],
                ["lv1_L2_V", "lv1_L2_A", "lv1_L2_KW"],
                ["lv1_L3_V", "lv1_L3_A", "lv1_L3_KW"],
                ["lv1_temp", "lv1_oil_level"],
            ],
        }
    },
}

SITE_ENV_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "blank": {
        "name": "빈 템플릿",
        "description": "아무 변경 없이 시작합니다.",
        "config": {},
    },
    "electrical_min": {
        "name": "전기 중심",
        "description": "시설 하위 탭 일부를 숨기고 전기 탭 위주로 구성합니다.",
        "config": {
            "hide_tabs": ["facility_telecom"],
            "tabs": {
                "tr1": {"title": "LV1", "hide_fields": []},
                "tr2": {"title": "LV2", "hide_fields": []},
                "meter": {"title": "전력량계"},
            },
        },
    },
    "safety_focus": {
        "name": "안전 점검 강화",
        "description": "소방/시설검침 항목을 우선으로 확장하는 기본안입니다.",
        "config": {
            "tabs": {
                "facility_check": {
                    "add_fields": [
                        {"k": "generator_fuel", "label": "발전기 연료(%)", "type": "number", "step": "0.1", "warn_min": 0, "warn_max": 100}
                    ],
                    "rows": [
                        ["tank_level_1", "tank_level_2"],
                        ["hydrant_pressure", "sp_pump_pressure"],
                        ["high_pressure", "low_pressure"],
                        ["office_pressure", "shop_pressure"],
                        ["generator_fuel"],
                    ],
                },
                "facility_fire": {
                    "title": "소방시설관리(강화)",
                },
            }
        },
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
    "tr1": {
        "table": "transformer_450_reads",
        "key_cols": ["site_name", "entry_date", "work_type"],
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
    "tr2": {
        "table": "transformer_400_reads",
        "key_cols": ["site_name", "entry_date", "work_type"],
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
    "main_vcb": {
        "table": "main_vcb_reads",
        "key_cols": ["site_name", "entry_date", "work_type"],
        "column_map": {
            "main_vcb_kv": "main_vcb_kv",
            "main_vcb_l1_a": "main_vcb_l1_a",
            "main_vcb_l2_a": "main_vcb_l2_a",
            "main_vcb_l3_a": "main_vcb_l3_a",
        },
    },
    "dc_panel": {
        "table": "dc_panel_reads",
        "key_cols": ["site_name", "entry_date", "work_type"],
        "column_map": {
            "dc_panel_v": "dc_panel_v",
            "dc_panel_a": "dc_panel_a",
        },
    },
    "temperature": {
        "table": "temperature_reads",
        "key_cols": ["site_name", "entry_date", "work_type"],
        "column_map": {
            "temperature_tr1": "temperature_tr1",
            "temperature_tr2": "temperature_tr2",
            "temperature_tr3": "temperature_tr3",
            "temperature_tr4": "temperature_tr4",
            "temperature_indoor": "temperature_indoor",
        },
    },
    "meter": {
        "table": "power_meter_reads",
        "key_cols": ["site_name", "entry_date", "work_type"],
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
        "key_cols": ["site_name", "entry_date", "work_type"],
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
        "key_cols": ["site_name", "entry_date", "domain_key", "work_type"],
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
        "key_cols": ["site_name", "entry_date", "domain_key", "work_type"],
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
        "key_cols": ["site_name", "entry_date", "domain_key", "work_type"],
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


def _schema_source(schema_defs: Dict[str, Dict[str, Any]] | None = None) -> Dict[str, Dict[str, Any]]:
    return schema_defs if isinstance(schema_defs, dict) else SCHEMA_DEFS


def schema_fields(tab_key: str, schema_defs: Dict[str, Dict[str, Any]] | None = None) -> List[Dict[str, Any]]:
    key = canonical_tab_key(tab_key)
    return list((_schema_source(schema_defs).get(key) or {}).get("fields") or [])


def schema_field_keys(tab_key: str, schema_defs: Dict[str, Dict[str, Any]] | None = None) -> List[str]:
    return [str(f.get("k")) for f in schema_fields(tab_key, schema_defs=schema_defs) if f.get("k")]


def canonicalize_tab_fields(
    tab_key: str,
    values: Dict[str, Any] | None,
    schema_defs: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    if not isinstance(values, dict):
        return {}
    canonical_key = canonical_tab_key(tab_key)
    aliases = LEGACY_FIELD_ALIASES.get(canonical_key, {})
    allowed = set(schema_field_keys(canonical_key, schema_defs=schema_defs))
    out: Dict[str, Any] = {}
    for raw_key, raw_val in values.items():
        field_key = aliases.get(str(raw_key), str(raw_key))
        if field_key in allowed:
            out[field_key] = raw_val
    return out


def normalize_tabs_payload(
    tabs: Dict[str, Any] | None,
    schema_defs: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Dict[str, Any]]:
    if not isinstance(tabs, dict):
        return {}
    source = _schema_source(schema_defs)
    out: Dict[str, Dict[str, Any]] = {}
    for tab_key, tab_values in tabs.items():
        key = canonical_tab_key(tab_key)
        if key not in source:
            continue
        out[key] = canonicalize_tab_fields(
            key,
            tab_values if isinstance(tab_values, dict) else {},
            schema_defs=source,
        )
    return out


def _clean_field_type(value: Any) -> str:
    t = str(value or "text").strip().lower()
    return t if t in FIELD_TYPE_SET else "text"


def _clean_field_def(obj: Any) -> Dict[str, Any] | None:
    if not isinstance(obj, dict):
        return None
    key = str(obj.get("k") or "").strip()
    label = str(obj.get("label") or "").strip()
    if not key or not label:
        return None
    ftype = _clean_field_type(obj.get("type"))
    out: Dict[str, Any] = {"k": key, "label": label, "type": ftype}
    for k in ("placeholder", "step", "min", "max", "warn_min", "warn_max"):
        if k in obj and obj[k] is not None and str(obj[k]).strip() != "":
            out[k] = obj[k]
    if ftype == "select":
        raw_opts = obj.get("options") or []
        if isinstance(raw_opts, list):
            opts = [str(x).strip() for x in raw_opts if str(x).strip()]
            if opts:
                out["options"] = opts
    return out


def normalize_site_env_config(config: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    out: Dict[str, Any] = {}

    raw_hide_tabs = config.get("hide_tabs")
    if isinstance(raw_hide_tabs, list):
        hide_tabs = []
        seen = set()
        for x in raw_hide_tabs:
            k = canonical_tab_key(x)
            if not k or k in seen:
                continue
            hide_tabs.append(k)
            seen.add(k)
        if hide_tabs:
            out["hide_tabs"] = hide_tabs

    raw_tabs = config.get("tabs")
    if isinstance(raw_tabs, dict):
        clean_tabs: Dict[str, Dict[str, Any]] = {}
        for raw_tab_key, raw_tab_cfg in raw_tabs.items():
            tab_key = canonical_tab_key(raw_tab_key)
            if not tab_key or not isinstance(raw_tab_cfg, dict):
                continue
            tab_cfg: Dict[str, Any] = {}

            if "title" in raw_tab_cfg:
                title = str(raw_tab_cfg.get("title") or "").strip()
                if title:
                    tab_cfg["title"] = title

            if isinstance(raw_tab_cfg.get("hide_fields"), list):
                vals = []
                seen = set()
                for f in raw_tab_cfg["hide_fields"]:
                    fk = str(f or "").strip()
                    if not fk or fk in seen:
                        continue
                    vals.append(fk)
                    seen.add(fk)
                if vals:
                    tab_cfg["hide_fields"] = vals

            if isinstance(raw_tab_cfg.get("field_labels"), dict):
                labels: Dict[str, str] = {}
                for fk, lv in raw_tab_cfg["field_labels"].items():
                    key = str(fk or "").strip()
                    val = str(lv or "").strip()
                    if key and val:
                        labels[key] = val
                if labels:
                    tab_cfg["field_labels"] = labels

            if isinstance(raw_tab_cfg.get("field_overrides"), dict):
                overs: Dict[str, Dict[str, Any]] = {}
                for fk, ov in raw_tab_cfg["field_overrides"].items():
                    key = str(fk or "").strip()
                    if not key or not isinstance(ov, dict):
                        continue
                    clean: Dict[str, Any] = {}
                    for kk in ("label", "placeholder", "step", "min", "max", "warn_min", "warn_max"):
                        if kk in ov and ov[kk] is not None and str(ov[kk]).strip() != "":
                            clean[kk] = ov[kk]
                    if "type" in ov:
                        clean["type"] = _clean_field_type(ov.get("type"))
                    if "options" in ov and isinstance(ov["options"], list):
                        opts = [str(x).strip() for x in ov["options"] if str(x).strip()]
                        if opts:
                            clean["options"] = opts
                    if clean:
                        overs[key] = clean
                if overs:
                    tab_cfg["field_overrides"] = overs

            if isinstance(raw_tab_cfg.get("add_fields"), list):
                add_fields = []
                for x in raw_tab_cfg["add_fields"]:
                    field = _clean_field_def(x)
                    if field:
                        add_fields.append(field)
                if add_fields:
                    tab_cfg["add_fields"] = add_fields

            if isinstance(raw_tab_cfg.get("rows"), list):
                rows: List[List[str]] = []
                for row in raw_tab_cfg["rows"]:
                    if not isinstance(row, list):
                        continue
                    rr = [str(x or "").strip() for x in row if str(x or "").strip()]
                    if rr:
                        rows.append(rr)
                if rows:
                    tab_cfg["rows"] = rows

            if tab_cfg:
                prior = clean_tabs.get(tab_key)
                if isinstance(prior, dict):
                    merged_tab = copy.deepcopy(prior)
                    merged_tab.update(tab_cfg)
                    if "hide_fields" in prior or "hide_fields" in tab_cfg:
                        h_prev = [str(x or "").strip() for x in (prior.get("hide_fields") or []) if str(x or "").strip()]
                        h_new = [str(x or "").strip() for x in (tab_cfg.get("hide_fields") or []) if str(x or "").strip()]
                        merged_tab["hide_fields"] = list(dict.fromkeys([*h_prev, *h_new]))
                    if "field_labels" in prior or "field_labels" in tab_cfg:
                        merged_tab["field_labels"] = {**(prior.get("field_labels") or {}), **(tab_cfg.get("field_labels") or {})}
                    if "field_overrides" in prior or "field_overrides" in tab_cfg:
                        overs = copy.deepcopy(prior.get("field_overrides") or {})
                        for fk, ov in (tab_cfg.get("field_overrides") or {}).items():
                            prev_ov = dict(overs.get(fk) or {})
                            if isinstance(ov, dict):
                                prev_ov.update(ov)
                            if prev_ov:
                                overs[fk] = prev_ov
                        if overs:
                            merged_tab["field_overrides"] = overs
                    if "add_fields" in prior or "add_fields" in tab_cfg:
                        by_key: Dict[str, Dict[str, Any]] = {}
                        for f in [*(prior.get("add_fields") or []), *(tab_cfg.get("add_fields") or [])]:
                            fk = str((f or {}).get("k") or "").strip()
                            if fk:
                                by_key[fk] = f
                        if by_key:
                            merged_tab["add_fields"] = list(by_key.values())
                    tab_cfg = merged_tab
                clean_tabs[tab_key] = tab_cfg

        if clean_tabs:
            out["tabs"] = clean_tabs

    return out


def default_site_env_config() -> Dict[str, Any]:
    return normalize_site_env_config(DEFAULT_SITE_ENV_CONFIG)


def merge_site_env_configs(
    base_config: Dict[str, Any] | None,
    override_config: Dict[str, Any] | None,
) -> Dict[str, Any]:
    base = normalize_site_env_config(base_config)
    override = normalize_site_env_config(override_config)
    if not base:
        return override
    if not override:
        return base

    out: Dict[str, Any] = copy.deepcopy(base)

    hide_tabs: List[str] = []
    seen_tabs = set()
    for tab_key in [*(out.get("hide_tabs") or []), *(override.get("hide_tabs") or [])]:
        k = str(tab_key or "").strip()
        if not k or k in seen_tabs:
            continue
        hide_tabs.append(k)
        seen_tabs.add(k)
    if hide_tabs:
        out["hide_tabs"] = hide_tabs
    elif "hide_tabs" in out:
        out.pop("hide_tabs", None)

    merged_tabs: Dict[str, Dict[str, Any]] = copy.deepcopy(out.get("tabs") or {})
    for tab_key, tab_cfg in (override.get("tabs") or {}).items():
        cur = copy.deepcopy(merged_tabs.get(tab_key) or {})
        if "title" in tab_cfg:
            cur["title"] = str(tab_cfg.get("title") or "").strip()

        if isinstance(tab_cfg.get("hide_fields"), list):
            vals: List[str] = []
            seen = set()
            for fk in [*(cur.get("hide_fields") or []), *(tab_cfg.get("hide_fields") or [])]:
                key = str(fk or "").strip()
                if not key or key in seen:
                    continue
                vals.append(key)
                seen.add(key)
            if vals:
                cur["hide_fields"] = vals
            elif "hide_fields" in cur:
                cur.pop("hide_fields", None)

        if isinstance(tab_cfg.get("field_labels"), dict):
            cur["field_labels"] = {**(cur.get("field_labels") or {}), **tab_cfg["field_labels"]}

        if isinstance(tab_cfg.get("field_overrides"), dict):
            merged_overs = copy.deepcopy(cur.get("field_overrides") or {})
            for fk, ov in tab_cfg["field_overrides"].items():
                if not isinstance(ov, dict):
                    continue
                prior = dict(merged_overs.get(fk) or {})
                prior.update(ov)
                if prior:
                    merged_overs[fk] = prior
            if merged_overs:
                cur["field_overrides"] = merged_overs

        if isinstance(tab_cfg.get("add_fields"), list):
            by_key: Dict[str, Dict[str, Any]] = {}
            for f in cur.get("add_fields") or []:
                k = str((f or {}).get("k") or "").strip()
                if k:
                    by_key[k] = f
            for f in tab_cfg["add_fields"]:
                k = str((f or {}).get("k") or "").strip()
                if k:
                    by_key[k] = f
            if by_key:
                cur["add_fields"] = list(by_key.values())

        if isinstance(tab_cfg.get("rows"), list):
            cur["rows"] = copy.deepcopy(tab_cfg["rows"])

        if cur:
            merged_tabs[tab_key] = cur

    if merged_tabs:
        out["tabs"] = merged_tabs

    return normalize_site_env_config(out)


def build_effective_schema(
    *,
    base_schema: Dict[str, Dict[str, Any]] | None = None,
    site_env_config: Dict[str, Any] | None = None,
) -> Dict[str, Dict[str, Any]]:
    schema = copy.deepcopy(_schema_source(base_schema))
    config = normalize_site_env_config(site_env_config)
    if not config:
        return schema

    tabs_cfg = config.get("tabs") or {}
    for tab_key in config.get("hide_tabs") or []:
        # Explicit tab config means caller wants the tab visible/customized.
        if tab_key in tabs_cfg:
            continue
        schema.pop(tab_key, None)

    for tab_key, tab_cfg in tabs_cfg.items():
        tab = schema.get(tab_key)
        if not tab:
            tab = {"title": tab_key, "fields": []}
            schema[tab_key] = tab

        if "title" in tab_cfg:
            tab["title"] = str(tab_cfg["title"])

        fields = [dict(x) for x in (tab.get("fields") or []) if isinstance(x, dict) and x.get("k")]
        hide_set = set(tab_cfg.get("hide_fields") or [])
        if hide_set:
            fields = [f for f in fields if str(f.get("k")) not in hide_set]

        field_by_key: Dict[str, Dict[str, Any]] = {str(f["k"]): f for f in fields}

        for fk, label in (tab_cfg.get("field_labels") or {}).items():
            if fk in field_by_key:
                field_by_key[fk]["label"] = str(label)

        for fk, ov in (tab_cfg.get("field_overrides") or {}).items():
            if fk not in field_by_key:
                continue
            for kk, vv in ov.items():
                field_by_key[fk][kk] = vv

        for add_field in tab_cfg.get("add_fields") or []:
            k = str(add_field.get("k") or "").strip()
            if not k:
                continue
            if k in field_by_key:
                for kk, vv in add_field.items():
                    if kk == "k":
                        continue
                    field_by_key[k][kk] = vv
            else:
                newf = dict(add_field)
                fields.append(newf)
                field_by_key[k] = newf

        existing_keys = [str(f.get("k")) for f in fields if f.get("k")]
        key_set = set(existing_keys)
        rows = tab_cfg.get("rows")
        if isinstance(rows, list):
            normalized_rows: List[List[str]] = []
            used = set()
            for row in rows:
                rr: List[str] = []
                for k in row:
                    if k in key_set and k not in used:
                        rr.append(k)
                        used.add(k)
                if rr:
                    normalized_rows.append(rr)
            for k in existing_keys:
                if k not in used:
                    normalized_rows.append([k])
            tab["rows"] = normalized_rows
        elif "rows" in tab:
            normalized_rows = []
            used = set()
            for row in (tab.get("rows") or []):
                rr = []
                for k in row:
                    kk = str(k or "")
                    if kk in key_set and kk not in used:
                        rr.append(kk)
                        used.add(kk)
                if rr:
                    normalized_rows.append(rr)
            for k in existing_keys:
                if k not in used:
                    normalized_rows.append([k])
            tab["rows"] = normalized_rows

        tab["fields"] = fields

    to_remove = [k for k, v in schema.items() if not (v.get("fields") or [])]
    for k in to_remove:
        schema.pop(k, None)
    if not schema:
        source = _schema_source(base_schema)
        fallback: Dict[str, Dict[str, Any]] = {}
        for tab_key in DEFAULT_PRIMARY_TAB_KEYS:
            tab = source.get(tab_key)
            if not isinstance(tab, dict):
                continue
            fields = [dict(x) for x in (tab.get("fields") or []) if isinstance(x, dict) and x.get("k")]
            if not fields:
                continue
            item = copy.deepcopy(tab)
            item["fields"] = fields
            fallback[tab_key] = item
        if fallback:
            schema = fallback
    return schema


def site_env_template() -> Dict[str, Any]:
    return copy.deepcopy(SITE_ENV_TEMPLATE)


def site_env_templates() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for key, row in SITE_ENV_TEMPLATES.items():
        item = {
            "name": str((row or {}).get("name") or key),
            "description": str((row or {}).get("description") or ""),
            "config": normalize_site_env_config((row or {}).get("config") or {}),
        }
        out[str(key)] = item
    if "default" not in out:
        out["default"] = {
            "name": "기본 추천",
            "description": "필드 추가/레이아웃 예시가 포함된 기본 템플릿",
            "config": site_env_template(),
        }
    return out
