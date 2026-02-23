from __future__ import annotations

from app.schema_defs import merge_site_env_configs, normalize_site_env_config, site_env_templates
from app.utils import _resolve_pdf_render_plan


def test_normalize_site_env_config_report_fields() -> None:
    cfg = normalize_site_env_config(
        {
            "report": {
                "pdf_profile_id": " substation_daily_generic_a4 ",
                "pdf_template_name": "../substation_daily_generic_a4.html",
            }
        }
    )
    assert cfg["report"]["pdf_profile_id"] == "substation_daily_generic_a4"
    assert cfg["report"]["pdf_template_name"] == "substation_daily_generic_a4.html"


def test_normalize_site_env_config_invalid_report_template_removed() -> None:
    cfg = normalize_site_env_config(
        {
            "report": {
                "pdf_profile_id": "substation_daily_generic_a4",
                "pdf_template_name": "template.txt",
            }
        }
    )
    assert cfg["report"]["pdf_profile_id"] == "substation_daily_generic_a4"
    assert "pdf_template_name" not in cfg["report"]


def test_merge_site_env_configs_report_override() -> None:
    merged = merge_site_env_configs(
        {"report": {"pdf_profile_id": "substation_daily_a4"}},
        {"report": {"pdf_profile_id": "substation_daily_generic_a4"}},
    )
    assert merged["report"]["pdf_profile_id"] == "substation_daily_generic_a4"


def test_site_env_templates_contains_pdf_profiles() -> None:
    templates = site_env_templates()
    assert "report_substation_a4" in templates
    assert "report_generic_a4" in templates
    assert templates["report_substation_a4"]["config"]["report"]["pdf_profile_id"] == "substation_daily_a4"
    assert templates["report_generic_a4"]["config"]["report"]["pdf_profile_id"] == "substation_daily_generic_a4"


def test_resolve_pdf_render_plan_default_profile() -> None:
    plan = _resolve_pdf_render_plan({})
    assert plan["profile_id"] == "substation_daily_a4"
    assert plan["template_name"] == "substation_daily_a4.html"
    assert plan["context_builder"] == "substation"


def test_resolve_pdf_render_plan_generic_profile() -> None:
    plan = _resolve_pdf_render_plan({"report": {"pdf_profile_id": "substation_daily_generic_a4"}})
    assert plan["profile_id"] == "substation_daily_generic_a4"
    assert plan["template_name"] == "substation_daily_generic_a4.html"
    assert plan["context_builder"] == "generic"


def test_resolve_pdf_render_plan_unknown_profile_falls_back_to_default() -> None:
    plan = _resolve_pdf_render_plan({"report": {"pdf_profile_id": "unknown-profile"}})
    assert plan["profile_id"] == "substation_daily_a4"
    assert plan["template_name"] == "substation_daily_a4.html"
    assert plan["context_builder"] == "substation"


def test_resolve_pdf_render_plan_missing_template_falls_back_to_profile_template() -> None:
    plan = _resolve_pdf_render_plan(
        {
            "report": {
                "pdf_profile_id": "substation_daily_generic_a4",
                "pdf_template_name": "missing-template.html",
            }
        }
    )
    assert plan["profile_id"] == "substation_daily_generic_a4"
    assert plan["template_name"] == "substation_daily_generic_a4.html"
    assert plan["context_builder"] == "generic"
