from __future__ import annotations

from app.schema_defs import merge_site_env_configs, normalize_site_env_config, site_env_templates
from app.utils import _render_pdf_html_template, _resolve_pdf_render_plan


def test_normalize_site_env_config_report_fields() -> None:
    cfg = normalize_site_env_config(
        {
            "report": {
                "pdf_profile_id": " substation_daily_generic_a4 ",
                "locked_profile_id": " substation_daily_a4 ",
                "page_margin_mm": " 7.5 ",
                "pdf_template_name": "../substation_daily_generic_a4.html",
            }
        }
    )
    assert cfg["report"]["pdf_profile_id"] == "substation_daily_generic_a4"
    assert cfg["report"]["locked_profile_id"] == "substation_daily_a4"
    assert cfg["report"]["page_margin_mm"] == 7.5
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


def test_normalize_site_env_config_invalid_report_page_margin_removed() -> None:
    cfg = normalize_site_env_config({"report": {"page_margin_mm": "abc"}})
    assert cfg == {}


def test_normalize_site_env_config_report_page_margin_clamped() -> None:
    cfg = normalize_site_env_config({"report": {"page_margin_mm": "26.88"}})
    assert cfg["report"]["page_margin_mm"] == 20.0


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
    assert "report_substation_ami4_locked" in templates
    assert templates["report_substation_a4"]["config"]["report"]["pdf_profile_id"] == "substation_daily_a4"
    assert templates["report_generic_a4"]["config"]["report"]["pdf_profile_id"] == "substation_daily_generic_a4"
    assert templates["report_substation_ami4_locked"]["config"]["report"]["locked_profile_id"] == "substation_daily_ami4_a4"


def test_resolve_pdf_render_plan_default_profile() -> None:
    plan = _resolve_pdf_render_plan({})
    assert plan["profile_id"] == "substation_daily_a4"
    assert plan["template_name"] == "substation_daily_a4.html"
    assert plan["context_builder"] == "substation"
    assert plan["page_margin_mm"] == 4.0


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


def test_resolve_pdf_render_plan_locked_profile_overrides_requested_profile_and_template() -> None:
    plan = _resolve_pdf_render_plan(
        {
            "report": {
                "pdf_profile_id": "substation_daily_generic_a4",
                "locked_profile_id": "substation_daily_a4",
                "pdf_template_name": "substation_daily_generic_a4.html",
            }
        }
    )
    assert plan["profile_id"] == "substation_daily_a4"
    assert plan["template_name"] == "substation_daily_a4.html"
    assert plan["context_builder"] == "substation"
    assert plan["locked_profile_id"] == "substation_daily_a4"


def test_resolve_pdf_render_plan_unknown_locked_profile_ignored() -> None:
    plan = _resolve_pdf_render_plan(
        {
            "report": {
                "locked_profile_id": "missing-profile",
                "pdf_profile_id": "substation_daily_generic_a4",
            }
        }
    )
    assert plan["profile_id"] == "substation_daily_generic_a4"
    assert plan["template_name"] == "substation_daily_generic_a4.html"
    assert plan["context_builder"] == "generic"


def test_resolve_pdf_render_plan_page_margin_clamped() -> None:
    plan = _resolve_pdf_render_plan({"report": {"page_margin_mm": "-2"}})
    assert plan["page_margin_mm"] == 0.0

    plan = _resolve_pdf_render_plan({"report": {"page_margin_mm": "99"}})
    assert plan["page_margin_mm"] == 20.0


def test_render_pdf_html_template_applies_page_margin() -> None:
    html, _template_dir, _template_name = _render_pdf_html_template(
        site_name="테스트단지",
        date="2026-02-23",
        tabs={},
        site_env_config={"report": {"page_margin_mm": 6.5}},
    )
    assert "margin: 6.5mm;" in html
    assert "margin: 10mm;" not in html
