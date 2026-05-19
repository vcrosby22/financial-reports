"""Regression tests for mobile layout CSS in report HTML."""

from __future__ import annotations

import re
from pathlib import Path

REPORT_PY = Path(__file__).resolve().parents[1] / "src" / "report.py"
DOCS_HTML = Path(__file__).resolve().parents[1] / "docs" / "index.html"


def _style_block() -> str:
    text = REPORT_PY.read_text(encoding="utf-8")
    match = re.search(r"<style>(.*?)</style>", text, re.DOTALL)
    assert match, "report template must include embedded <style>"
    return match.group(1)


def _rendered_css(style: str) -> str:
    """Unescape f-string braces so CSS rules match generated HTML."""
    return style.replace("{{", "{").replace("}}", "}")


def test_html_root_does_not_hide_overflow():
    """html { overflow-x: hidden } clips cascade text on iOS when any child is wide."""
    style = _rendered_css(_style_block())
    html_rule = re.search(r"html\s*\{([^}]+)\}", style, re.DOTALL)
    assert html_rule, "missing html {} rule"
    assert "overflow-x" not in html_rule.group(1), (
        "html must not set overflow-x; it clipped the cascade panel on mobile"
    )


def test_body_overflow_hidden_only_from_tablet():
    style = _rendered_css(_style_block())
    body_rule = re.search(r"^body\s*\{([^}]+)\}", style, re.MULTILINE | re.DOTALL)
    assert body_rule, "missing body {} rule"
    assert "overflow-x" not in body_rule.group(1), (
        "body must not clip overflow on phones; use @media (min-width: 768px)"
    )
    assert re.search(
        r"@media\s*\(min-width:\s*768px\)\s*\{[^}]*body\s*\{[^}]*overflow-x:\s*hidden",
        style,
        re.DOTALL,
    ), "tablet+ body overflow-x guard expected"


def test_section_header_uses_shrinkable_grid():
    style = _rendered_css(_style_block())
    assert "grid-template-columns: auto minmax(0, 1fr)" in style
    assert re.search(
        r"\.section-header\s*\{[^}]*display:\s*grid",
        style,
        re.DOTALL,
    ), ".section-header must use grid so long titles wrap"


def test_cascade_uses_div_stage_cards_not_table():
    text = REPORT_PY.read_text(encoding="utf-8")
    assert 'class="cascade-stages"' in text
    assert 'class="cascade-stage-table"' not in text
    assert '<article class="cascade-stage"' in text


def test_inactive_tab_panels_use_hidden_inert():
    text = REPORT_PY.read_text(encoding="utf-8")
    assert 'data-tab="cascade" hidden inert' in text
    assert 'removeAttribute("hidden")' in text


def test_committed_docs_html_matches_mobile_css_contract():
    """docs/index.html on main must not ship the old html/body clip rules."""
    if not DOCS_HTML.is_file():
        return
    html = DOCS_HTML.read_text(encoding="utf-8")
    style_match = re.search(r"<style>(.*?)</style>", html, re.DOTALL)
    assert style_match, "docs/index.html must include embedded styles"
    style = style_match.group(1)
    html_rule = re.search(r"html\s*\{([^}]+)\}", style, re.DOTALL)
    assert html_rule and "overflow-x" not in html_rule.group(1)
    body_rule = re.search(r"^body\s*\{([^}]+)\}", style, re.MULTILINE | re.DOTALL)
    assert body_rule and "overflow-x" not in body_rule.group(1)
