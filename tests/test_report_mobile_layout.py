"""Regression tests for mobile layout CSS in report HTML."""

from __future__ import annotations

import re
from pathlib import Path

REPORT_PY = Path(__file__).resolve().parents[1] / "src" / "report.py"


def _style_block() -> str:
    text = REPORT_PY.read_text(encoding="utf-8")
    match = re.search(r"<style>(.*?)</style>", text, re.DOTALL)
    assert match, "report template must include embedded <style>"
    return match.group(1)


def test_html_root_does_not_hide_overflow():
    """html { overflow-x: hidden } clips cascade text on iOS when any child is wide."""
    style = _style_block()
    html_rule = re.search(r"html\s*\{([^}]+)\}", style)
    assert html_rule, "missing html {} rule"
    assert "overflow-x" not in html_rule.group(1), (
        "html must not set overflow-x; it clipped the cascade panel on mobile"
    )


def test_section_header_uses_shrinkable_grid():
    style = _style_block()
    assert "grid-template-columns: auto minmax(0, 1fr)" in style
    assert ".section-header" in style


def test_cascade_uses_div_stage_cards_not_table():
    text = REPORT_PY.read_text(encoding="utf-8")
    assert 'class="cascade-stages"' in text
    assert 'class="cascade-stage-table"' not in text
    assert '<article class="cascade-stage"' in text


def test_inactive_tab_panels_use_hidden_inert():
    text = REPORT_PY.read_text(encoding="utf-8")
    assert 'data-tab="cascade" hidden inert' in text
    assert "removeAttribute(\"hidden\")" in text
