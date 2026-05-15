"""TankerMap parser tests based on recorded public JSON response shapes."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from src.data.tankermap import (
    HORMUZ_NORMAL_DAILY_AVG,
    fetch_hormuz_traffic,
    parse_hormuz_traffic,
    parse_market_bars,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    with (FIXTURE_DIR / name).open() as f:
        return json.load(f)


def test_parse_hormuz_traffic_normalizes_grounded_metrics():
    snap = parse_hormuz_traffic(_load("tankermap_chokepoints.json"))

    assert snap.status == "ok"
    assert snap.current_zone_vessels == 1
    assert snap.current_7d_avg == 0.4
    assert snap.current_7d_total == 3
    assert snap.crude_7d_total == 1
    assert snap.product_7d_total == 0
    assert snap.lng_7d_total == 2
    assert snap.normal_daily_avg == HORMUZ_NORMAL_DAILY_AVG
    assert round(snap.percent_of_normal or 0, 3) == 0.019
    assert snap.severity_label == "severely reduced"
    assert snap.latest_date.isoformat() == "2026-05-15"


def test_parse_market_bars_uses_latest_bar():
    snap = parse_market_bars(_load("tankermap_brent.json"))

    assert snap.status == "ok"
    assert snap.ticker == "BRENT"
    assert snap.latest_close == 109.33
    assert snap.latest_date.isoformat() == "2026-05-15"
    assert "KuzTerm" in snap.source


def test_parse_hormuz_traffic_rejects_malformed_shape():
    try:
        parse_hormuz_traffic({"data": {}})
    except ValueError as e:
        assert "data.hormuz" in str(e)
    else:
        raise AssertionError("malformed response should raise ValueError")


def test_fetch_hormuz_traffic_returns_explicit_status_on_unreachable():
    with patch("src.data.tankermap._get_json", side_effect=TimeoutError("timeout")):
        snap = fetch_hormuz_traffic()

    assert snap is not None
    assert snap.status == "unreachable"
    assert snap.current_7d_avg is None
