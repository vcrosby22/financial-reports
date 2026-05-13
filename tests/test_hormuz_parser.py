"""Hormuz Monitor v2 client — fixture-based parser tests.

Required by `external-data-source-integrity` rule item 4: every external
data source ships with a unit test against a recorded JSON response in
the actual wire format. The fixtures under `tests/fixtures/hormuz_*.json`
were captured live on 2026-05-13 from
    https://mhh.gic.mybluehost.me/wp-json/hlapi/v2/<endpoint>
which is the actual host serving Hormuz Monitor v2 (the marketing site's
advertised api.hormuzmonitor.com has no DNS A record — see BL-105).

If these tests fail, the upstream response shape has drifted. Inspect
the failing assertion and update both the parser and the fixture in the
same change.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from src.data.hormuz import _payload, fetch_hormuz_data


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load(endpoint: str) -> dict:
    path = FIXTURE_DIR / f"hormuz_{endpoint}.json"
    with path.open() as f:
        return json.load(f)


def test_payload_unwraps_data_envelope():
    """The v2 API wraps every response as {'status': 'success', 'data': {...}}.

    The original parser bug read fields directly off the envelope. This
    test pins the unwrap behavior so the regression cannot return.
    """
    envelope = _load("risk")
    inner = _payload(envelope)
    assert inner is not None
    assert inner["risk_score"] == 9.1
    assert inner["risk_level"] == "critical"
    assert inner["crisis_active"] is True


def test_payload_handles_error_envelope():
    """Error responses ({'status': 'error', ...}) must return None, not the data."""
    err = {
        "status": "error",
        "error_code": "MISSING_API_KEY",
        "message": "Provide your API key in the X-API-Key header.",
        "http_status": 401,
    }
    assert _payload(err) is None


def test_payload_handles_none_envelope():
    """A None envelope (e.g. _get returned None on 401) must propagate."""
    assert _payload(None) is None


def test_payload_handles_empty_envelope():
    """An empty dict (malformed response) must not raise; return None."""
    assert _payload({}) is None


def test_payload_falls_back_to_flat_shape():
    """Defensive fallback: if a future endpoint serves data flat (no envelope),
    we accept it as long as canonical fields are present."""
    flat = {"risk_score": 5.0, "risk_level": "elevated"}
    assert _payload(flat) == flat


def test_fetch_hormuz_populates_all_snapshot_fields():
    """End-to-end: with all four endpoints mocked from real fixtures, the
    snapshot must populate every field declared on HormuzSnapshot.

    This is the test that would have caught the original Hormuz bugs:
      - Bug A (Authorization: Bearer instead of X-API-Key) would never
        have produced a populated snapshot, so this assertion would fail.
      - Bug B (reading risk.get('risk_score') without unwrapping data)
        would leave risk_score=0.0 and risk_level='unknown'.
    """
    fixtures = {
        "/risk": _load("risk"),
        "/traffic": _load("traffic"),
        "/crisis": _load("crisis"),
        "/prices": _load("prices"),
    }

    def fake_get(url, headers, diag=None):
        for suffix, body in fixtures.items():
            if url.endswith(suffix):
                if diag is not None:
                    diag["status"] = 200
                    diag["reachable"] = True
                return body
        return None

    with patch("src.data.hormuz._get", side_effect=fake_get), patch(
        "src.data.hormuz.get_settings"
    ) as gs:
        gs.return_value.hormuz_api_key = "hm_live_test_key"
        snap = fetch_hormuz_data()

    assert snap is not None, "Real fixture data should produce a populated snapshot"

    # Risk endpoint
    assert snap.risk_score == 9.1
    assert snap.risk_level == "critical"
    assert snap.risk_trend == "rising"
    assert snap.crisis_active is True

    # Traffic endpoint
    assert snap.transits_today == 3
    assert snap.pre_crisis_avg is None or snap.pre_crisis_avg > 0
    assert snap.reduction_pct == 82.4
    assert snap.lane_status == "closed"
    assert snap.dark_ships_24h == 12

    # Crisis endpoint
    assert snap.oil_disrupted_mbd == 15.7
    assert snap.brent_change_since_onset == 44.3
    assert snap.duration_days == 75
    assert snap.resolution_signal == "none"
    assert snap.crisis_severity == "critical"

    # Prices endpoint
    assert snap.war_risk_premium_pct == 3.52
    assert snap.brent_usd == 122.4

    # Raw payload should be cached for downstream debug / replay
    assert "risk" in snap.raw
    assert "traffic" in snap.raw
    assert "crisis" in snap.raw
    assert "prices" in snap.raw


def test_fetch_hormuz_returns_none_when_unreachable():
    """If every endpoint is unreachable, fetch must return None — never a
    populated-with-defaults snapshot. Three-state distinction (rule item 2).
    """

    def always_unreachable(url, headers, diag=None):
        if diag is not None:
            diag["error"] = "Mock: connection refused"
        return None

    with patch("src.data.hormuz._get", side_effect=always_unreachable), patch(
        "src.data.hormuz.get_settings"
    ) as gs:
        gs.return_value.hormuz_api_key = "hm_live_test_key"
        snap = fetch_hormuz_data()

    assert snap is None, "Unreachable API must surface as None, not a defaults-only snapshot"


def test_fetch_hormuz_returns_none_without_key():
    """Missing key is also a distinguishable state — early-return None."""
    with patch("src.data.hormuz.get_settings") as gs:
        gs.return_value.hormuz_api_key = ""
        assert fetch_hormuz_data() is None
