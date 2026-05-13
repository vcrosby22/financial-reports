"""Hormuz Monitor API client — Strait of Hormuz risk + traffic data.

Free tier: 60 req/hr, 15-min data refresh, no credit card.
Env-gated: returns None when HORMUZ_API_KEY is empty or API unreachable.

Docs: https://hormuzmonitor.com/hormuz-monitor-api/
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import requests
from rich.console import Console

from ..config import get_settings

console = Console()

# The marketing site at hormuzmonitor.com advertises
#   https://api.hormuzmonitor.com/v2
# but that subdomain has no DNS A record (NXDOMAIN from every public
# resolver, including the parent's authoritative Bluehost nameservers
# ns1/ns2.bluehost.com). The actual API is served by a WordPress
# plugin at the Bluehost-hosted origin below. Same auth scheme
# (X-API-Key), same response envelope ({"status","data":{...}}).
# Verified live 2026-05-13: returns risk_score 9.1 / crisis_active true.
# See BL-105 in the workspace BACKLOG.md for the full incident.
_BASE = "https://mhh.gic.mybluehost.me/wp-json/hlapi/v2"
_TIMEOUT = 15


@dataclass
class HormuzSnapshot:
    """Combined snapshot from /risk + /traffic + /crisis + /prices."""

    risk_score: float = 0.0
    risk_level: str = "unknown"
    risk_trend: str = "stable"
    crisis_active: bool = False

    transits_today: int | None = None
    pre_crisis_avg: float | None = None
    reduction_pct: float | None = None
    lane_status: str | None = None
    dark_ships_24h: int | None = None

    oil_disrupted_mbd: float | None = None
    brent_change_since_onset: float | None = None
    duration_days: int | None = None
    resolution_signal: bool | None = None
    crisis_severity: str | None = None

    war_risk_premium_pct: float | None = None
    brent_usd: float | None = None

    raw: dict[str, Any] = field(default_factory=dict)


def fetch_hormuz_data() -> HormuzSnapshot | None:
    """Fetch all free-tier Hormuz Monitor endpoints. Returns None on failure.

    The Hormuz Monitor v2 API uses ``X-API-Key`` for auth (NOT
    ``Authorization: Bearer``) and wraps every payload as
    ``{"status": "success", "data": {...}}``. The ``_payload`` helper
    here unwraps the ``data`` envelope so callers see field names
    directly.

    See https://hormuzmonitor.com/hormuz-monitor-api/ §02 / §03.
    """
    key = get_settings().hormuz_api_key
    if not key:
        return None

    headers = {"X-API-Key": key, "Accept": "application/json"}
    snap = HormuzSnapshot()
    diag = {"reachable": False, "status": None, "error": None}

    try:
        risk = _payload(_get(f"{_BASE}/risk", headers, diag))
        if risk:
            snap.risk_score = float(risk.get("risk_score", 0) or 0)
            snap.risk_level = risk.get("risk_level", "unknown") or "unknown"
            snap.risk_trend = risk.get("trend", "stable") or "stable"
            snap.crisis_active = bool(risk.get("crisis_active", False))
            snap.raw["risk"] = risk

        traffic = _payload(_get(f"{_BASE}/traffic", headers, diag))
        if traffic:
            snap.transits_today = traffic.get("transits_today")
            snap.pre_crisis_avg = traffic.get("pre_crisis_avg")
            snap.reduction_pct = traffic.get("reduction_pct")
            snap.lane_status = traffic.get("inbound_lane_status") or traffic.get("lane_status")
            snap.dark_ships_24h = traffic.get("dark_ships_detected_24h")
            snap.raw["traffic"] = traffic

        crisis = _payload(_get(f"{_BASE}/crisis", headers, diag))
        if crisis:
            snap.oil_disrupted_mbd = crisis.get("oil_supply_disrupted_mbd")
            snap.brent_change_since_onset = crisis.get("brent_change_since_onset")
            snap.duration_days = crisis.get("duration_days")
            snap.resolution_signal = crisis.get("resolution_signal")
            snap.crisis_severity = crisis.get("severity")
            snap.raw["crisis"] = crisis

        prices = _payload(_get(f"{_BASE}/prices", headers, diag))
        if prices:
            snap.war_risk_premium_pct = prices.get("war_risk_premium_pct")
            snap.brent_usd = prices.get("brent_usd")
            snap.raw["prices"] = prices

        # Distinguish "endpoint reachable, calm state" from "endpoint
        # unreachable / auth failure / DNS NXDOMAIN" — both used to
        # silently return risk=0.0 (unknown).
        if not diag["reachable"]:
            console.print(
                f"[yellow]Hormuz Monitor: API unreachable "
                f"(status={diag['status']}, err={diag['error']})[/yellow]"
            )
            return None

        console.print(
            f"  [dim]Hormuz Monitor: risk={snap.risk_score:.1f} "
            f"({snap.risk_level})[/dim]"
        )
        return snap

    except Exception as e:
        console.print(f"[yellow]Hormuz Monitor unavailable: {e}[/yellow]")
        return None


def _get(url: str, headers: dict, diag: dict | None = None) -> dict | None:
    """Fetch JSON; record reachability + status into ``diag`` for the caller."""
    try:
        r = requests.get(url, headers=headers, timeout=_TIMEOUT)
        if diag is not None:
            diag["status"] = r.status_code
            if r.status_code == 200:
                diag["reachable"] = True
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        if diag is not None:
            diag["error"] = str(e)[:120]
        return None


def _payload(envelope: dict | None) -> dict | None:
    """Unwrap the ``{"status": "success", "data": {...}}`` envelope.

    The v2 API always wraps its payload under a top-level ``data`` key;
    callers care only about the inner dict. Returns ``None`` if the
    envelope is missing, errored, or malformed.
    """
    if not envelope:
        return None
    if envelope.get("status") and envelope.get("status") != "success":
        return None
    data = envelope.get("data")
    if isinstance(data, dict):
        return data
    # Defensive fallback: some staging endpoints occasionally return the
    # payload at the top level. Only trust this when the expected fields
    # are present, to avoid swallowing an error envelope.
    if any(k in envelope for k in ("risk_score", "transits_today", "crisis_active", "brent_usd")):
        return envelope
    return None
