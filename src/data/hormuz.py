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

_BASE = "https://api.hormuzmonitor.com/v2"
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
    """Fetch all free-tier Hormuz Monitor endpoints. Returns None on failure."""
    key = get_settings().hormuz_api_key
    if not key:
        return None

    headers = {"Authorization": f"Bearer {key}", "Accept": "application/json"}
    snap = HormuzSnapshot()

    try:
        risk = _get(f"{_BASE}/risk", headers)
        if risk:
            snap.risk_score = float(risk.get("risk_score", 0))
            snap.risk_level = risk.get("risk_level", "unknown")
            snap.risk_trend = risk.get("trend", "stable")
            snap.crisis_active = bool(risk.get("crisis_active", False))
            snap.raw["risk"] = risk

        traffic = _get(f"{_BASE}/traffic", headers)
        if traffic:
            snap.transits_today = traffic.get("transits_today")
            snap.pre_crisis_avg = traffic.get("pre_crisis_avg")
            snap.reduction_pct = traffic.get("reduction_pct")
            snap.lane_status = traffic.get("inbound_lane_status") or traffic.get("lane_status")
            snap.dark_ships_24h = traffic.get("dark_ships_detected_24h")
            snap.raw["traffic"] = traffic

        crisis = _get(f"{_BASE}/crisis", headers)
        if crisis:
            snap.oil_disrupted_mbd = crisis.get("oil_supply_disrupted_mbd")
            snap.brent_change_since_onset = crisis.get("brent_change_since_onset")
            snap.duration_days = crisis.get("duration_days")
            snap.resolution_signal = crisis.get("resolution_signal")
            snap.crisis_severity = crisis.get("severity")
            snap.raw["crisis"] = crisis

        prices = _get(f"{_BASE}/prices", headers)
        if prices:
            snap.war_risk_premium_pct = prices.get("war_risk_premium_pct")
            snap.brent_usd = prices.get("brent_usd")
            snap.raw["prices"] = prices

        console.print(f"  [dim]Hormuz Monitor: risk={snap.risk_score:.1f} ({snap.risk_level})[/dim]")
        return snap

    except Exception as e:
        console.print(f"[yellow]Hormuz Monitor unavailable: {e}[/yellow]")
        return None


def _get(url: str, headers: dict) -> dict | None:
    try:
        r = requests.get(url, headers=headers, timeout=_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None
