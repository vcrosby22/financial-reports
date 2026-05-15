"""TankerMap public API client for Hormuz traffic evidence.

This source is used as corroborating evidence for the supply-chain cascade,
not as a scoring authority yet. The public endpoints are unauthenticated, but
still treated as external integrations: explicit status, fixture tests, and no
silent default snapshots on failure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import requests
from rich.console import Console

console = Console()

_BASE = "https://tankermap.com/api"
_TIMEOUT = 15

# Public TankerMap Hormuz page showed "Est. Daily Avg 21" on 2026-05-15.
# This is a baseline context value, not a dynamic API field. Validate against
# EIA/IEA/open-source baselines before allowing it to drive scoring.
HORMUZ_NORMAL_DAILY_AVG = 21.0
HORMUZ_BASELINE_SOURCE = "TankerMap Strait of Hormuz page, fetched 2026-05-15"


@dataclass
class TankerMapMarketBar:
    time: date
    close: float
    fetched_at: datetime | None = None


@dataclass
class TankerMapMarketSnapshot:
    status: str
    ticker: str
    latest_close: float | None = None
    latest_date: date | None = None
    source: str = "TankerMap"
    warnings: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class HormuzTrafficSnapshot:
    status: str
    current_zone_vessels: int | None = None
    current_7d_avg: float | None = None
    current_7d_total: int | None = None
    normal_daily_avg: float | None = HORMUZ_NORMAL_DAILY_AVG
    percent_of_normal: float | None = None
    latest_date: date | None = None
    source: str = "TankerMap"
    baseline_source: str = HORMUZ_BASELINE_SOURCE
    crude_7d_total: int | None = None
    product_7d_total: int | None = None
    lng_7d_total: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def severity_label(self) -> str:
        if self.percent_of_normal is None:
            return "unknown"
        if self.percent_of_normal <= 0.1:
            return "severely reduced"
        if self.percent_of_normal <= 0.5:
            return "reduced"
        return "near normal"


def fetch_hormuz_traffic() -> HormuzTrafficSnapshot | None:
    """Fetch and normalize TankerMap Hormuz chokepoint traffic."""
    try:
        data = _get_json(f"{_BASE}/analytics/chokepoints")
        snap = parse_hormuz_traffic(data)
        console.print(
            f"  [dim]TankerMap Hormuz traffic: {snap.current_7d_avg}/day "
            f"vs {snap.normal_daily_avg:.0f}/day normal[/dim]"
        )
        return snap
    except ValueError as e:
        console.print(f"[yellow]TankerMap Hormuz traffic malformed: {e}[/yellow]")
        return HormuzTrafficSnapshot(status="malformed")
    except Exception as e:
        console.print(f"[yellow]TankerMap Hormuz traffic unavailable: {e}[/yellow]")
        return HormuzTrafficSnapshot(status="unreachable")


def fetch_brent_market() -> TankerMapMarketSnapshot | None:
    """Fetch TankerMap Brent market bars for cross-source market context."""
    try:
        return parse_market_bars(_get_json(f"{_BASE}/market-data/brent"))
    except Exception as e:
        console.print(f"[yellow]TankerMap Brent unavailable: {e}[/yellow]")
        return None


def parse_hormuz_traffic(data: dict[str, Any]) -> HormuzTrafficSnapshot:
    """Parse the chokepoint endpoint into a stable Hormuz traffic snapshot."""
    if not isinstance(data, dict):
        raise ValueError("TankerMap chokepoint response is not a JSON object")
    hormuz = (data.get("data") or {}).get("hormuz")
    summary = (data.get("summary") or {}).get("hormuz") or {}
    if not isinstance(hormuz, dict):
        raise ValueError("TankerMap response missing data.hormuz")

    dates = hormuz.get("dates") or []
    totals = hormuz.get("total") or []
    ma7 = hormuz.get("ma7") or []
    if not dates or not totals:
        raise ValueError("TankerMap Hormuz response missing dates/total arrays")

    latest_date = _parse_date(dates[-1])
    current_7d_avg = _last_number(ma7)
    current_7d_total = _sum_last(hormuz.get("total"), 7)
    crude_7d_total = _sum_last(hormuz.get("crude"), 7)
    product_7d_total = _sum_last(hormuz.get("product"), 7)
    lng_7d_total = _sum_last(hormuz.get("lng"), 7)
    current_zone_vessels = _int_or_none(summary.get("live_count"))

    percent_of_normal = None
    if current_7d_avg is not None and HORMUZ_NORMAL_DAILY_AVG > 0:
        percent_of_normal = current_7d_avg / HORMUZ_NORMAL_DAILY_AVG

    return HormuzTrafficSnapshot(
        status="ok",
        current_zone_vessels=current_zone_vessels,
        current_7d_avg=current_7d_avg,
        current_7d_total=current_7d_total,
        percent_of_normal=percent_of_normal,
        latest_date=latest_date,
        crude_7d_total=crude_7d_total,
        product_7d_total=product_7d_total,
        lng_7d_total=lng_7d_total,
        raw={"summary": summary, "meta": data.get("meta", {})},
    )


def parse_market_bars(data: dict[str, Any]) -> TankerMapMarketSnapshot:
    """Parse a TankerMap market-data endpoint."""
    if not isinstance(data, dict):
        raise ValueError("TankerMap market response is not a JSON object")
    bars = data.get("bars") or []
    if not bars:
        raise ValueError("TankerMap market response missing bars")
    latest = bars[-1]
    return TankerMapMarketSnapshot(
        status="ok",
        ticker=str(data.get("ticker") or "unknown"),
        latest_close=float(latest["close"]),
        latest_date=_parse_date(latest["time"]),
        source=(data.get("meta") or {}).get("source", "TankerMap"),
        warnings=list((data.get("meta") or {}).get("warnings") or []),
        raw={"meta": data.get("meta", {}), "latest": latest},
    )


def _get_json(url: str) -> dict[str, Any]:
    r = requests.get(url, timeout=_TIMEOUT, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value[:10])


def _last_number(values: list[Any]) -> float | None:
    for val in reversed(values):
        if val is not None:
            return float(val)
    return None


def _sum_last(values: list[Any] | None, n: int) -> int | None:
    if not values:
        return None
    return int(sum(float(v or 0) for v in values[-n:]))


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
