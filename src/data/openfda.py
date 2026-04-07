"""openFDA drug shortages client — US drug shortage data.

Free API; key recommended for higher rate limits.
Env-gated: works without key for light use, returns None on failure.

Docs: https://open.fda.gov/apis/drug/drugshortages/
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import requests
from rich.console import Console

from ..config import get_settings

console = Console()

_BASE = "https://api.fda.gov/drug/shortages.json"
_TIMEOUT = 15


@dataclass
class FDAShortageSnapshot:
    """Rolling shortage statistics from openFDA."""

    total_active: int = 0
    new_last_30d: int = 0
    new_last_90d: int = 0


def fetch_fda_shortages() -> FDAShortageSnapshot | None:
    """Fetch rolling drug shortage counts. Returns None on failure."""
    key = get_settings().fda_api_key
    params: dict = {"limit": 1}
    if key:
        params["api_key"] = key

    try:
        snap = FDAShortageSnapshot()

        r_total = requests.get(_BASE, params={**params, "limit": 1}, timeout=_TIMEOUT)
        if r_total.status_code == 200:
            data = r_total.json()
            meta = data.get("meta", {}).get("results", {})
            snap.total_active = meta.get("total", 0)

        now = datetime.utcnow()
        d30 = (now - timedelta(days=30)).strftime("%Y%m%d")
        d90 = (now - timedelta(days=90)).strftime("%Y%m%d")
        today_str = now.strftime("%Y%m%d")

        count_30 = _count_since(key, d30, today_str)
        if count_30 is not None:
            snap.new_last_30d = count_30

        count_90 = _count_since(key, d90, today_str)
        if count_90 is not None:
            snap.new_last_90d = count_90

        console.print(
            f"  [dim]openFDA shortages: {snap.total_active} active, "
            f"{snap.new_last_30d} new (30d)[/dim]"
        )
        return snap

    except Exception as e:
        console.print(f"[yellow]openFDA unavailable: {e}[/yellow]")
        return None


def _count_since(key: str | None, start: str, end: str) -> int | None:
    """Count shortage records created between start and end (YYYYMMDD)."""
    params: dict = {
        "search": f"meta.date_created:[{start}+TO+{end}]",
        "limit": 1,
    }
    if key:
        params["api_key"] = key
    try:
        r = requests.get(_BASE, params=params, timeout=_TIMEOUT)
        if r.status_code == 200:
            return r.json().get("meta", {}).get("results", {}).get("total", 0)
        return None
    except Exception:
        return None
