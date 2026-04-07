"""EIA Open Data v2 client — US energy fundamentals.

Free API key (email registration).
Env-gated: returns None when EIA_API_KEY is empty or API unreachable.

Docs: https://www.eia.gov/opendata/
"""

from __future__ import annotations

from dataclasses import dataclass

import requests
from rich.console import Console

from ..config import get_settings

console = Console()

_BASE = "https://api.eia.gov/v2"
_TIMEOUT = 15


@dataclass
class EIASnapshot:
    """Key energy storage and price data from EIA."""

    ng_storage_bcf: float | None = None
    ng_storage_yoy_pct: float | None = None
    ng_futures_price: float | None = None


def fetch_eia_data() -> EIASnapshot | None:
    """Fetch natural gas storage + futures price. Returns None on failure."""
    key = get_settings().eia_api_key
    if not key:
        return None

    snap = EIASnapshot()

    try:
        storage = _fetch_series(
            key,
            route="/natural-gas/stor/wkly",
            facets={"process": ["SAL"]},
            sort_col="period",
            limit=2,
        )
        if storage and len(storage) >= 1:
            snap.ng_storage_bcf = _float(storage[0].get("value"))
            if len(storage) >= 2 and storage[1].get("value"):
                prev = _float(storage[1].get("value"))
                if prev and prev > 0 and snap.ng_storage_bcf is not None:
                    snap.ng_storage_yoy_pct = (
                        (snap.ng_storage_bcf - prev) / prev
                    ) * 100

        futures = _fetch_series(
            key,
            route="/natural-gas/pri/fut",
            facets={"series": ["RNGC1"]},
            sort_col="period",
            limit=1,
        )
        if futures and len(futures) >= 1:
            snap.ng_futures_price = _float(futures[0].get("value"))

        console.print(
            f"  [dim]EIA: NG storage={snap.ng_storage_bcf} Bcf, "
            f"futures=${snap.ng_futures_price}[/dim]"
        )
        return snap

    except Exception as e:
        console.print(f"[yellow]EIA unavailable: {e}[/yellow]")
        return None


def _fetch_series(
    key: str,
    route: str,
    facets: dict | None = None,
    sort_col: str = "period",
    limit: int = 1,
) -> list[dict] | None:
    url = f"{_BASE}{route}/data/"
    params: dict = {
        "api_key": key,
        "frequency": "weekly",
        "data[0]": "value",
        "sort[0][column]": sort_col,
        "sort[0][direction]": "desc",
        "length": limit,
    }
    if facets:
        for facet_key, facet_vals in facets.items():
            for v in facet_vals:
                params[f"facets[{facet_key}][]"] = v

    try:
        r = requests.get(url, params=params, timeout=_TIMEOUT)
        if r.status_code == 200:
            data = r.json().get("response", {}).get("data", [])
            return data if data else None
        return None
    except Exception:
        return None


def _float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None
