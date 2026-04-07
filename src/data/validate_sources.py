"""Preflight checks for FRED series registry and Yahoo Finance symbols.

External providers rename or retire identifiers; this cannot be fully prevented,
but we can fail fast in CI or before a publish run.
"""

from __future__ import annotations

import yfinance as yf
from rich.console import Console

from ..config import get_settings, load_config
from .macro import FRED_SERIES

console = Console()


def collect_report_symbols(config: dict) -> list[str]:
    """Symbol universe used for price data in the HTML report, deduped, order preserved."""
    seen: set[str] = set()
    out: list[str] = []
    for sym in config.get("market_indices", []):
        if sym not in seen:
            seen.add(sym)
            out.append(sym)
    wl = config.get("watchlist", {})
    for key in ("stocks", "etfs"):
        for sym in wl.get(key) or []:
            if sym not in seen:
                seen.add(sym)
                out.append(sym)
    commodity_tickers = config.get("commodities", []) or wl.get("commodities") or []
    for sym in commodity_tickers:
        if sym not in seen:
            seen.add(sym)
            out.append(sym)
    for sym in config.get("supply_chain_proxies", []) or []:
        if sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def validate_fred_series() -> list[tuple[str, str]]:
    """Return (series_id, message) for each ID that does not resolve via FRED (metadata only)."""
    settings = get_settings()
    if not settings.fred_api_key:
        return []
    try:
        from fredapi import Fred

        fred = Fred(api_key=settings.fred_api_key)
    except Exception as e:
        return [("__connection__", str(e))]

    failures: list[tuple[str, str]] = []
    for series_id, name, _cat in FRED_SERIES:
        try:
            fred.get_series_info(series_id)
        except Exception as e:
            failures.append((series_id, f"{name}: {e}"))
    return failures


def validate_yahoo_symbols(symbols: list[str]) -> list[str]:
    """Symbols with no recent price history (delisted, wrong ticker, or transient Yahoo outage)."""
    bad: list[str] = []
    for sym in symbols:
        try:
            hist = yf.Ticker(sym).history(period="1mo")
            if hist is None or hist.empty:
                bad.append(sym)
        except Exception:
            bad.append(sym)
    return bad


def run_validate_sources() -> int:
    """Print a summary; return 0 if all checks pass, 1 if any hard failure."""
    config = load_config()
    ok = True

    settings = get_settings()
    if not settings.fred_api_key:
        console.print("[yellow]FRED: skipped (no FRED_API_KEY — export or add to .env)[/yellow]")
    else:
        console.print("[bold]Validating FRED series IDs (metadata)…[/bold]")
        fred_fail = validate_fred_series()
        if fred_fail:
            ok = False
            if fred_fail[0][0] == "__connection__":
                console.print(f"[red]FRED connection / client error: {fred_fail[0][1]}[/red]")
            else:
                for sid, msg in fred_fail:
                    console.print(f"  [red]{sid}[/red] — {msg}")
                console.print(
                    f"[red]FRED: {len(fred_fail)} series ID(s) invalid or unreachable.[/red]"
                )
        else:
            console.print(f"[green]FRED: all {len(FRED_SERIES)} series IDs resolve.[/green]")

    symbols = collect_report_symbols(config)
    console.print(f"\n[bold]Validating Yahoo Finance symbols ({len(symbols)} tickers, ~1mo history)…[/bold]")
    bad = validate_yahoo_symbols(symbols)
    if bad:
        ok = False
        for sym in bad:
            console.print(
                f"  [red]{sym}[/red] — no price history (delisted, renamed, or Yahoo outage)"
            )
        console.print(f"[red]Yahoo: {len(bad)} symbol(s) failed.[/red]")
    else:
        console.print(f"[green]Yahoo: all {len(symbols)} symbols returned history.[/green]")

    if ok:
        console.print("\n[green]All source checks passed.[/green]")
    else:
        console.print(
            "\n[yellow]Update FRED_SERIES in src/data/macro.py and/or tickers in config.yaml, "
            "then re-run.[/yellow]"
        )
    return 0 if ok else 1
