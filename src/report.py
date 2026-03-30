"""Generate a static HTML report from the latest market scan data.

Usage: python -m src report
Produces: reports/market-report-YYYY-MM-DD.html
"""

import os
import webbrowser
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from rich.console import Console

from .analysis.bond_bank_narrative import build_bond_bank_friend_html
from .personal.historical import CRASHES, crash_comparison_for_dashboard
from .analysis.memory import build_trend_context
from .analysis.opportunities import Opportunity, screen_opportunities
from .analysis.risk import (
    MarketHealthReport,
    assess_market_health,
    get_position_guidance,
)
from .config import load_config
from .data.database import get_session, init_db
from .data.models import MarketSnapshot
from .data.risk_score_log import RiskTrend, append_risk_score_log, compute_trend
from .data.crypto import fetch_crypto_data
from .data.forex import fetch_forex_data
from .data.fundamentals import StockFundamentals, fetch_fundamentals_batch
from .data.macro import MacroSnapshot, fetch_macro_data
from .data.stocks import fetch_market_indices, fetch_multiple

console = Console()

REPORTS_DIR = Path(__file__).parent.parent / "reports"


def _history_last_timestamp(hist) -> datetime | None:
    """Last OHLCV bar timestamp from yfinance history, as America/New_York."""
    if hist is None or getattr(hist, "empty", True):
        return None
    et = ZoneInfo("America/New_York")
    ts = hist.index[-1]
    if hasattr(ts, "to_pydatetime"):
        dt = ts.to_pydatetime()
    else:
        dt = ts
    if isinstance(dt, date) and not isinstance(dt, datetime):
        dt = datetime.combine(dt, datetime.min.time())
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=et)
    else:
        dt = dt.astimezone(et)
    return dt


def _market_prices_as_of_display(market_data: dict) -> tuple[str, datetime | None]:
    """Return (human-readable label, latest moment in ET or None).

    Combines yfinance last bars (indices, stocks, ETFs, forex) and crypto exchange quote times.
    """
    et = ZoneInfo("America/New_York")
    utc = ZoneInfo("UTC")
    moments_utc: list[datetime] = []

    for cat in ("indices", "stocks", "etfs", "forex"):
        for item in market_data.get(cat, []):
            if not isinstance(item, dict):
                continue
            dt = _history_last_timestamp(item.get("history"))
            if dt is not None:
                moments_utc.append(dt.astimezone(utc))

    for item in market_data.get("crypto", []):
        if not isinstance(item, dict):
            continue
        qt = item.get("quote_time")
        if qt is not None:
            if not isinstance(qt, datetime):
                continue
            if qt.tzinfo is None:
                qt = qt.replace(tzinfo=timezone.utc)
            moments_utc.append(qt.astimezone(utc))

    if not moments_utc:
        return "Unavailable (no price history)", None

    latest_et = max(moments_utc).astimezone(et)
    if latest_et.hour == 0 and latest_et.minute == 0 and latest_et.second == 0:
        label = (
            f'{latest_et.strftime("%B %d, %Y")} '
            "(last bar date in data; US-listed names usually last regular session)"
        )
    else:
        label = latest_et.strftime("%B %d, %Y at %I:%M %p ET")
    return label, latest_et


def _fred_observations_html(macro_data: MacroSnapshot | None) -> str:
    if macro_data and macro_data.indicators:
        if macro_data.fred_observations_through:
            d = macro_data.fred_observations_through.strftime("%B %d, %Y")
            return (
                f'<div class="subtitle-line subtitle-detail">Macro (FRED) observations through: <strong>{escape(d)}</strong> '
                f"(release cadence varies by series).</div>"
            )
        return (
            '<div class="subtitle-line subtitle-detail">Macro (FRED): loaded, but observation dates unavailable.</div>'
        )
    return (
        '<div class="subtitle-line subtitle-detail">Macro (FRED): not in this snapshot '
        "(missing API key or fetch failed).</div>"
    )


def generate_report(output_path: str | None = None, open_browser: bool = True):
    """Collect all data and generate a static HTML report.

    Args:
        output_path: If set, write the report to this exact path instead of reports/.
        open_browser: If False, skip opening the report in the browser (for CI).
    """
    config = load_config()
    watchlist = config.get("watchlist", {})

    console.print("\n[bold]Collecting data for report...[/bold]")

    console.print("  Market indices...")
    indices = fetch_market_indices(config.get("market_indices", []))
    console.print("  Stocks...")
    stocks = fetch_multiple(watchlist.get("stocks", []), asset_type="stock")
    console.print("  ETFs...")
    etfs = fetch_multiple(watchlist.get("etfs", []), asset_type="etf")
    crypto = []
    if watchlist.get("crypto"):
        console.print("  Crypto...")
        crypto = fetch_crypto_data(watchlist["crypto"])
    console.print("  Forex...")
    forex = fetch_forex_data(watchlist.get("forex", []))
    commodities = []
    commodity_tickers = config.get("commodities", []) or watchlist.get("commodities", [])
    if commodity_tickers:
        console.print("  Commodities (oil)...")
        commodities = fetch_multiple(commodity_tickers, asset_type="commodity")

    market_data = {"indices": indices, "stocks": stocks, "etfs": etfs, "crypto": crypto, "forex": forex, "commodities": commodities}

    console.print("  Macro indicators (FRED)...")
    macro_data = fetch_macro_data()

    console.print("  Fundamentals...")
    stock_symbols = watchlist.get("stocks", [])
    fundamentals = fetch_fundamentals_batch(stock_symbols) if stock_symbols else {}

    console.print("  Risk assessment...")
    health = assess_market_health(
        market_data, config.get("risk_thresholds", {}),
        macro_data=macro_data, fundamentals_data=fundamentals,
    )

    console.print("  Saving snapshots...")
    init_db()
    session = get_session()
    for category in ["indices", "stocks", "etfs", "crypto", "forex", "commodities"]:
        for item in market_data.get(category, []):
            snapshot = MarketSnapshot(
                ticker=item["ticker"],
                asset_type=item.get("asset_type", category),
                price=item.get("price"),
                open_price=item.get("open_price"),
                high=item.get("high"),
                low=item.get("low"),
                volume=item.get("volume"),
                market_cap=item.get("market_cap"),
                change_pct_1d=item.get("change_pct_1d"),
                change_pct_1w=item.get("change_pct_1w"),
                change_pct_1m=item.get("change_pct_1m"),
                pe_ratio=item.get("pe_ratio"),
                pb_ratio=item.get("pb_ratio"),
                dividend_yield=item.get("dividend_yield"),
                fifty_day_ma=item.get("fifty_day_ma"),
                two_hundred_day_ma=item.get("two_hundred_day_ma"),
                rsi_14=item.get("rsi_14"),
            )
            session.add(snapshot)
    session.commit()
    session.close()

    trend_context = build_trend_context()

    console.print("  Screening opportunities...")
    opportunities = screen_opportunities(market_data, fundamentals, macro_data, health)
    console.print(f"  Found {len(opportunities)} opportunities.")

    console.print("  Computing risk trend...")
    risk_trend = compute_trend(health)
    if risk_trend.has_any:
        parts = []
        if risk_trend.delta_1d is not None:
            parts.append(f"1d: {risk_trend.delta_1d:+d}")
        if risk_trend.delta_1w is not None:
            parts.append(f"1w: {risk_trend.delta_1w:+d}")
        if risk_trend.delta_1m is not None:
            parts.append(f"1m: {risk_trend.delta_1m:+d}")
        console.print(f"  Risk trend: {', '.join(parts)}")
    else:
        console.print("  [dim]No prior risk data — trend indicator hidden.[/dim]")

    console.print("  Building HTML...")
    html = _build_html(market_data, macro_data, fundamentals, health, trend_context, opportunities, risk_trend)

    if output_path:
        filepath = Path(output_path)
        filepath.parent.mkdir(parents=True, exist_ok=True)
    else:
        REPORTS_DIR.mkdir(exist_ok=True)
        et = ZoneInfo("America/New_York")
        filename = f"market-report-{datetime.now(et).strftime('%Y-%m-%d-%H%M')}.html"
        filepath = REPORTS_DIR / filename

    filepath.write_text(html)
    log_path = append_risk_score_log(health)
    if log_path:
        console.print(f"[dim]Risk score log: {log_path}[/dim]")
    console.print(f"\n[bold green]Report saved: {filepath}[/bold green]")

    if open_browser:
        try:
            webbrowser.open(f"file://{filepath.resolve()}")
            console.print("[dim]Opened in browser.[/dim]")
        except Exception:
            console.print(f"[dim]Open manually: file://{filepath.resolve()}[/dim]")


def _build_html(
    market_data: dict,
    macro_data: MacroSnapshot | None,
    fundamentals: dict[str, StockFundamentals],
    health: MarketHealthReport,
    trend_context: str,
    opportunities: list[Opportunity] | None = None,
    risk_trend: RiskTrend | None = None,
) -> str:
    et = ZoneInfo("America/New_York")
    now_et = datetime.now(et)
    now = now_et.strftime("%B %d, %Y at %I:%M %p ET")

    market_asof_label, market_asof_dt = _market_prices_as_of_display(market_data)
    title_asof = (
        market_asof_dt.strftime("%Y-%m-%d") if market_asof_dt else "see snapshot"
    )
    fred_block = _fred_observations_html(macro_data)
    if macro_data and macro_data.fred_observations_through:
        fred_footer = (
            "Macro (FRED) observations through: "
            f"{macro_data.fred_observations_through.strftime('%B %d, %Y')} "
            "(cadence varies by series)."
        )
    elif macro_data and macro_data.indicators:
        fred_footer = "Macro (FRED): loaded; observation dates unavailable."
    else:
        fred_footer = "Macro (FRED): not in this snapshot."

    risk_color = {
        "low": "#22c55e", "moderate": "#eab308", "elevated": "#f97316",
        "high": "#ef4444", "critical": "#dc2626",
        "severe": "#991b1b", "extreme": "#7f1d1d", "catastrophic": "#b91c1c",
    }.get(health.overall_risk, "#6b7280")

    conf_color = {"high": "#22c55e", "medium": "#eab308", "low": "#ef4444"}.get(health.confidence, "#6b7280")

    guidance = get_position_guidance(health.overall_risk)

    indices = market_data.get("indices", [])
    commodities = market_data.get("commodities", [])
    vix_data = next((i for i in indices if i.get("ticker") == "^VIX"), None)
    sp500_kpi = next((i for i in indices if i.get("ticker") == "^GSPC"), None)
    oil_kpi = next((i for i in commodities if i.get("ticker") == "BZ=F"), None) if commodities else None

    sections = []
    sections.append(_section_kpi_cards(health, risk_color, sp500_kpi, vix_data, oil_kpi))
    risk_inner = _section_risk_summary(health, risk_color, conf_color, guidance)
    if risk_trend and risk_trend.has_any:
        risk_inner += _section_risk_trend(risk_trend)
    risk_inner += _section_risk_score_reader_context(health)
    sections.append(_collapsible(
        f'Risk Overview — <span style="color:{risk_color}">{health.overall_risk.upper()}</span> (Score: {_health_uncapped_score(health)})',
        risk_inner,
        open_default=False,
        section_id="risk",
    ))
    sections.append(_section_score_attribution(health))
    sections.append(_section_risk_legend(health))
    sections.append(_section_market_table(market_data))
    if macro_data and macro_data.indicators:
        sections.append(_section_macro(macro_data))
    if fundamentals:
        name_lookup = {item["ticker"]: item.get("name", item["ticker"])
                       for cat in market_data.values() if isinstance(cat, list)
                       for item in cat if isinstance(item, dict) and "ticker" in item}
        sections.append(_section_fundamentals(fundamentals, name_lookup))
    if opportunities:
        sections.append(_section_opportunities(opportunities, health))
    sections.append(_section_signals(health))
    sections.append(_section_bond_bank_plain_english(macro_data))
    sp500_data = next((i for i in indices if i.get("ticker") == "^GSPC"), None)
    sp500_price = sp500_data["price"] if sp500_data and sp500_data.get("price") else None
    sections.append(_section_historical_parallels(sp500_price))
    sections.append(_section_supply_chain())
    if trend_context:
        sections.append(_section_trend_context(trend_context))
    sections.append(_section_authoritative_sources())

    body = "\n".join(sections)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Market Report — data as of {escape(title_asof)}</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='6' fill='%231e293b'/><rect x='6' y='18' width='4' height='8' rx='1' fill='%233b82f6'/><rect x='14' y='12' width='4' height='14' rx='1' fill='%233b82f6'/><rect x='22' y='6' width='4' height='20' rx='1' fill='%2322c55e'/></svg>">
<style>
/* \u2500\u2500 Design tokens \u2500\u2500 */
:root {{
  --bg: #0f172a;
  --surface: #1e293b;
  --surface2: #334155;
  --text: #e2e8f0;
  --text-dim: #94a3b8;
  --border: #475569;
  --green: #22c55e;
  --red: #ef4444;
  --yellow: #eab308;
  --orange: #f97316;
  --blue: #3b82f6;
  --cyan: #06b6d4;
  --pad-inline: clamp(0.5rem, -0.25rem + 2.5vw, 2rem);
  --pad-block: clamp(0.65rem, -0.1rem + 2.5vw, 2rem);
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
html {{ -webkit-text-size-adjust: 100%; scroll-behavior: smooth; overscroll-behavior-x: none; }}

/* \u2500\u2500 Base: phone-first (360\u2013430 CSS px) \u2500\u2500 */
.section-anchor {{ scroll-margin-top: 6rem; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
  background: var(--bg); color: var(--text);
  line-height: 1.6;
  padding: var(--pad-block) var(--pad-inline);
  padding-left: max(var(--pad-inline), env(safe-area-inset-left, 0px));
  padding-right: max(var(--pad-inline), env(safe-area-inset-right, 0px));
  padding-bottom: max(var(--pad-block), env(safe-area-inset-bottom, 0px));
  max-width: 1400px; margin: 0 auto;
  overflow-x: hidden;
}}
h1 {{ font-size: clamp(1.15rem, 1rem + 0.75vw, 1.5rem); font-weight: 600; margin-bottom: 0.25rem; }}
h2 {{
  font-size: clamp(0.95rem, 0.9rem + 0.25vw, 1.1rem); font-weight: 600; color: var(--cyan);
  border-bottom: 1px solid var(--border); padding-bottom: 0.5rem;
  margin: 1.5rem 0 0.75rem;
}}
.subtitle {{ color: var(--text-dim); font-size: 0.85rem; margin-bottom: 1.5rem; }}
.subtitle-stack {{ display: flex; flex-direction: column; gap: 0.4rem; margin-bottom: 1.5rem; }}
.subtitle-line {{ color: var(--text-dim); font-size: 0.85rem; line-height: 1.45; }}
.subtitle-detail {{ display: none; }}
.card {{
  background: var(--surface); border-radius: 0.75rem;
  padding: 1rem; margin-bottom: 1rem;
  border: 1px solid var(--border);
}}
.risk-banner {{
  display: flex; flex-direction: column; align-items: flex-start; gap: 1rem;
  flex-wrap: wrap;
  padding: 1rem;
  padding-left: max(1rem, env(safe-area-inset-left, 0px));
  padding-right: max(1rem, env(safe-area-inset-right, 0px));
  border-radius: 0.75rem; margin-bottom: 1.5rem;
  background: var(--surface); border-left: 4px solid {risk_color};
}}
.risk-banner .level {{
  font-size: clamp(1.4rem, 1.2rem + 1vw, 2rem); font-weight: 700; letter-spacing: 0.05em;
}}
.risk-banner .meta {{ color: var(--text-dim); font-size: 0.85rem; }}
.risk-banner .meta span {{
  display: inline-block; margin-right: 0.75rem; margin-bottom: 0.25rem;
}}
.stat {{
  display: inline-block; padding: 0.4rem 0.8rem; border-radius: 0.5rem;
  background: var(--surface2); font-size: 0.8rem; margin: 0.2rem;
}}
table {{
  width: 100%; border-collapse: collapse; font-size: 0.8rem;
  margin-bottom: 0.5rem;
}}
th {{
  text-align: left; padding: 0.45rem 0.5rem; color: var(--text-dim);
  font-weight: 500; font-size: 0.75rem; text-transform: uppercase;
  letter-spacing: 0.05em; border-bottom: 1px solid var(--border);
}}
td {{
  padding: 0.45rem 0.5rem; border-bottom: 1px solid var(--surface2);
  word-break: break-word;
}}
tr:hover {{ background: var(--surface2); }}
.section-label {{
  padding: 0.3rem 0.75rem; background: var(--surface2);
  font-weight: 600; font-size: 0.75rem; text-transform: uppercase;
  letter-spacing: 0.08em; color: var(--cyan);
}}
.pos {{ color: var(--green); }}
.neg {{ color: var(--red); }}
.warn {{ color: var(--yellow); }}
.crit {{ color: var(--red); font-weight: 600; }}
.info {{ color: var(--text-dim); }}
.neutral {{ color: var(--text-dim); }}
.tag {{
  display: inline-block; padding: 0.15rem 0.5rem; border-radius: 0.25rem;
  font-size: 0.7rem; font-weight: 600; text-transform: uppercase; white-space: nowrap;
}}
.tag-critical {{ background: #dc2626; color: #ffffff; border: 1px solid #f87171; box-shadow: 0 0 0 1px rgba(220,38,38,0.35); }}
.tag-warning {{ background: #d97706; color: #fffbeb; border: 1px solid #fbbf24; }}
.tag-info {{ background: #059669; color: #ecfdf5; border: 1px solid #34d399; }}
.tag-leading {{ background: #064e3b; color: #6ee7b7; }}
.tag-lagging {{ background: var(--surface2); color: var(--text-dim); }}
.tag-strong {{ background: #064e3b; color: #6ee7b7; }}
.tag-moderate {{ background: #d97706; color: #fffbeb; border: 1px solid #fbbf24; }}
.tag-weak {{ background: #dc2626; color: #ffffff; border: 1px solid #f87171; }}
.tag-distressed {{ background: #b91c1c; color: #ffffff; font-weight: 700; border: 1px solid #f87171; }}
.tag-unknown {{ background: var(--surface2); color: var(--text-dim); }}
.footer {{
  margin-top: 2rem; padding-top: 1rem; border-top: 1px solid var(--border);
  color: var(--text-dim); font-size: 0.72rem; line-height: 1.65;
}}
.two-col {{ display: grid; grid-template-columns: 1fr; gap: 1rem; }}
pre {{
  background: var(--surface2); padding: 1rem; border-radius: 0.5rem;
  font-size: 0.8rem; overflow-x: auto; white-space: pre-wrap;
  color: var(--text-dim);
}}
.section-collapse {{
  margin-bottom: 1rem; border: 1px solid var(--border);
  border-radius: 0.75rem; overflow: clip;
}}
.section-header {{
  cursor: pointer; padding: 0.85rem 0.75rem;
  min-height: max(2.75rem, 44px);
  font-size: clamp(0.9rem, 0.85rem + 0.2vw, 1.05rem); font-weight: 600; color: var(--cyan);
  background: var(--surface); list-style: none;
  display: flex; align-items: center; gap: 0.5rem;
  line-height: 1.35;
}}
.section-header::-webkit-details-marker {{ display: none; }}
.section-header::before {{
  content: "\u25b6"; font-size: 0.7rem; transition: transform 0.2s;
  color: var(--text-dim);
}}
details[open] > .section-header::before {{ transform: rotate(90deg); }}
.section-body {{ padding: 0 0.35rem 0.65rem; }}
.section-body .card {{ margin-bottom: 0.75rem; }}
.section-body .table-scroll.card {{ padding: 0.5rem 0.35rem; }}
.section-body h2 {{ display: none; }}
.section-body h3 {{ margin-top: 1rem; }}
.bond-bank-intro {{ margin-bottom: 1rem; }}
.bond-bank-intro-list {{
  margin: 0; padding-left: 1.25rem;
  color: var(--text-dim); font-size: 0.88rem; line-height: 1.55;
}}
.bond-bank-intro-list li {{ margin-bottom: 0.45rem; }}
.bond-bank-intro-list strong {{ color: var(--text); }}
.bond-bank-scan {{ display: flex; flex-direction: column; gap: 0.5rem; }}
.bond-bank-item {{
  border: 1px solid var(--border); border-radius: 0.5rem;
  background: var(--surface2); border-left: 3px solid var(--cyan);
  overflow: hidden;
}}
.bond-bank-summary {{
  cursor: pointer; list-style: none;
  min-height: max(2.65rem, 44px);
  padding: 0.65rem 0.75rem;
  font-size: 0.86rem; line-height: 1.45;
  display: flex; flex-wrap: wrap; align-items: center;
  gap: 0.4rem 0.5rem;
}}
.bond-bank-summary::-webkit-details-marker {{ display: none; }}
.bond-bank-summary::before {{
  content: "\u25b6"; font-size: 0.65rem;
  color: var(--text-dim); flex-shrink: 0;
  transition: transform 0.15s; margin-right: 0.15rem;
}}
details[open] > .bond-bank-summary::before {{ transform: rotate(90deg); }}
.bond-bank-item-body {{
  padding: 0 0.75rem 0.75rem 0.75rem;
  border-top: 1px solid var(--border);
  font-size: 0.86rem; color: var(--text-dim);
}}
.bond-bank-item-body p {{ margin: 0.55rem 0 0 0; }}
.bond-bank-item-body p:first-child {{ margin-top: 0.45rem; }}
.mobile-rotate-hint {{
  display: block;
  font-size: 0.78rem; color: var(--text-dim); line-height: 1.45;
  padding: 0.35rem 0; border-left: 3px solid var(--cyan);
  padding-left: 0.6rem; margin-top: 0.25rem;
}}
.table-scroll {{
  overflow-x: auto; -webkit-overflow-scrolling: touch;
  overscroll-behavior-x: contain;
  width: 100%; max-width: 100%; margin-bottom: 0.5rem;
}}
.table-scroll.table-edge-hint {{
  box-shadow: inset -12px 0 14px -12px rgba(0, 0, 0, 0.55);
}}
.sticky-first-col table th:first-child,
.sticky-first-col table td:first-child {{
  position: sticky; left: 0; z-index: 2;
  background: var(--surface);
  box-shadow: 4px 0 10px -4px rgba(0, 0, 0, 0.5);
}}
.sticky-first-col table thead th:first-child {{ z-index: 4; background: var(--surface); }}
.sticky-first-col tbody tr:hover td:first-child {{ background: var(--surface2); }}
.table-scroll.wide-min > table {{
  min-width: 20rem; width: max-content; max-width: none;
}}
.table-scroll:not(.wide-min) > table {{
  min-width: 0; width: 100%; max-width: 100%;
}}
.col-m-hide {{ display: none !important; }}
.kpi-row {{
  display: grid; grid-template-columns: 1fr 1fr; gap: 0.75rem;
  margin-bottom: 1.5rem;
}}
.kpi-row > div {{
  min-width: 0;
}}
.opp-signal-grid {{
  display: grid; grid-template-columns: 1fr;
  gap: 1rem; margin-bottom: 0.75rem;
}}
.nav-bar {{
  position: sticky; top: 0; z-index: 100;
  background: rgba(15, 23, 42, 0.92); backdrop-filter: blur(8px);
  border-bottom: 1px solid var(--border);
  padding: 0.4rem 0.5rem;
  margin: 0 calc(var(--pad-inline) * -1) 0.75rem calc(var(--pad-inline) * -1);
  display: flex; flex-wrap: nowrap; gap: 0.15rem 0.3rem;
  overflow-x: auto; -webkit-overflow-scrolling: touch;
  scrollbar-width: none;
  padding-left: max(0.5rem, env(safe-area-inset-left, 0px));
  padding-right: max(0.5rem, env(safe-area-inset-right, 0px));
}}
.nav-bar::-webkit-scrollbar {{ display: none; }}
.nav-bar a {{
  color: var(--cyan); text-decoration: none; font-size: 0.7rem;
  font-weight: 600; padding: 0.4rem 0.55rem; border-radius: 0.35rem;
  text-transform: uppercase; letter-spacing: 0.04em;
  white-space: nowrap; transition: background 0.15s;
  min-height: 44px; display: inline-flex; align-items: center;
}}
.nav-bar a:hover {{ background: var(--surface); }}

/* \u2500\u2500 Foldable / mini-tablet (\u2265 600px) \u2500\u2500 */
@media (min-width: 600px) {{
  .table-scroll.wide-min > table {{ min-width: 22rem; font-size: 0.8rem; }}
  .section-body {{ padding: 0 0.5rem 0.75rem; }}
  .section-body .table-scroll.card {{ padding: 0.75rem 0.5rem; }}
}}

/* \u2500\u2500 Tablet (\u2265 768px) \u2500\u2500 */
@media (min-width: 768px) {{
  .section-anchor {{ scroll-margin-top: 4.5rem; }}
  h2 {{ margin: 2rem 0 1rem; }}
  .card {{ padding: 1.25rem; }}
  .risk-banner {{
    flex-direction: row; align-items: center; gap: 2rem;
    padding: 1.5rem;
  }}
  .risk-banner .meta span {{ margin-right: 1.5rem; margin-bottom: 0; }}
  table {{ font-size: 0.85rem; }}
  th {{ padding: 0.6rem 0.75rem; }}
  td {{ padding: 0.5rem 0.75rem; word-break: normal; }}
  .footer {{ font-size: 0.75rem; line-height: 1.8; }}
  .two-col {{ grid-template-columns: 1fr 1fr; }}
  .section-header {{ padding: 0.75rem 1rem; min-height: auto; }}
  .section-body {{ padding: 0 1rem 1rem; }}
  .section-body .table-scroll.card {{ padding: 1.25rem; }}
  .mobile-rotate-hint {{ display: none; }}
  .subtitle-detail {{ display: block; }}
  .col-m-hide {{ display: table-cell !important; }}
  .table-scroll.wide-min > table {{ min-width: 30rem; }}
  .opp-signal-grid {{ grid-template-columns: 1fr 1fr; }}
  .kpi-row {{ grid-template-columns: repeat(4, 1fr); gap: 1rem; }}
  .nav-bar {{
    flex-wrap: wrap; overflow-x: visible; justify-content: center;
    padding: 0.5rem 0; gap: 0.25rem 0.5rem;
    padding-left: max(1rem, env(safe-area-inset-left, 0px));
    padding-right: max(1rem, env(safe-area-inset-right, 0px));
  }}
  .nav-bar a {{ font-size: 0.75rem; padding: 0.3rem 0.6rem; min-height: auto; }}
  .bond-bank-summary {{ min-height: auto; align-items: baseline; }}
}}

/* \u2500\u2500 Desktop (\u2265 1024px) \u2500\u2500 */
@media (min-width: 1024px) {{
  .table-scroll.wide-min > table {{ min-width: 36rem; }}
  .table-scroll.table-edge-hint {{ box-shadow: none; }}
}}
</style>
</head>
<body>
<h1>Financial Agent — Market Report</h1>
<div class="subtitle-stack">
<div class="subtitle-line">Market prices & technicals as of: <strong>{escape(market_asof_label)}</strong></div>
{fred_block}
<div class="subtitle-line">Snapshot generated: <strong>{escape(now)}</strong> (HTML build / CI time — not the same as market close).</div>
<div class="subtitle-line subtitle-detail">Data layers: Technical + {"Macro (FRED) + " if macro_data and macro_data.indicators else ""}{"Fundamentals" if fundamentals else "Technical only"}.</div>
<div class="subtitle-line subtitle-detail" id="viewer-opened"></div>
<div class="subtitle-line mobile-rotate-hint" aria-hidden="true">
<strong style="color:var(--cyan);">Tip:</strong> Rotating to landscape gives wider tables and less side-to-side scrolling — optional; the report is usable in portrait too.
</div>
</div>

<div class="nav-bar">
  <a href="#risk">Risk</a>
  <a href="#markets">Markets</a>
  <a href="#macro">Macro</a>
  <a href="#fundamentals">Fundamentals</a>
  <a href="#opportunities">Opportunities</a>
  <a href="#signals">Signals</a>
  <a href="#bonds-banks">Bonds &amp; Banks</a>
  <a href="#historical">Historical</a>
  <a href="#supply-chain">Supply Chain</a>
</div>

{body}

<div class="footer">
<strong>Freshness:</strong> Market prices & technicals as of: {escape(market_asof_label)}.
{escape(fred_footer)} Snapshot generated: {escape(now)}.
Static Pages only update when CI runs — between runs, numbers are stale even if this page says “today” on your device.<br>
<strong>Limitations:</strong> Cannot predict black swan events (pandemics, wars, regulatory shocks).
Correlations may break down in crises — "diversified" assets can fall together.
Free data sources (yfinance) may have delays or accuracy issues.<br>
<strong>Data sources:</strong> yfinance (unofficial){", FRED API (official U.S. macro series)" if macro_data else ""}.
Macro signals and narratives are rule-based heuristics on those series — not investment advice, not bank safety ratings (not CAMELS), not forecasts.<br>
This report is for educational / family context. <a href="#authoritative-sources" style="color:var(--cyan);">Authoritative data sources</a><br>
Generated by Financial Agent v0.2
</div>

{_section_definitions()}

<script>
(function () {{
  var el = document.getElementById("viewer-opened");
  if (!el) return;
  var d = new Date();
  el.textContent =
    "You opened this page: " +
    d.toLocaleString(undefined, {{ dateStyle: "long", timeStyle: "short" }}) +
    " (your device clock — not a market quote time).";
}})();
(function () {{
  function openAndScroll(id) {{
    var target = document.getElementById(id);
    if (!target) return;
    var allSections = document.querySelectorAll("details.section-collapse");
    for (var i = 0; i < allSections.length; i++) {{
      allSections[i].open = false;
    }}
    if (target.tagName === "DETAILS") {{
      target.open = true;
    }}
    requestAnimationFrame(function () {{
      requestAnimationFrame(function () {{
        target.scrollIntoView({{ behavior: "smooth", block: "start" }});
      }});
    }});
  }}
  var links = document.querySelectorAll(".nav-bar a[href^='#']");
  for (var i = 0; i < links.length; i++) {{
    links[i].addEventListener("click", function (e) {{
      e.preventDefault();
      var id = this.getAttribute("href").slice(1);
      history.pushState(null, "", "#" + id);
      openAndScroll(id);
    }});
  }}
  window.addEventListener("popstate", function () {{
    var hash = window.location.hash;
    if (hash && hash.length > 1) openAndScroll(decodeURIComponent(hash.slice(1)));
  }});
  var initHash = window.location.hash;
  if (initHash && initHash.length > 1) {{
    openAndScroll(decodeURIComponent(initHash.slice(1)));
  }}
}})();
</script>
</body>
</html>"""


def _health_uncapped_score(health: MarketHealthReport) -> int:
    """Raw score sum before 100 cap; mirrors `risk.MarketHealthReport.score_uncapped` when present."""
    return int(getattr(health, "score_uncapped", health.score))


def _health_score_contributions(health: MarketHealthReport) -> list:
    """Score breakdown rows; empty if public deploy still uses an older `risk.py` without this field."""
    contribs = getattr(health, "score_contributions", None)
    return list(contribs) if contribs else []


_SEVERITY_TITLES = {
    "critical": "Highest severity — strongest risk signal",
    "warning": "Elevated severity — watch closely",
    "info": "Lower severity — milder or contextual signal",
}


def _severity_tag_html(severity: str, display: str | None = None) -> str:
    """Severity badge: color encodes level (red / amber / green); optional tooltip."""
    sev = (severity or "").lower().strip()
    sev_class = {"critical": "tag-critical", "warning": "tag-warning", "info": "tag-info"}.get(
        sev, "tag-unknown"
    )
    label = escape(display if display is not None else (severity or "—"))
    title = _SEVERITY_TITLES.get(sev, "")
    title_attr = f' title="{escape(title, quote=True)}"' if title else ""
    return f"<span class='tag {sev_class}'{title_attr}>{label}</span>"


def _severity_legend_html() -> str:
    """One-line traffic-light key so hue matches level without reading every badge."""
    return """<div role="group" aria-label="Severity color key" style="display:flex;flex-wrap:wrap;align-items:center;gap:0.35rem 0.65rem;font-size:0.75rem;color:var(--text-dim);margin:0 0 0.75rem 0;line-height:1.5;">
<span style="font-weight:600;color:var(--text);">Severity key —</span>
<span class="tag tag-critical">critical</span><span>highest</span>
<span aria-hidden="true" style="opacity:0.45;">·</span>
<span class="tag tag-warning">warning</span><span>elevated</span>
<span aria-hidden="true" style="opacity:0.45;">·</span>
<span class="tag tag-info">info</span><span>lower</span>
</div>"""


def _attribution_lead_lag_span(signal_type: str) -> str:
    st = (signal_type or "").lower().strip()
    type_class = "tag-leading" if st == "leading" else "tag-lagging"
    return f"<span class='tag {type_class}'>{escape(signal_type or '—')}</span>"


def _attribution_points_style(points: int) -> str:
    """Stronger color for larger point contributions."""
    if points >= 20:
        return "color:var(--red);font-weight:700;"
    if points >= 12:
        return "color:var(--yellow);font-weight:600;"
    if points >= 6:
        return "color:var(--orange);"
    return "color:var(--text);font-weight:600;"


def _section_score_attribution(health: MarketHealthReport) -> str:
    """Top contributors to capped score; shows compression when raw sum &gt; 100."""
    rows = []
    for c in _health_score_contributions(health)[:15]:
        ticker_display = escape(c.ticker) if c.ticker and c.ticker != "—" else ""
        pts_style = _attribution_points_style(c.points)
        rows.append(
            "<tr>"
            f"<td style=\"text-align:right;{pts_style}\">{c.points}</td>"
            f"<td>{escape(c.name)}</td>"
            f"<td>{ticker_display}</td>"
            f"<td class='col-m-hide'>{escape(c.category)}</td>"
            f"<td>{_severity_tag_html(c.severity)}</td>"
            f"<td class='col-m-hide'>{_attribution_lead_lag_span(c.signal_type)}</td>"
            "</tr>"
        )
    compression = ""
    uncapped = _health_uncapped_score(health)
    if uncapped > health.score:
        compression = (
            f"<p style=\"font-size:0.8rem;color:var(--text-dim);\">"
            f"Raw sum before cap: <strong>{uncapped}</strong> → displayed score "
            f"<strong>{health.score}</strong> (max 100).</p>"
        )
    if not rows:
        return ""
    return f"""
<details style="margin-bottom:1.25rem;">
<summary style="cursor:pointer;color:var(--cyan);font-size:0.9rem;font-weight:600;">
  Score attribution (top contributors)
</summary>
<div class="card" style="margin-top:0.5rem;">
{_severity_legend_html()}
{compression}
<div class="table-scroll wide-min sticky-first-col table-edge-hint">
<table style="width:100%;font-size:0.8rem;">
<thead><tr>
<th style="text-align:right;padding-right:0.5rem;width:3.5rem;">Pts</th><th>Signal</th><th style="width:4rem;">Ticker</th><th class="col-m-hide">Cat</th><th style="width:4.5rem;">Sev</th><th class='col-m-hide'>Lead/Lag</th>
</tr></thead>
<tbody>{"".join(rows)}</tbody>
</table>
</div>
<p style="font-size:0.75rem;color:var(--text-dim);margin-top:0.5rem;">
Death cross, EPS / distress / insider impacts use <strong>breadth</strong> signals (scaled sublinearly),
not one row per watchlist ticker — see risk legend.
</p>
</div>
</details>
"""


def _section_kpi_cards(
    health: MarketHealthReport,
    risk_color: str,
    sp500: dict | None,
    vix: dict | None,
    oil: dict | None,
) -> str:
    """Row of 4 KPI summary cards — the executive snapshot before any detail."""
    uncapped = _health_uncapped_score(health)
    score_display = str(uncapped) if uncapped > 100 else str(health.score)

    def _kpi(label: str, value: str, color: str, sub: str = "") -> str:
        return (
            f'<div style="background:var(--surface);border-radius:0.75rem;'
            f'padding:0.75rem 1rem;border-left:4px solid {color};">'
            f'<div style="font-size:0.75rem;color:var(--text-dim);text-transform:uppercase;'
            f'letter-spacing:0.05em;margin-bottom:0.3rem;">{label}</div>'
            f'<div style="font-size:clamp(1rem, 4vw, 1.8rem);font-weight:700;color:{color};line-height:1.1;overflow-wrap:break-word;">{value}</div>'
            f'{"<div style=\"font-size:0.8rem;color:var(--text-dim);margin-top:0.2rem;\">" + sub + "</div>" if sub else ""}'
            f'</div>'
        )

    cards = [_kpi("Risk Level", health.overall_risk.upper(), risk_color, f"Score: {score_display}")]

    if sp500 and sp500.get("price"):
        chg = sp500.get("change_1d", 0) or 0
        arrow = "&#9650;" if chg >= 0 else "&#9660;"
        chg_color = "var(--green)" if chg >= 0 else "var(--red)"
        cards.append(_kpi(
            "S&amp;P 500",
            f"{sp500['price']:,.0f}",
            chg_color,
            f"{arrow} {chg:+.2f}%",
        ))

    if vix and vix.get("price"):
        vix_val = vix["price"]
        vix_color = "#22c55e" if vix_val < 20 else "#eab308" if vix_val < 30 else "#f97316" if vix_val < 40 else "#ef4444"
        cards.append(_kpi("VIX", f"{vix_val:.1f}", vix_color, "Fear Index"))

    if oil and oil.get("price"):
        oil_color = "#22c55e" if oil["price"] < 80 else "#eab308" if oil["price"] < 100 else "#f97316" if oil["price"] < 130 else "#ef4444"
        cards.append(_kpi("Brent Crude", f"${oil['price']:.2f}", oil_color, "per barrel"))

    return (
        '<div class="kpi-row">'
        + "".join(cards)
        + '</div>'
    )


def _risk_gauge_html(uncapped: int) -> str:
    """CSS-only gauge bar showing where the risk score sits on the extended scale."""
    display_max = max(uncapped, 200)
    pct = min(uncapped / display_max * 100, 100)
    segments = [
        (20 / display_max * 100, "#22c55e", "LOW"),
        (20 / display_max * 100, "#eab308", "MOD"),
        (20 / display_max * 100, "#f97316", "ELEV"),
        (20 / display_max * 100, "#ef4444", "HIGH"),
        (20 / display_max * 100, "#dc2626", "CRIT"),
    ]
    if display_max > 100:
        segments.append((50 / display_max * 100, "#991b1b", "SEVERE"))
    if display_max > 150:
        segments.append((50 / display_max * 100, "#7f1d1d", "EXTREME"))
    if display_max > 200:
        segments.append((100 / display_max * 100, "#b91c1c", "CATAST"))

    gradient_parts = []
    pos = 0
    for width, color, _ in segments:
        gradient_parts.append(f"{color} {pos:.1f}%, {color} {pos + width:.1f}%")
        pos += width

    gradient = ", ".join(gradient_parts)
    return f"""<div style="position:relative;height:24px;border-radius:12px;overflow:hidden;
      background:linear-gradient(to right, {gradient});margin:0.5rem 0;">
  <div style="position:absolute;left:{pct:.1f}%;top:0;bottom:0;width:3px;background:white;
    box-shadow:0 0 6px rgba(255,255,255,0.8);transform:translateX(-50%);"></div>
  <div style="position:absolute;left:{pct:.1f}%;top:-2px;transform:translateX(-50%);
    font-size:0.75rem;font-weight:700;color:white;text-shadow:0 0 4px #000;">▼</div>
</div>
<div style="display:flex;justify-content:space-between;font-size:0.75rem;color:var(--text-dim);padding:0 2px;">
  <span>0</span><span>20</span><span>40</span><span>60</span><span>80</span><span>100</span>
  {"<span>150</span>" if display_max > 100 else ""}{"<span>200+</span>" if display_max > 150 else ""}
</div>"""


def _section_risk_summary(health: MarketHealthReport, risk_color: str, conf_color: str, guidance: dict) -> str:
    uncapped = _health_uncapped_score(health)
    score_display = f"{uncapped}" if uncapped > 100 else f"{health.score}"
    score_label = f"Score (uncapped)" if uncapped > 100 else "Score / 100"
    gauge = _risk_gauge_html(uncapped)
    return f"""
<div class="risk-banner">
  <div>
    <div class="level" style="color: {risk_color}">{health.overall_risk.upper()}</div>
    <div style="color: var(--text-dim); font-size: 0.8rem;">Risk Level</div>
  </div>
  <div>
    <div class="level">{score_display}</div>
    <div style="color: var(--text-dim); font-size: 0.8rem;">{score_label}</div>
  </div>
  <div>
    <div class="level" style="color: {conf_color}">{health.confidence.upper()}</div>
    <div style="color: var(--text-dim); font-size: 0.8rem;">Data completeness</div>
  </div>
  <div style="flex-grow: 1;">
    {gauge}
    <div class="meta" style="margin-top: 0.4rem;">
      <span>Critical: {health.critical_count}</span>
      <span>Warnings: {health.warning_count}</span>
      <span>Leading: {health.leading_signal_count}</span>
    </div>
    <div class="meta" style="margin-top: 0.3rem;">
      Position size: <strong>{guidance['max_position']}</strong> &nbsp;|&nbsp; Stop-loss: <strong>{guidance['stop_loss']}</strong>
    </div>
    <div class="meta" style="margin-top: 0.25rem; color: var(--text-dim); font-size: 0.78rem; line-height: 1.5;">
      {guidance.get('explanation', '')}
    </div>
    {"<div class='meta' style='margin-top:0.3rem;color:var(--yellow)'>Data gaps: " + ", ".join(health.data_sources_missing) + "</div>" if health.data_sources_missing else ""}
  </div>
</div>"""


def _section_risk_trend(trend: RiskTrend) -> str:
    """Render risk score trend indicators (daily / weekly / monthly deltas)."""

    def _chip(label: str, delta: int, prev_level: str | None) -> str:
        if delta > 0:
            arrow, color, word = "&#9650;", "var(--red)", "up"
        elif delta < 0:
            arrow, color, word = "&#9660;", "var(--green)", "down"
        else:
            arrow, color, word = "&#9644;", "var(--text-dim)", "flat"
        level_note = f" ({prev_level.upper()})" if prev_level else ""
        return (
            f'<div style="display:inline-flex;align-items:center;gap:0.35rem;'
            f'background:var(--surface);border:1px solid var(--border);'
            f'border-radius:0.5rem;padding:0.35rem 0.65rem;font-size:0.82rem;">'
            f'<span style="font-size:0.7rem;color:{color};">{arrow}</span>'
            f'<span style="color:var(--text-dim);">{label}:</span>'
            f'<span style="color:{color};font-weight:600;">{delta:+d}</span>'
            f'<span style="color:var(--text-dim);font-size:0.72rem;">{level_note}</span>'
            f'</div>'
        )

    chips = []
    if trend.delta_1d is not None:
        chips.append(_chip("1d", trend.delta_1d, trend.prev_1d_level))
    if trend.delta_1w is not None:
        chips.append(_chip("1w", trend.delta_1w, trend.prev_1w_level))
    if trend.delta_1m is not None:
        chips.append(_chip("1m", trend.delta_1m, trend.prev_1m_level))

    if not chips:
        return ""

    return (
        '<div style="display:flex;flex-wrap:wrap;gap:0.5rem;margin:0.75rem 0 0.5rem;">'
        '<span style="font-size:0.78rem;color:var(--text-dim);align-self:center;">'
        'Score trend (uncapped):</span>'
        + "".join(chips)
        + '</div>'
    )


def _section_risk_score_reader_context(health: MarketHealthReport) -> str:
    """Plain-language explanation of uncapped score, labels, and what this site stores."""
    uncapped = _health_uncapped_score(health)
    capped = health.score
    return f"""
<div id="risk-explainer" class="card" style="border-left:3px solid var(--cyan);margin-bottom:1.25rem;">
<h3 style="margin:0 0 0.5rem 0;font-size:0.95rem;color:var(--text);">How this risk score works (read this first)</h3>
<div style="font-size:0.82rem;color:var(--text-dim);line-height:1.6;">
<p style="margin:0 0 0.65rem 0;">
<strong style="color:var(--text);">1. How we calculate it</strong> —
Each detected signal adds <strong>points</strong> (amount varies by rule and severity; <strong>leading</strong> macro/fundamental signals are weighted <strong>1.5×</strong> vs <strong>lagging</strong> technical signals).
We add them across VIX, drawdowns, death crosses, macro (FRED), fundamentals, and breadth.
Your <strong>raw total</strong> is <strong>{uncapped}</strong>; we also show a <strong>0–100 capped</strong> score ({capped}) for comparison.
The <strong>named level</strong> (Moderate through Catastrophic) uses the <strong>raw</strong> total so conditions can still look worse once many signals stack, even though the capped number stops at 100.
</p>
<p style="margin:0 0 0.65rem 0;">
<strong style="color:var(--text);">2. What “Catastrophic” means here</strong> —
It only means the raw sum is <strong>≥ 200</strong> on our rule set.
That usually means <strong>many warnings fired at once</strong>, not that a specific bad outcome will happen.
It is a <strong>model severity label</strong>, not a forecast, not a bank rating, and not personalized advice.
</p>
<p style="margin:0;">
<strong style="color:var(--text);">3. Snapshots and history</strong> —
Each report run saves <strong>one set of numbers for “now.”</strong>
If you run the tool locally, a SQLite database can accumulate past tick snapshots over time.
<strong>This public page</strong> is rebuilt on a <strong>schedule</strong> (weekdays, plus when the repo updates); it does <strong>not</strong> keep a growing score history on the server unless you add that separately.
</p>
</div>
</div>
"""


def _section_risk_legend(health: MarketHealthReport) -> str:
    active_level = health.overall_risk

    levels = [
        ("CATASTROPHIC", "raw sum 200+", "#b91c1c",
         "The <strong>uncapped</strong> point total reached 200 or more — many rule-based signals fired together "
         "(technical + macro + fundamentals). This label measures <strong>stacked stress in this model</strong>; "
         "it is <strong>not</strong> a prediction of collapse and <strong>not</strong> financial advice."),
        ("EXTREME", "150–199", "#7f1d1d",
         "Broad systemic failure signals across financial, commodity, and macro dimensions. "
         "Maximum defensive posture: cash, short-duration treasuries, zero equity exposure."),
        ("SEVERE", "100–149", "#991b1b",
         "Multiple compounding crises active. The score exceeds 100 because more risk signals "
         "are firing than the baseline scale anticipated. Beyond critical — conditions are still deteriorating."),
        ("CRITICAL", "80–99", "#dc2626",
         "Multiple severe risk signals firing simultaneously. Defensive posture — "
         "avoid new positions, prioritize capital preservation, increase cash allocation."),
        ("HIGH", "60–79", "#ef4444",
         "Significant risk signals across multiple categories. Reduce exposure to risk assets, "
         "tighten stop-losses, and size any new positions very small (0.5–1% max)."),
        ("ELEVATED", "40–59", "#f97316",
         "Several warning signals are active. Proceed with caution, favor quality over speculation, "
         "and keep position sizes moderate (1–3%)."),
        ("MODERATE", "20–39", "#eab308",
         "A few risk signals are present but the market is broadly functional. "
         "Normal investing with standard position sizing (3–5%)."),
        ("LOW", "0–19", "#22c55e",
         "Minimal risk signals detected. Market conditions are calm. Standard investing with full position sizing."),
    ]

    level_rows = []
    for name, score_range, color, description in levels:
        is_active = name.lower() == active_level
        highlight = "border: 1px solid " + color + "; background: #1e293b;" if is_active else ""
        arrow = " ◀ CURRENT" if is_active else ""
        level_rows.append(
            f'<div style="display:flex;flex-wrap:wrap;gap:0.5rem 1rem;align-items:flex-start;padding:0.75rem;border-radius:0.5rem;{highlight}">'
            f'<div style="min-width:5.5rem;text-align:center;">'
            f'<span style="color:{color};font-weight:700;font-size:0.85rem;">{name}</span>'
            f'<div style="color:var(--text-dim);font-size:0.75rem;">{score_range}</div>'
            f'</div>'
            f'<div style="font-size:0.8rem;color:var(--text-dim);line-height:1.5;">{description}'
            f'{"<span style=\"color:" + color + ";font-weight:600\">" + arrow + "</span>" if is_active else ""}'
            f'</div></div>'
        )

    conf_levels = [
        ("HIGH", "#22c55e", "3+ data layers present (technical + macro + fundamental) AND 2+ leading signals counted. "
         "Means <strong>inputs are rich</strong> — not a guarantee the risk <em>label</em> is correct."),
        ("MEDIUM", "#eab308", "2 data layers OR at least 1 leading signal — usable but incomplete coverage."),
        ("LOW", "#ef4444", "Technical-only or very thin leading coverage — score is more fragile."),
    ]

    conf_rows = []
    active_conf = health.confidence
    for name, color, description in conf_levels:
        is_active = name.lower() == active_conf
        highlight = "border: 1px solid " + color + "; background: #1e293b;" if is_active else ""
        arrow = " ◀ CURRENT" if is_active else ""
        conf_rows.append(
            f'<div style="display:flex;flex-wrap:wrap;gap:0.5rem 1rem;align-items:flex-start;padding:0.6rem;border-radius:0.5rem;{highlight}">'
            f'<span style="color:{color};font-weight:700;font-size:0.8rem;min-width:4.5rem;text-align:center;">{name}</span>'
            f'<div style="font-size:0.8rem;color:var(--text-dim);line-height:1.5;">{description}'
            f'{"<span style=\"color:" + color + ";font-weight:600\">" + arrow + "</span>" if is_active else ""}'
            f'</div></div>'
        )

    return f"""
<details style="margin-bottom:1.5rem;">
<summary style="cursor:pointer;color:var(--cyan);font-size:0.85rem;font-weight:600;padding:0.5rem 0;">
Understanding Risk Levels &amp; How They Are Calculated
</summary>
<div class="card" style="margin-top:0.5rem;">

<div style="margin-bottom:1.25rem;">
<div style="font-weight:600;font-size:0.85rem;margin-bottom:0.75rem;color:var(--text);">Risk Levels</div>
<div style="display:flex;flex-direction:column;gap:0.4rem;">
{"".join(level_rows)}
</div>
</div>

<div style="border-top:1px solid var(--border);padding-top:1rem;margin-bottom:1.25rem;">
<div style="font-weight:600;font-size:0.85rem;margin-bottom:0.5rem;color:var(--text);">How the Score Is Calculated</div>
<div style="font-size:0.8rem;color:var(--text-dim);line-height:1.7;">
The risk score is a number from <strong style="color:var(--text);">0 to 100</strong> (hard-capped — it can never exceed 100).
Every detected risk signal adds points based on its severity:<br>
</div>

<div class="table-scroll wide-min sticky-first-col table-edge-hint">
<table style="width:auto;margin:0.75rem 0 0.75rem 0.5rem;font-size:0.8rem;">
<thead><tr>
<th style="text-align:left;padding-right:1.5rem;">Signal Severity</th>
<th style="text-align:right;padding-right:1.5rem;">Lagging</th>
<th style="text-align:right;">Leading (1.5×)</th>
</tr></thead>
<tbody>
<tr><td><span style="color:var(--red);font-weight:600;">Critical</span></td>
<td style="text-align:right;padding-right:1.5rem;">+25 pts</td>
<td style="text-align:right;font-weight:600;">+37 pts</td></tr>
<tr><td><span style="color:var(--yellow);font-weight:600;">Warning</span></td>
<td style="text-align:right;padding-right:1.5rem;">+10 pts</td>
<td style="text-align:right;font-weight:600;">+15 pts</td></tr>
<tr><td><span style="color:var(--text-dim);">Info</span></td>
<td style="text-align:right;padding-right:1.5rem;">+2 pts</td>
<td style="text-align:right;">+2 pts</td></tr>
</tbody>
</table>
</div>

<div style="font-size:0.8rem;color:var(--text-dim);line-height:1.7;">
All signals add up, then the total is capped at 100.<br><br>
<strong style="color:var(--text);">Why leading indicators count more:</strong>
<strong>Leading indicators</strong> (macro data, earnings revisions, insider activity) <em>predict</em> future conditions — they get 1.5× weight.
<strong>Lagging indicators</strong> (RSI, moving averages, price drops) <em>confirm</em> what already happened — important but less predictive.<br><br>
<strong style="color:var(--text);">Three data layers are checked:</strong><br>
<span style="margin-left:1rem;">1. <strong>Technical</strong> — price, volume, momentum, moving averages (lagging)</span><br>
<span style="margin-left:1rem;">2. <strong>Macro</strong> — FRED economic data: yield curve, credit spreads, unemployment, consumer confidence (leading)</span><br>
<span style="margin-left:1rem;">3. <strong>Fundamental</strong> — earnings revisions, insider activity, analyst targets, financial health (leading)</span><br><br>
<strong style="color:var(--text);">Breadth and watchlists:</strong> Technical signals still scale with how many tickers fire
(e.g. many RSI warnings). <strong>Fundamental</strong> impacts use <strong>one breadth row</strong> each for EPS deterioration,
insider selling, and distress — scaled sublinearly so a large watchlist does not automatically max the score.<br><br>
<strong style="color:var(--text);">Example:</strong> A score of 85 (CRITICAL) usually means several severe signals stacked before the 100 cap
—not a single soft datapoint.
A single leading critical macro signal alone is about 25–37 points (often <strong>MODERATE</strong> bucket).
</div>
</div>

<div style="border-top:1px solid var(--border);padding-top:1rem;">
<div style="font-weight:600;font-size:0.85rem;margin-bottom:0.75rem;color:var(--text);">Data completeness (not narrative confidence)</div>
<div style="display:flex;flex-direction:column;gap:0.4rem;">
{"".join(conf_rows)}
</div>
</div>

</div>
</details>"""


def _market_category_table_rows(items: list) -> list[str]:
    out = []
    for item in items:
        ticker = escape(item["ticker"])
        name = escape(item.get("name", item["ticker"]))
        price = f"${item['price']:,.2f}" if item.get("price") else "—"
        d1 = _pct_cell(item.get("change_pct_1d"))
        w1 = _pct_cell(item.get("change_pct_1w"))
        m1 = _pct_cell(item.get("change_pct_1m"))
        rsi = f"{item['rsi_14']:.0f}" if item.get("rsi_14") else "—"
        signal = _signal_badges(item)
        out.append(
            f"<tr><td><strong>{ticker}</strong></td>"
            f"<td class='col-m-hide' style='color:var(--text-dim);font-size:0.8rem;'>{name}</td>"
            f"<td style='text-align:right'>{price}</td>"
            f"<td style='text-align:right'>{d1}</td>"
            f"<td class='col-m-hide' style='text-align:right'>{w1}</td>"
            f"<td class='col-m-hide' style='text-align:right'>{m1}</td>"
            f"<td style='text-align:right'>{rsi}</td>"
            f"<td>{signal}</td></tr>"
        )
    return out


def _key_movers(items: list, max_summary: int = 12) -> tuple[list, list]:
    """Split items into key movers (summary) and the rest (full)."""
    if len(items) <= max_summary:
        return items, []

    scored = []
    for item in items:
        s = 0
        chg = abs(item.get("change_pct_1d") or 0)
        s += chg * 2
        rsi = item.get("rsi_14")
        if rsi and (rsi >= 70 or rsi <= 30):
            s += 5
        signals = item.get("signals", [])
        if isinstance(signals, list):
            s += len(signals) * 3
        scored.append((s, item))

    scored.sort(key=lambda x: -x[0])
    summary = [item for _, item in scored[:max_summary]]
    rest = [item for _, item in scored[max_summary:]]
    return summary, rest


def _section_market_table(market_data: dict) -> str:
    categories = [
        ("indices", "Indices"),
        ("commodities", "Commodities"),
        ("stocks", "Stocks"),
        ("etfs", "ETFs"),
        ("crypto", "Crypto"),
        ("forex", "Forex"),
    ]
    counts = {k: len(market_data.get(k, [])) for k, _ in categories}
    total = sum(counts.values())
    n_cats = sum(1 for v in counts.values() if v)
    breakdown = ", ".join(f"{counts[k]} {k}" for k in counts if counts[k])

    header = f"""<div class="subtitle" style="margin-bottom:1rem;">{total} assets in {n_cats} categories ({breakdown}). Expand each section below.</div>
"""

    table_head = """<div class="card table-scroll wide-min sticky-first-col table-edge-hint">
<table>
<thead><tr><th>Ticker</th><th class="col-m-hide">Name</th><th style="text-align:right">Price</th><th style="text-align:right">1D</th>
<th class="col-m-hide" style="text-align:right">1W</th><th class="col-m-hide" style="text-align:right">1M</th><th style="text-align:right">RSI</th><th>Signals</th></tr></thead>
<tbody>"""
    table_tail = """</tbody>
</table>
</div>"""

    parts = [header]
    for category, label in categories:
        items = market_data.get(category, [])
        if not items:
            continue

        if category in ("stocks", "etfs") and len(items) > 12:
            summary, rest = _key_movers(items)
            summary_rows = "".join(_market_category_table_rows(summary))
            rest_rows = "".join(_market_category_table_rows(rest))
            content = (
                f'<div style="font-size:0.8rem;color:var(--text-dim);margin-bottom:0.5rem;">'
                f'Showing {len(summary)} key movers (biggest moves + signals). '
                f'{len(rest)} more assets available below.</div>'
                + table_head + summary_rows + table_tail
            )
            if rest:
                content += (
                    f'<details style="margin-top:0.5rem;">'
                    f'<summary style="cursor:pointer;color:var(--cyan);font-size:0.8rem;font-weight:600;">'
                    f'Show all {len(items)} {label.lower()} ▸</summary>'
                    + table_head + rest_rows + table_tail
                    + '</details>'
                )
            parts.append(_collapsible(f"{label} ({len(items)})", content))
        else:
            rows_html = "".join(_market_category_table_rows(items))
            parts.append(_collapsible(f"{label} ({len(items)})", table_head + rows_html + table_tail))

    inner = "\n".join(parts) + _glossary()
    return _collapsible(
        f"Market Overview — {total} assets in {n_cats} categories",
        inner,
        open_default=False,
        section_id="markets",
    )


def _glossary() -> str:
    terms = [
        ("DC", "Death Cross", "tag-critical",
         "The 50-day moving average has dropped below the 200-day moving average. "
         "This means recent prices (~2.5 months) are consistently lower than the longer-term average (~10 months) — "
         "momentum has shifted downward. It's a <strong>lagging</strong> indicator: by the time it triggers, "
         "the price drop has already happened. Better at confirming a trend than predicting one. "
         "When multiple stocks show death crosses simultaneously, it usually reflects broad market weakness "
         "rather than individual company problems."),

        ("GC", "Golden Cross", "tag-strong",
         "The opposite of a death cross — the 50-day moving average has risen above the 200-day moving average. "
         "This is a bullish signal indicating upward momentum. Like the death cross, it's a lagging indicator "
         "that confirms a trend already underway."),

        ("OB", "Overbought (RSI ≥ 70)", "tag-warning",
         "The Relative Strength Index (RSI) is at or above 70, meaning the asset has had strong recent gains "
         "and may be due for a pullback. It doesn't mean the price <em>will</em> drop — assets can stay "
         "overbought for extended periods in strong uptrends — but it signals that buying pressure may be "
         "getting stretched."),

        ("OS", "Oversold (RSI ≤ 30)", "tag-strong",
         "RSI is at or below 30, meaning the asset has had sharp recent declines and may be due for a bounce. "
         "This can represent a buying opportunity (value investors look for oversold quality assets), "
         "but it can also signal that something is fundamentally wrong. Context matters — "
         "oversold in a healthy market is different from oversold during a crisis."),
    ]

    column_terms = [
        ("RSI", "Relative Strength Index",
         "A momentum indicator ranging from 0 to 100 that measures the speed and magnitude of recent price changes. "
         "Calculated over the last 14 trading days. "
         "Below 30 = oversold (potential bounce), above 70 = overbought (potential pullback), "
         "30–70 = neutral. RSI is a lagging indicator — it shows what momentum <em>has been</em>, not what it will be."),

        ("50-day MA", "50-Day Moving Average",
         "The average closing price over the last 50 trading days (~2.5 months). "
         "Represents the short-to-medium-term trend. When the current price is above the 50-day MA, "
         "the short-term trend is generally positive."),

        ("200-day MA", "200-Day Moving Average",
         "The average closing price over the last 200 trading days (~10 months). "
         "Represents the long-term trend. Institutional investors often use this as a key support/resistance level. "
         "The relationship between the 50-day and 200-day MA produces death cross and golden cross signals."),

        ("1D / 1W / 1M", "Percentage Change",
         "Price change over the last 1 day, 1 week, or 1 month respectively. "
         "Green = positive (price went up), red = negative (price went down). "
         "Large single-day or weekly drops trigger warning signals in the risk engine."),

        ("P/E Ratio", "Price-to-Earnings",
         "The stock price divided by earnings per share. Measures how much investors are paying for each dollar of earnings. "
         "A high P/E can mean the stock is overvalued or that investors expect high future growth. "
         "A low P/E can mean the stock is undervalued or that the business is struggling. "
         "Always compare P/E within the same industry — tech companies normally have higher P/E than banks."),

        ("D/E", "Debt-to-Equity Ratio",
         "Total debt divided by shareholder equity. Measures how much a company relies on borrowing. "
         "Below 0.5 = conservatively financed, 0.5–1.0 = moderate, above 2.0 = heavily leveraged. "
         "High debt becomes dangerous when interest rates rise or revenue declines."),

        ("ROE", "Return on Equity",
         "Net income divided by shareholder equity. Measures how efficiently a company generates profit "
         "from its shareholders' investment. Above 15% is generally strong. "
         "Negative ROE means the company is losing money."),
    ]

    signal_rows = "\n".join(
        f'<tr><td><span class="tag {cls}">{abbr}</span></td>'
        f'<td style="font-weight:600;">{name}</td>'
        f'<td style="color:var(--text-dim);line-height:1.5;">{desc}</td></tr>'
        for abbr, name, cls, desc in terms
    )

    metric_rows = "\n".join(
        f'<tr><td style="font-weight:600;white-space:nowrap;color:var(--cyan);">{abbr}</td>'
        f'<td style="font-weight:600;">{name}</td>'
        f'<td style="color:var(--text-dim);line-height:1.5;">{desc}</td></tr>'
        for abbr, name, desc in column_terms
    )

    return """
<details style="margin-bottom:1rem;">
<summary style="cursor:pointer;color:var(--cyan);font-size:0.85rem;font-weight:600;padding:0.5rem 0;">
Glossary — What Do These Terms &amp; Signals Mean?
</summary>
<div class="card" style="margin-top:0.5rem;">

<div style="font-weight:600;font-size:0.85rem;margin-bottom:0.75rem;color:var(--text);">Signal Badges</div>
<div class="table-scroll wide-min sticky-first-col table-edge-hint">
<table style="margin-bottom:1.25rem;">
<thead><tr><th style="width:3.5rem;">Badge</th><th style="width:10rem;">Name</th><th>What It Means</th></tr></thead>
<tbody>""" + signal_rows + """</tbody>
</table>
</div>

<div style="border-top:1px solid var(--border);padding-top:1rem;">
<div style="font-weight:600;font-size:0.85rem;margin-bottom:0.75rem;color:var(--text);">Column Definitions &amp; Key Metrics</div>
<div class="table-scroll wide-min sticky-first-col table-edge-hint">
<table>
<thead><tr><th style="width:5.5rem;">Term</th><th style="width:10rem;">Full Name</th><th>What It Means</th></tr></thead>
<tbody>""" + metric_rows + """</tbody>
</table>
</div>
</div>

</div>
</details>"""


def _section_bond_bank_plain_english(macro_data: MacroSnapshot | None) -> str:
    inner = build_bond_bank_friend_html(macro_data)
    return _collapsible(
        "Banking & bonds — plain English",
        f'<div class="card">{inner}</div>',
        open_default=False,
        section_id="bonds-banks",
    )


def _section_authoritative_sources() -> str:
    """Curated links (desk research); not scraped at runtime."""
    links = [
        ("FRED (Federal Reserve Economic Data)", "https://fred.stlouisfed.org/"),
        ("U.S. Treasury — Fiscal Data", "https://fiscaldata.treasury.gov/"),
        ("U.S. Treasury — Home", "https://home.treasury.gov/"),
        ("FDIC — BankFind Suite", "https://banks.data.fdic.gov/bankfind-suite/bankfind"),
        ("FDIC — Data & statistics", "https://www.fdic.gov/data"),
        ("FFIEC — Central Data Repository (public)", "https://cdr.ffiec.gov/public/ManageFacilities.aspx"),
        ("Federal Reserve — H.8 (commercial banks, aggregate)", "https://www.federalreserve.gov/releases/h8/current/default.htm"),
        ("Federal Reserve — Z.1 Financial Accounts", "https://www.federalreserve.gov/releases/z1/default.htm"),
        ("Federal Reserve — Data", "https://www.federalreserve.gov/data.htm"),
        ("SEC — EDGAR (public company filings)", "https://www.sec.gov/edgar.shtml"),
        ("Bureau of Labor Statistics", "https://www.bls.gov/"),
        ("Federal Reserve Bank of New York — Data & statistics", "https://www.newyorkfed.org/data-and-statistics"),
        ("FINRA — Market data", "https://www.finra.org/finra-data"),
        ("SIFMA — Research & resources", "https://www.sifma.org/resources/research/"),
    ]
    lis = "".join(
        f'<li style="margin-bottom:0.35rem;"><a href="{escape(url)}" rel="noopener noreferrer" target="_blank">'
        f"{escape(title)}</a></li>"
        for title, url in links
    )
    content = (
        "<p>Starting points from regulators and standard data publishers — useful if you want to read the "
        "same underlying official series this report leans on (plus banking disclosure context).</p>"
        f'<ul style="margin:0.75rem 0 0 1.1rem;line-height:1.55;font-size:0.9rem;">{lis}</ul>'
    )
    return _collapsible(
        "Authoritative sources — banking & bonds",
        content,
        open_default=False,
        section_id="authoritative-sources",
    )


def _section_macro(macro_data: MacroSnapshot) -> str:
    category_order = ("core_macro", "banking_system", "bond_market")
    category_heading = {
        "core_macro": "Core macro",
        "banking_system": "Banking system",
        "bond_market": "Bond market",
    }
    category_subtitle = {
        "core_macro": "leading indicators",
        "banking_system": "system-wide aggregates (not CAMELS / not one bank)",
        "bond_market": "Treasuries &amp; investment-grade spreads",
    }

    by_cat: dict[str, list] = {}
    for ind in macro_data.indicators:
        by_cat.setdefault(ind.category, []).append(ind)

    sig_cls = {"critical": "tag-critical", "warning": "tag-warning", "bearish": "tag-warning",
               "bullish": "tag-strong", "neutral": "tag-info"}

    def _macro_indicator_row(ind) -> str:
        signal_class = sig_cls.get(ind.signal, "tag-info")
        change = f"{ind.change:+,.2f}" if ind.change is not None else "—"
        return (
            f"<tr><td><strong>{escape(ind.name)}</strong>"
            f"<span class='subtitle-detail' style='font-size:0.75rem;color:var(--text-dim);'>{escape(ind.series_id)}</span></td>"
            f"<td style='text-align:right;white-space:nowrap;'>{ind.value:,.2f}</td>"
            f"<td class='col-m-hide' style='text-align:right'>{change}</td>"
            f"<td style='white-space:nowrap;'><span class='tag {signal_class}'>{ind.signal}</span></td>"
            f"<td class='col-m-hide' style='color:var(--text-dim)'>{escape(ind.description)}</td></tr>"
        )

    def _append_category_block(cat: str, inds: list) -> None:
        if not inds:
            return
        sub = category_subtitle.get(cat, "")
        sub_html = f" <span class='subtitle-detail' style='font-weight:400;color:var(--text-dim);'>— {sub}</span>" if sub else ""
        rows.append(
            "<tr><td colspan=\"5\" style=\"background:var(--surface2);font-size:0.8rem;padding:0.35rem 0.6rem;"
            "border-top:1px solid var(--border);color:var(--text-dim);\">"
            f"<strong style=\"color:var(--text);\">{escape(category_heading.get(cat, cat))}</strong>{sub_html}</td></tr>"
        )
        rows.extend(_macro_indicator_row(ind) for ind in inds)

    rows: list[str] = []
    for cat in category_order:
        _append_category_block(cat, by_cat.pop(cat, None) or [])
    for cat in sorted(by_cat.keys()):
        _append_category_block(cat, by_cat[cat])

    alerts = ""
    if macro_data.yield_curve_inverted:
        alerts += '<div class="card" style="border-color:var(--red);background:#1c1917;margin-top:0.5rem;">⚠ <strong style="color:var(--red)">YIELD CURVE INVERTED</strong> — Historically precedes every US recession since 1955.</div>'
    if macro_data.credit_stress:
        alerts += '<div class="card" style="border-color:var(--red);background:#1c1917;margin-top:0.5rem;">⚠ <strong style="color:var(--red)">CREDIT STRESS DETECTED</strong> — Corporate distress fears rising.</div>'

    critical_count = sum(1 for ind in macro_data.indicators if ind.signal == "critical")
    warning_count = sum(1 for ind in macro_data.indicators if ind.signal == "warning")
    summary_parts = []
    if critical_count:
        summary_parts.append(f'<span style="color:var(--red);">{critical_count} critical</span>')
    if warning_count:
        summary_parts.append(f'<span style="color:var(--yellow);">{warning_count} warning</span>')
    if not summary_parts:
        summary_parts.append('<span style="color:var(--green);">all stable</span>')
    summary = " — " + ", ".join(summary_parts)

    methodology = (
        '<div style="font-size:0.8rem;color:var(--text-dim);line-height:1.45;margin-bottom:0.75rem;'
        'padding:0.55rem 0.65rem;background:var(--surface2);border-radius:0.5rem;border:1px solid var(--border);">'
        "<strong style=\"color:var(--text);\">Data vs. narrative.</strong> "
        "Table values are official FRED series. Category groupings label what kind of signal each block represents. "
        "Signals and the assessment column are automated rules for scan context — not investment advice, not forecasts, "
        "not regulatory ratings, and (for banking rows) not institution-level health."
        "</div>"
    )

    return _collapsible(
        f"Macroeconomic Indicators (FRED){summary}",
        methodology
        + f"""<div class="card table-scroll wide-min sticky-first-col table-edge-hint">
<table>
<thead><tr><th>Indicator</th><th style="text-align:right;width:5.5rem;">Value</th><th class="col-m-hide" style="text-align:right">Change</th><th style="width:4.5rem;">Signal</th><th class="col-m-hide">Assessment</th></tr></thead>
<tbody>{"".join(rows)}</tbody>
</table>
</div>
{alerts}""",
        section_id="macro",
    )


def _section_fundamentals(fundamentals: dict[str, StockFundamentals], name_lookup: dict[str, str] | None = None) -> str:
    name_lookup = name_lookup or {}
    rows = []
    for ticker, f in fundamentals.items():
        name = escape(name_lookup.get(ticker, ticker))
        health_class = f"tag-{f.fundamental_health}"
        eps = f.eps_revision_trend
        eps_style = "pos" if eps == "improving" else "neg" if eps == "deteriorating" else "neutral"
        insider = f.insider_signal
        insider_style = "pos" if insider == "buying" else "neg" if insider == "selling" else "neutral"
        upside = f"{f.upside_to_mean_target:+.1f}%" if f.upside_to_mean_target is not None else "—"
        de = f"{f.debt_to_equity:.2f}" if f.debt_to_equity is not None else "—"
        roe = f"{f.roe:.1%}" if f.roe is not None else "—"
        completeness = f"{f.data_completeness:.0%}"

        rows.append(
            f"<tr><td style='white-space:nowrap;'><strong>{escape(ticker)}</strong>"
            f"<span class='subtitle-detail' style='font-size:0.75rem;color:var(--text-dim);'>{name}</span></td>"
            f"<td style='white-space:nowrap;'><span class='tag {health_class}'>{f.fundamental_health}</span></td>"
            f"<td class='{eps_style}' style='white-space:nowrap;'>{eps}</td>"
            f"<td class='{insider_style}' style='white-space:nowrap;'>{insider}</td>"
            f"<td style='text-align:right;white-space:nowrap;'>{upside}</td>"
            f"<td class='col-m-hide' style='text-align:right'>{de}</td>"
            f"<td class='col-m-hide' style='text-align:right'>{roe}</td>"
            f"<td class='col-m-hide' style='text-align:right'>{completeness}</td></tr>"
        )

    strong = sum(1 for f in fundamentals.values() if f.fundamental_health == "strong")
    distressed = sum(1 for f in fundamentals.values() if f.fundamental_health in ("weak", "distressed"))
    summary_parts = []
    if strong:
        summary_parts.append(f'<span style="color:var(--green);">{strong} strong</span>')
    if distressed:
        summary_parts.append(f'<span style="color:var(--red);">{distressed} weak/distressed</span>')
    summary = " — " + ", ".join(summary_parts) if summary_parts else ""

    return _collapsible(
        f"Fundamentals ({len(fundamentals)} stocks){summary}",
        f"""<div class="card table-scroll wide-min sticky-first-col table-edge-hint">
<table>
<thead><tr><th>Ticker</th><th>Health</th><th>EPS Trend</th><th>Insiders</th>
<th style="text-align:right">Analyst Upside</th><th class="col-m-hide" style="text-align:right">D/E</th>
<th class="col-m-hide" style="text-align:right">ROE</th><th class="col-m-hide" style="text-align:right">Data</th></tr></thead>
<tbody>{"".join(rows)}</tbody>
</table>
</div>""",
        section_id="fundamentals",
    )


def _section_opportunities(opportunities: list[Opportunity], health: MarketHealthReport) -> str:
    longs = [o for o in opportunities if o.direction == "long"]
    shorts = [o for o in opportunities if o.direction == "short"]

    risk_color_map = {
        range(1, 4): "#22c55e",
        range(4, 7): "#eab308",
        range(7, 11): "#ef4444",
    }

    def _risk_color(score: int) -> str:
        for r, c in risk_color_map.items():
            if score in r:
                return c
        return "#6b7280"

    conf_color_map = {"high": "#22c55e", "medium": "#eab308", "low": "#ef4444"}

    def _opp_card(opp: Opportunity) -> str:
        rc = _risk_color(opp.risk_score)
        cc = conf_color_map.get(opp.confidence, "#6b7280")
        direction_icon = "&#9650;" if opp.direction == "long" else "&#9660;"
        direction_color = "var(--green)" if opp.direction == "long" else "var(--red)"

        signals_for_html = "".join(f"<li>{escape(s)}</li>" for s in opp.signals_for)
        signals_against_html = "".join(f"<li>{escape(s)}</li>" for s in opp.signals_against)

        return f"""
<div class="card" style="border-left:3px solid {rc};">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.5rem;margin-bottom:0.75rem;">
    <div style="display:flex;flex-wrap:wrap;align-items:center;gap:0.35rem 0.75rem;">
      <span style="font-size:1.3rem;color:{direction_color};">{direction_icon}</span>
      <span style="font-size:1.1rem;font-weight:700;">{escape(opp.ticker)}</span>
      <span style="font-size:0.85rem;color:var(--text-dim);">{escape(opp.name)}</span>
      <span class="tag tag-{"strong" if opp.direction == "long" else "critical"}">{opp.direction.upper()}</span>
      <span style="font-size:0.8rem;color:var(--text-dim);">{opp.horizon_label}</span>
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:0.35rem 0.75rem;align-items:center;">
      <span style="font-size:0.8rem;">Risk: <strong style="color:{rc};">{opp.risk_score}/10 ({opp.risk_label})</strong></span>
      <span style="font-size:0.8rem;">Confidence: <strong style="color:{cc};">{opp.confidence.upper()}</strong></span>
    </div>
  </div>

  <div style="font-size:0.85rem;line-height:1.6;margin-bottom:0.75rem;">{escape(opp.thesis)}</div>

  <div class="opp-signal-grid">
    <div>
      <div style="font-size:0.75rem;font-weight:600;color:var(--green);margin-bottom:0.3rem;">SIGNALS FOR</div>
      <ul style="font-size:0.8rem;color:var(--text-dim);margin:0;padding-left:1.2rem;line-height:1.6;">{signals_for_html}</ul>
    </div>
    <div>
      <div style="font-size:0.75rem;font-weight:600;color:var(--red);margin-bottom:0.3rem;">SIGNALS AGAINST</div>
      <ul style="font-size:0.8rem;color:var(--text-dim);margin:0;padding-left:1.2rem;line-height:1.6;">{signals_against_html if signals_against_html else "<li>None detected</li>"}</ul>
    </div>
  </div>

  <div style="background:var(--surface2);border-radius:0.5rem;padding:0.6rem 0.75rem;margin-bottom:0.5rem;">
    <span style="font-size:0.75rem;font-weight:600;color:var(--red);">WHAT COULD GO WRONG:</span>
    <span style="font-size:0.8rem;color:var(--text-dim);"> {escape(opp.risks)}</span>
  </div>

  <div style="font-size:0.8rem;color:var(--text-dim);">
    Position sizing: <strong>{escape(opp.position_sizing)}</strong>
  </div>
</div>"""

    long_cards = "".join(_opp_card(o) for o in longs) if longs else '<div class="card" style="color:var(--text-dim);">No long opportunities identified in current conditions.</div>'
    short_cards = "".join(_opp_card(o) for o in shorts) if shorts else '<div class="card" style="color:var(--text-dim);">No short opportunities identified in current conditions.</div>'

    market_risk_color = {
        "low": "var(--green)", "moderate": "var(--yellow)", "elevated": "var(--orange)",
        "high": "var(--red)", "critical": "var(--red)",
        "severe": "var(--red)", "extreme": "var(--red)", "catastrophic": "var(--red)",
    }.get(health.overall_risk, "var(--text)")

    long_count = len(longs)
    short_count = len(shorts)
    summary = f' — <span style="color:var(--green);">{long_count} long</span>, <span style="color:var(--red);">{short_count} short</span>'

    content = f"""
<div class="card" style="background:var(--surface2);border-color:var(--yellow);margin-bottom:1rem;">
<div style="font-size:0.8rem;color:var(--text-dim);line-height:1.6;">
<strong style="color:var(--yellow);">Important:</strong> Opportunities are ranked by risk-adjusted potential.
Higher expected return generally comes with higher risk — there is no such thing as "guaranteed low risk, high yield."
Every opportunity lists what could go wrong. Position sizing reflects the risk level.
This is analysis for educational purposes, not financial advice.
Market risk is currently <strong style="color:{market_risk_color};">{health.overall_risk.upper()}</strong>
 — factor this into all decisions.
</div>
</div>

<h3 style="color:var(--green);font-size:0.95rem;margin:1.25rem 0 0.75rem;">&#9650; Long Opportunities (Buy)</h3>
{long_cards}

<h3 style="color:var(--red);font-size:0.95rem;margin:1.25rem 0 0.75rem;">&#9660; Short Opportunities</h3>
<div style="font-size:0.8rem;color:var(--text-dim);margin-bottom:0.75rem;padding:0.5rem;background:var(--surface2);border-radius:0.5rem;">
Short selling means profiting from price declines. It carries <strong>unlimited loss risk</strong> — if the stock rises instead of falls,
losses are theoretically uncapped. Max position: 1% of portfolio with a defined buy-stop.
</div>
{short_cards}"""

    return _collapsible(
        f"Opportunities ({len(opportunities)} found){summary}",
        content,
        open_default=False,
        section_id="opportunities",
    )


def _section_signals(health: MarketHealthReport) -> str:
    rows = []
    sorted_signals = sorted(health.signals, key=lambda s: (
        {"critical": 0, "warning": 1, "info": 2}.get(s.severity, 3),
        0 if s.signal_type == "leading" else 1,
    ))
    for sig in sorted_signals:
        type_class = "tag-leading" if sig.signal_type == "leading" else "tag-lagging"
        rows.append(
            f"<tr><td>{_severity_tag_html(sig.severity)}</td>"
            f"<td class='col-m-hide'><span class='tag {type_class}'>{sig.signal_type}</span></td>"
            f"<td class='col-m-hide'>{escape(sig.category)}</td>"
            f"<td><strong>{escape(sig.name)}</strong></td>"
            f"<td style='color:var(--text-dim)'>{escape(sig.message)}</td></tr>"
        )

    crit = health.critical_count
    warn = health.warning_count
    summary_parts = []
    if crit:
        summary_parts.append(f'<span style="color:var(--red);">{crit} critical</span>')
    if warn:
        summary_parts.append(f'<span style="color:var(--yellow);">{warn} warning</span>')
    summary = " — " + ", ".join(summary_parts) if summary_parts else ""

    return _collapsible(
        f"Risk Signals ({len(health.signals)}){summary}",
        f"""<div class="card table-scroll wide-min sticky-first-col table-edge-hint">
{_severity_legend_html()}
<table>
<thead><tr><th>Severity</th><th class="col-m-hide">Type</th><th class="col-m-hide">Category</th><th>Signal</th><th>Detail</th></tr></thead>
<tbody>{"".join(rows)}</tbody>
</table>
</div>""",
        section_id="signals",
    )


def _section_historical_parallels(sp500_price: float | None) -> str:
    """Public-safe section comparing current situation to historical crashes."""
    peak = 6900
    if sp500_price:
        decline_pct = ((sp500_price - peak) / peak) * 100
    else:
        decline_pct = -7.5

    rows = []
    for crash in CRASHES:
        if crash.name.startswith("2026"):
            continue
        rec = f"{crash.months_to_recovery:.0f} months" if crash.months_to_recovery else "—"
        oil_tag = '<span style="color:var(--orange);">Yes</span>' if crash.oil_shock else "No"
        rows.append(
            f"<tr><td>{escape(crash.name)}</td>"
            f"<td style='text-align:right;color:var(--red);'>{crash.decline_pct:.1f}%</td>"
            f"<td style='text-align:right;'>{crash.days_to_bottom}</td>"
            f"<td style='text-align:right;'>{rec}</td>"
            f"<td>{oil_tag}</td></tr>"
        )

    current_bar_pct = min(abs(decline_pct) / 90 * 100, 100)
    comparison = crash_comparison_for_dashboard(sp500_price or (peak * (1 + decline_pct / 100)), peak)
    best_match = comparison.get("best_match")
    match_name = best_match.name if best_match else "1973-74 Oil Crisis"

    return _collapsible(
        f"Crisis Context: Historical Parallels — current decline {decline_pct:+.1f}%, closest match: {match_name}",
        f"""<div class="card">
<div style="margin-bottom:1rem;">
  <div style="font-size:0.85rem;color:var(--text-dim);margin-bottom:0.3rem;">Current S&amp;P 500 decline from Jan 2026 peak (~6,900)</div>
  <div style="display:flex;align-items:center;gap:0.75rem;">
    <div style="flex-grow:1;height:20px;background:var(--surface);border-radius:10px;overflow:hidden;">
      <div style="width:{current_bar_pct:.1f}%;height:100%;background:linear-gradient(to right,#eab308,#ef4444,#dc2626);border-radius:10px;"></div>
    </div>
    <span style="font-weight:700;color:var(--red);min-width:4rem;text-align:right;">{decline_pct:+.1f}%</span>
  </div>
  <div style="display:flex;justify-content:space-between;font-size:0.75rem;color:var(--text-dim);padding:0 2px;margin-top:2px;">
    <span>0%</span><span>-10%</span><span>-20%</span><span>-30%</span><span>-50%</span><span>-90%</span>
  </div>
</div>
<div class="table-scroll wide-min table-edge-hint sticky-first-col">
<table>
<thead><tr><th>Crash</th><th style="text-align:right">Decline</th><th style="text-align:right">Days to Bottom</th><th style="text-align:right">Recovery</th><th>Oil Shock?</th></tr></thead>
<tbody>
{"".join(rows)}
</tbody>
</table>
</div>
<div style="margin-top:1rem;font-size:0.85rem;color:var(--text-dim);line-height:1.6;">
<strong>Key finding:</strong> Across 8 major US crashes (1907-2020), the market recovered every time.
The only crash where early withdrawal would have been correct was 1929 — under conditions (no FDIC, no SEC, no Fed backstop)
that cannot recur in the modern financial system. Average oil-shock recovery: {comparison.get("avg_oil_crash_recovery_months", 0):.0f} months.
</div>
</div>""",
        section_id="historical",
    )


def _section_supply_chain() -> str:
    """Public-safe supply chain risk monitor — no personal data."""
    cascade_stages = [
        ("Week 1-2", "Oil Price Shock", "Brent crude spikes, WTI follows. Gasoline prices at pump rise within days.", True),
        ("Month 1-2", "Energy Cost Cascade", "Natural gas prices spike (LNG rerouting). Electricity costs rise in Europe/Asia. Industrial production slows.", True),
        ("Month 2-4", "Helium &amp; Semiconductor Squeeze", "Helium spot prices surge. Semiconductor fabs reduce output. Lead times extend to 6-12 months for advanced chips.", True),
        ("Month 3-6", "Fertilizer &amp; Food Pressure", "Urea/ammonia prices spike. Spring planting disrupted. Food inflation becomes visible in grocery prices.", True),
        ("Month 4-8", "Pharmaceutical Delays", "India flags raw material shortages. Generic drug supply chains lengthen. Some medications face spot shortages.", False),
        ("Month 6-12", "Industrial Slowdown", "Petrochemical feedstock shortages. Plastics and packaging costs rise. Manufacturing slows in chemical-dependent sectors.", False),
        ("Year 1-3", "Infrastructure Rebuild", "Even after ceasefire, Ras Laffan and Gulf infrastructure require years to rebuild. LNG and helium supply remain constrained.", False),
        ("Year 3-5+", "New Supply Equilibrium", "Alternative supply chains mature. New helium plants (US, Russia, Algeria) reach capacity. Markets find new equilibrium at higher price levels.", False),
    ]

    stage_rows = []
    for timeframe, name, desc, is_active in cascade_stages:
        status = '<span style="color:var(--red);font-weight:600;">ACTIVE</span>' if is_active else '<span style="color:var(--text-dim);">Projected</span>'
        bg = "background:rgba(239,68,68,0.08);" if is_active else ""
        stage_rows.append(
            f'<tr style="{bg}"><td style="white-space:nowrap;font-weight:600;">{timeframe}</td>'
            f"<td><strong>{name}</strong><br><span style='font-size:0.8rem;color:var(--text-dim);'>{desc}</span></td>"
            f"<td>{status}</td></tr>"
        )

    return _collapsible(
        "Crisis Context: Supply Chain Cascade — Strait of Hormuz",
        f"""<div class="card">
<div style="font-size:0.85rem;color:var(--text-dim);margin-bottom:1rem;line-height:1.5;">
The Strait of Hormuz carries ~21% of global oil, ~25% of global LNG, and hosts the world's largest helium processing
facility at Ras Laffan, Qatar. Disruption creates a cascading timeline of impacts far beyond oil prices.
This is publicly sourced information — not personal financial data.
</div>
<div class="table-scroll table-edge-hint">
<table>
<thead><tr><th>Timeframe</th><th>Impact</th><th>Status</th></tr></thead>
<tbody>
{"".join(stage_rows)}
</tbody>
</table>
</div>
<div style="margin-top:1rem;font-size:0.8rem;color:var(--text-dim);line-height:1.5;">
<strong>Why this matters for markets:</strong> The 1973 oil crisis caused a 48% market decline and 8-year recovery.
The current situation is broader — it affects energy, semiconductors, food, and pharmaceuticals simultaneously.
Historical parallel suggests a longer recovery timeline than a pure oil shock.
</div>
</div>""",
        section_id="supply-chain",
    )


def _section_trend_context(trend_context: str) -> str:
    return _collapsible(
        "Historical Context",
        f"""<div class="card">
<pre>{escape(trend_context)}</pre>
</div>"""
    )


def _section_definitions() -> str:
    content = """
<div class="card" style="line-height:1.8;">

<h3 style="color:var(--cyan);font-size:0.95rem;margin-bottom:0.75rem;">Time Horizons</h3>
<div class="table-scroll sticky-first-col table-edge-hint">
<table style="width:100%;margin-bottom:1.25rem;">
<tbody>
<tr><td style="white-space:nowrap;vertical-align:top;padding-right:1rem;"><strong>Short-term</strong><br><span style="color:var(--text-dim);font-size:0.8rem;">1–4 weeks</span></td>
<td style="color:var(--text-dim);font-size:0.85rem;">Momentum / technical plays — oversold bounces, mean reversion, technical pattern setups.</td></tr>
<tr><td style="white-space:nowrap;vertical-align:top;padding-right:1rem;"><strong>Medium-term</strong><br><span style="color:var(--text-dim);font-size:0.8rem;">1–3 months</span></td>
<td style="color:var(--text-dim);font-size:0.85rem;">Earnings revision plays — improving fundamentals not yet priced in.</td></tr>
<tr><td style="white-space:nowrap;vertical-align:top;padding-right:1rem;"><strong>Long-term</strong><br><span style="color:var(--text-dim);font-size:0.8rem;">3–12 months</span></td>
<td style="color:var(--text-dim);font-size:0.85rem;">Value plays — quality businesses at discounted prices with macro tailwinds.</td></tr>
</tbody>
</table>
</div>

<h3 style="color:var(--cyan);font-size:0.95rem;margin-bottom:0.75rem;">Risk Levels (per opportunity, 1–10)</h3>
<div class="table-scroll sticky-first-col table-edge-hint">
<table style="width:100%;margin-bottom:1.25rem;">
<tbody>
<tr><td style="white-space:nowrap;vertical-align:top;padding-right:1rem;"><span class="tag tag-strong">Low (1–3)</span></td>
<td style="color:var(--text-dim);font-size:0.85rem;">3+ signals agree, strong fundamentals, macro supportive, limited downside history. <strong style="color:var(--text);">Position: 3–5%</strong></td></tr>
<tr><td style="white-space:nowrap;vertical-align:top;padding-right:1rem;"><span class="tag tag-warning">Medium (4–6)</span></td>
<td style="color:var(--text-dim);font-size:0.85rem;">2 signals agree, some data gaps or mixed macro. <strong style="color:var(--text);">Position: 1–3%</strong></td></tr>
<tr><td style="white-space:nowrap;vertical-align:top;padding-right:1rem;"><span class="tag tag-critical">High (7–10)</span></td>
<td style="color:var(--text-dim);font-size:0.85rem;">Single signal, weak fundamentals, adverse macro, or speculative. <strong style="color:var(--text);">Position: 0.5–1%</strong></td></tr>
</tbody>
</table>
</div>

<h3 style="color:var(--cyan);font-size:0.95rem;margin-bottom:0.75rem;">Honest Framing</h3>
<p style="color:var(--text-dim);font-size:0.85rem;">
Opportunities are ranked by <strong style="color:var(--text);">risk-adjusted potential</strong>, not "low risk / high yield."
Higher expected return generally comes with higher risk.
The system surfaces the best opportunities at each risk level and is explicit about what could go wrong.
</p>

</div>"""
    return _collapsible("Definitions — Time Horizons, Risk Levels &amp; Methodology", content)


def _collapsible(
    title: str,
    content: str,
    open_default: bool = False,
    section_id: str | None = None,
) -> str:
    open_attr = " open" if open_default else ""
    id_attr = f' id="{escape(section_id)}"' if section_id else ""
    anchor_cls = " section-anchor" if section_id else ""
    return f"""
<details class="section-collapse{anchor_cls}"{open_attr}{id_attr}>
<summary class="section-header">{title}</summary>
<div class="section-body">
{content}
</div>
</details>"""


def _pct_cell(value: float | None) -> str:
    if value is None:
        return '<span class="neutral">—</span>'
    css = "pos" if value > 0 else "neg" if value < 0 else "neutral"
    return f'<span class="{css}">{value:+.2f}%</span>'


def _signal_badges(item: dict) -> str:
    badges = []
    rsi = item.get("rsi_14")
    if rsi and rsi >= 70:
        badges.append('<span class="tag tag-warning">OB</span>')
    elif rsi and rsi <= 30:
        badges.append('<span class="tag tag-strong">OS</span>')
    ma50 = item.get("fifty_day_ma")
    ma200 = item.get("two_hundred_day_ma")
    if ma50 and ma200:
        if ma50 < ma200:
            badges.append('<span class="tag tag-critical">DC</span>')
        elif ma50 > ma200 * 1.02:
            badges.append('<span class="tag tag-strong">GC</span>')
    return " ".join(badges) if badges else '<span class="neutral">—</span>'
