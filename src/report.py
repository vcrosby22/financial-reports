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
from .personal.historical import CRASHES, FACTOR_LABELS, crash_comparison_for_dashboard, get_all_crashes
from .analysis.memory import build_trend_context
from .analysis.opportunities import Opportunity, screen_opportunities
from .analysis.risk import (
    MarketHealthReport,
    assess_market_health,
    direction_word,
    display_label,
    get_position_guidance,
)
from .config import load_config
from .data.database import get_session, init_db
from .data.models import MarketSnapshot
from .data.risk_score_daily import upsert_daily_risk_snapshot
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
    if macro_data is not None and not macro_data.indicators:
        return (
            '<div class="subtitle-line subtitle-detail">Macro (FRED): <strong>no series loaded</strong> '
            "(key may be invalid, rate-limited, or every series request failed — check Terminal output).</div>"
        )
    return (
        '<div class="subtitle-line subtitle-detail">Macro (FRED): <strong>not loaded</strong> — set '
        "<code>FRED_API_KEY</code> in <code>.env</code> (or the environment) and run "
        "<code>pip install fredapi</code>.</div>"
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

    supply_chain_proxies: dict[str, dict] = {}
    sc_tickers = config.get("supply_chain_proxies", [])
    if sc_tickers:
        console.print("  Supply chain proxies...")
        for item in fetch_multiple(sc_tickers, asset_type="supply_chain_proxy"):
            supply_chain_proxies[item["ticker"]] = item

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

    console.print("  Fetching optional cascade data sources...")
    from .data.hormuz import fetch_hormuz_data
    from .data.openfda import fetch_fda_shortages
    from .data.eia import fetch_eia_data
    hormuz_data = fetch_hormuz_data()
    fda_data = fetch_fda_shortages()
    eia_data = fetch_eia_data()

    console.print("  Evaluating supply chain cascade...")
    from .analysis.supply_chain import CascadeStage, evaluate_cascade, persist_cascade_snapshot
    cascade_stages = evaluate_cascade(
        supply_chain_proxies, macro_data, commodities,
        hormuz=hormuz_data, fda_shortages=fda_data, eia=eia_data,
        config=config,
    )
    active_stages = [s for s in cascade_stages if s.status == "active"]
    console.print(f"  Cascade: {len(active_stages)} active, {len(cascade_stages) - len(active_stages)} projected/not_started")
    sc_log = persist_cascade_snapshot(cascade_stages)
    if sc_log:
        console.print(f"[dim]Supply chain log: {sc_log}[/dim]")

    console.print("  Computing forward projection...")
    from .analysis.projection import BottomEstimate, RiskProjection, compute_bottom_estimate, compute_projection
    active_cascade_count = len([s for s in cascade_stages if s.status == "active"])
    projection = compute_projection(risk_trend, macro_data, active_cascade_count)
    console.print(f"  Projection: {projection.label} (confidence {projection.confidence:.0%})")

    console.print("  Computing bottom estimate...")
    sp500_idx = next((i for i in market_data.get("indices", []) if i.get("ticker") == "^GSPC"), None)
    sp500_for_estimate = sp500_idx["price"] if sp500_idx and sp500_idx.get("price") else None
    from .personal.historical import find_similar_crashes as _find_similar, get_all_crashes as _get_all
    _all = _get_all(sp500_for_estimate, macro_data, active_cascade_count)
    _current_ev = next((c for c in _all if c.name.startswith("2026")), None)
    _current_factors = _current_ev.crisis_factors if _current_ev else set()
    _current_decline = _current_ev.decline_pct if _current_ev else -7.5
    _similar = _find_similar(_current_decline, sp500_price=sp500_for_estimate, macro=macro_data, cascade_active_count=active_cascade_count)
    bottom_estimate = compute_bottom_estimate(sp500_for_estimate, _similar, _current_factors)
    if bottom_estimate:
        console.print(f"  Bottom estimate: optimistic {bottom_estimate.optimistic_decline:.1f}%, base {bottom_estimate.base_decline:.1f}%, pessimistic {bottom_estimate.pessimistic_decline:.1f}%")

    console.print("  Building HTML...")
    html = _build_html(market_data, macro_data, fundamentals, health, trend_context, opportunities, risk_trend, cascade_stages, projection, bottom_estimate)

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
    daily_path = upsert_daily_risk_snapshot(health)
    if daily_path:
        console.print(f"[dim]Risk score daily snapshot: {daily_path}[/dim]")
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
    cascade_stages: list | None = None,
    projection: object | None = None,
    bottom_estimate: object | None = None,
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
    elif macro_data is not None:
        fred_footer = "Macro (FRED): API path ran but zero indicators — see subtitle above."
    else:
        fred_footer = "Macro (FRED): not loaded — FRED_API_KEY + fredapi required for macro block."

    risk_color = {
        "low": "#22c55e", "moderate": "#eab308", "elevated": "#f97316",
        "high": "#ef4444", "acute_stress": "#dc2626",
        "compounding_stress": "#991b1b", "severe_stress": "#7f1d1d", "heavy_stress": "#b91c1c",
    }.get(health.overall_risk, "#6b7280")

    conf_color = {"high": "#22c55e", "medium": "#eab308", "low": "#ef4444"}.get(health.confidence, "#6b7280")

    guidance = get_position_guidance(health.overall_risk)

    indices = market_data.get("indices", [])
    commodities = market_data.get("commodities", [])
    vix_data = next((i for i in indices if i.get("ticker") == "^VIX"), None)
    sp500_kpi = next((i for i in indices if i.get("ticker") == "^GSPC"), None)
    dow_kpi = next((i for i in indices if i.get("ticker") == "^DJI"), None)
    nasdaq_kpi = next((i for i in indices if i.get("ticker") == "^IXIC"), None)
    oil_kpi = next((i for i in commodities if i.get("ticker") == "BZ=F"), None) if commodities else None

    from .data.risk_score_daily import backfill_daily_from_jsonl, list_daily_snapshots_chronological
    backfill_daily_from_jsonl()
    daily_snapshots = list_daily_snapshots_chronological()

    sections = []
    sections.append(_section_kpi_cards(health, risk_color, sp500_kpi, dow_kpi, nasdaq_kpi, vix_data, oil_kpi, risk_trend))
    risk_inner = _section_risk_summary(health, risk_color, conf_color, guidance, daily_snapshots, risk_trend)
    risk_inner += _snapshot_narrative(health, risk_trend)
    if risk_trend and risk_trend.has_any:
        risk_inner += _section_risk_trend(risk_trend)
    if projection and hasattr(projection, 'direction'):
        risk_inner += _section_projection(projection)
    risk_inner += _section_risk_score_reader_context(health, risk_trend)
    _ro_score = _health_uncapped_score(health)
    _ro_delta = ""
    if risk_trend and risk_trend.delta_1d is not None:
        _d = risk_trend.delta_1d
        _ro_delta = f" ({_d:+d} vs prior day)"
    sections.append(_collapsible(
        f'Risk Overview — Score {_ro_score}{_ro_delta} · <span style="color:{risk_color}">{display_label(health.overall_risk)}</span>',
        risk_inner,
        open_default=False,
        section_id="risk",
    ))
    sp500_data = next((i for i in indices if i.get("ticker") == "^GSPC"), None)
    sp500_price = sp500_data["price"] if sp500_data and sp500_data.get("price") else None
    cascade_active = sum(1 for s in (cascade_stages or []) if getattr(s, 'status', '') == "active")
    sections.append(_section_historical_parallels(sp500_price, macro_data, cascade_active, bottom_estimate))
    sections.append(_section_supply_chain(cascade_stages))
    sections.append(_section_score_attribution(health))
    sections.append(_section_risk_legend(health))
    sections.append(_section_market_table(market_data))
    if macro_data and macro_data.indicators:
        sections.append(_section_macro(macro_data))
        inflation_html = _section_inflation(macro_data)
        if inflation_html:
            sections.append(inflation_html)
    if fundamentals:
        name_lookup = {item["ticker"]: item.get("name", item["ticker"])
                       for cat in market_data.values() if isinstance(cat, list)
                       for item in cat if isinstance(item, dict) and "ticker" in item}
        sections.append(_section_fundamentals(fundamentals, name_lookup))
    if opportunities:
        sections.append(_section_opportunities(opportunities, health))
    sections.append(_section_signals(health))
    sections.append(_section_bond_bank_plain_english(macro_data))
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
.tag-elevated {{ background: #b45309; color: #fef3c7; border: 1px solid #d97706; }}
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
  position: relative;
  box-shadow: inset -12px 0 14px -12px rgba(0, 0, 0, 0.55);
}}
.table-scroll.table-edge-hint::after {{
  content: ''; position: absolute; top: 0; right: 0; bottom: 0;
  width: 2rem; pointer-events: none; z-index: 5;
  background: linear-gradient(to right, transparent, var(--bg));
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
#macro table td:first-child,
#macro table th:first-child {{ max-width: 7rem; word-break: break-word; }}
/* Risk Signals on narrow screens: WebKit miscomputes columns when .col-m-hide removes cells
   from a fixed-layout table — badge paints over Signal. Flatten to block + CSS grid per row. */
#signals .table-scroll.wide-min > table {{
  min-width: 0 !important;
  width: 100%;
  max-width: 100%;
  display: block;
  border-collapse: separate;
  border-spacing: 0;
}}
#signals .table-scroll.wide-min > table thead,
#signals .table-scroll.wide-min > table tbody {{
  display: block;
}}
#signals .sticky-first-col table th:first-child,
#signals .sticky-first-col table td:first-child {{
  position: static !important;
  left: auto !important;
  box-shadow: none !important;
  z-index: auto !important;
}}
#signals thead tr {{
  display: grid;
  grid-template-columns: auto 1fr 1fr;
  gap: 0.35rem 0.5rem;
  padding-bottom: 0.45rem;
  margin-bottom: 0.25rem;
  border-bottom: 1px solid var(--border);
}}
#signals thead th {{
  display: block;
  padding: 0.1rem 0 !important;
  border-bottom: none !important;
}}
#signals thead th:nth-child(1) {{ grid-column: 1; }}
#signals thead th:nth-child(4) {{ grid-column: 2; min-width: 0; }}
#signals thead th:nth-child(5) {{ grid-column: 3; min-width: 0; }}
#signals tbody tr {{
  display: grid;
  grid-template-columns: auto 1fr;
  grid-template-rows: auto auto;
  column-gap: 0.65rem;
  row-gap: 0.3rem;
  padding: 0.55rem 0;
  border-bottom: 1px solid var(--surface2);
  align-items: start;
}}
#signals tbody td {{
  display: block;
  padding: 0 !important;
  border-bottom: none !important;
}}
#signals tbody td:nth-child(1) {{ grid-column: 1; grid-row: 1; }}
#signals tbody td:nth-child(4) {{
  grid-column: 2;
  grid-row: 1;
  min-width: 0;
  word-break: break-word;
  overflow-wrap: break-word;
}}
#signals tbody td:nth-child(5) {{
  grid-column: 1 / -1;
  grid-row: 2;
  min-width: 0;
  word-break: break-word;
  overflow-wrap: break-word;
  color: var(--text-dim);
  font-size: 0.82rem;
  line-height: 1.45;
}}
.kpi-row {{
  display: grid; grid-template-columns: 1fr 1fr; gap: 0.75rem;
  margin-bottom: 1.5rem;
}}
.kpi-row.kpi-6 {{
  grid-template-columns: repeat(2, 1fr);
}}
.kpi-row > div {{
  min-width: 0;
}}
.opp-signal-grid {{
  display: grid; grid-template-columns: 1fr;
  gap: 1rem; margin-bottom: 0.75rem;
}}
.estimate-row {{
  display: grid; grid-template-columns: 1fr 1fr; gap: 0.35rem;
}}
.estimate-zone {{
  min-width: 0; padding: 0.6rem 0.5rem; border-radius: 0.4rem; text-align: center;
}}
.estimate-val {{ font-size: clamp(0.95rem, 0.9rem + 0.2vw, 1.1rem); }}
.estimate-val-lg {{ font-size: clamp(1.05rem, 1rem + 0.25vw, 1.3rem); }}
.estimate-arrow {{ display: none; }}
.section-detail {{ display: none; }}
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
.nav-bar::after {{
  content: ''; position: sticky; right: 0; flex-shrink: 0;
  min-width: 1.5rem; min-height: 100%; pointer-events: none;
  background: linear-gradient(to right, transparent, rgba(15, 23, 42, 0.92));
}}
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
  .estimate-row {{ display: flex; gap: 0.35rem; align-items: stretch; }}
  .estimate-zone {{ flex: 1; }}
  .estimate-arrow {{
    display: flex; align-items: center; color: var(--text-dim);
    font-size: 0.7rem; padding: 0 0.3rem;
  }}
  .section-detail {{ display: inline; }}
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
  #macro table td:first-child,
  #macro table th:first-child {{ max-width: none; }}
  #signals .table-scroll.wide-min > table {{
    display: table !important;
    min-width: 30rem !important;
    width: max-content;
    max-width: none;
    table-layout: auto;
  }}
  #signals .table-scroll.wide-min > table thead {{
    display: table-header-group !important;
  }}
  #signals .table-scroll.wide-min > table tbody {{
    display: table-row-group !important;
  }}
  #signals thead tr,
  #signals tbody tr {{
    display: table-row !important;
    grid-template-columns: unset;
    gap: unset;
    column-gap: unset;
    row-gap: unset;
    padding: unset;
    margin: unset;
    border: none;
    align-items: unset;
  }}
  #signals thead th,
  #signals tbody td {{
    display: table-cell !important;
    grid-column: unset;
    grid-row: unset;
  }}
  #signals thead th {{
    padding: 0.6rem 0.75rem !important;
    border-bottom: 1px solid var(--border) !important;
  }}
  #signals tbody td {{
    padding: 0.5rem 0.75rem !important;
    border-bottom: 1px solid var(--surface2) !important;
  }}
  #signals tbody td:nth-child(5) {{
    font-size: inherit;
    color: inherit;
    line-height: inherit;
  }}
  #signals .sticky-first-col table th:first-child,
  #signals .sticky-first-col table td:first-child {{
    position: sticky !important;
    left: 0 !important;
    z-index: 2 !important;
    background: var(--surface) !important;
    box-shadow: 4px 0 10px -4px rgba(0, 0, 0, 0.5) !important;
  }}
  #signals .sticky-first-col table thead th:first-child {{
    z-index: 4 !important;
    background: var(--surface) !important;
  }}
  #signals table td:nth-child(1),
  #signals table th:nth-child(1) {{ width: auto; min-width: 0; vertical-align: inherit; }}
  #signals table td:nth-child(4),
  #signals table th:nth-child(4) {{ width: auto; white-space: normal; }}
  #signals table td:nth-child(5),
  #signals table th:nth-child(5) {{ width: auto; min-width: 0; }}
  .table-scroll.wide-min > table {{ min-width: 30rem; }}
  .opp-signal-grid {{ grid-template-columns: 1fr 1fr; }}
  .kpi-row {{ grid-template-columns: repeat(3, 1fr); gap: 1rem; }}
  .kpi-row.kpi-6 {{ grid-template-columns: repeat(3, 1fr); }}
  .nav-bar {{
    flex-wrap: wrap; overflow-x: visible; justify-content: center;
    padding: 0.5rem 0; gap: 0.25rem 0.5rem;
    padding-left: max(1rem, env(safe-area-inset-left, 0px));
    padding-right: max(1rem, env(safe-area-inset-right, 0px));
  }}
  .nav-bar a {{ font-size: 0.75rem; padding: 0.3rem 0.6rem; min-height: auto; }}
  .nav-bar::after {{ display: none; }}
  .bond-bank-summary {{ min-height: auto; align-items: baseline; }}
}}

/* \u2500\u2500 Desktop (\u2265 1024px) \u2500\u2500 */
@media (min-width: 1024px) {{
  .table-scroll.wide-min > table {{ min-width: 36rem; }}
  #signals .table-scroll.wide-min > table {{ min-width: 36rem !important; }}
  .table-scroll.table-edge-hint {{ box-shadow: none; }}
  .table-scroll.table-edge-hint::after {{ display: none; }}
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
<strong>Data sources:</strong> yfinance (unofficial){", FRED API (official U.S. macro series)" if macro_data and macro_data.indicators else ""}.
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
    "elevated": "Above-normal — bearish macro signal worth monitoring",
    "info": "Lower severity — milder or contextual signal",
}


def _severity_tag_html(severity: str, display: str | None = None) -> str:
    """Severity badge: color encodes level (red / amber / green); optional tooltip."""
    sev = (severity or "").lower().strip()
    sev_class = {
        "critical": "tag-critical", "warning": "tag-warning",
        "elevated": "tag-elevated", "info": "tag-info",
    }.get(sev, "tag-unknown")
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
<span class="tag tag-warning">warning</span><span>high</span>
<span aria-hidden="true" style="opacity:0.45;">·</span>
<span class="tag tag-elevated">elevated</span><span>above-normal</span>
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
    dow: dict | None,
    nasdaq: dict | None,
    vix: dict | None,
    oil: dict | None,
    risk_trend: RiskTrend | None = None,
) -> str:
    """Grid of KPI summary cards — the executive snapshot before any detail."""
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

    def _index_card(label: str, data: dict | None) -> str | None:
        if not data or not data.get("price"):
            return None
        chg = data.get("change_pct_1d", 0) or 0
        arrow = "&#9650;" if chg >= 0 else "&#9660;"
        chg_color = "var(--green)" if chg >= 0 else "var(--red)"
        return _kpi(label, f"{data['price']:,.0f}", chg_color, f"{arrow} {chg:+.2f}%")

    delta_str = ""
    if risk_trend and risk_trend.delta_1d is not None:
        d = risk_trend.delta_1d
        arrow = "&#9650;" if d > 0 else "&#9660;" if d < 0 else "&#9644;"
        delta_color = "var(--red)" if d > 0 else "var(--green)" if d < 0 else "var(--text-dim)"
        delta_str = f' <span style="font-size:clamp(0.7rem, 2.5vw, 1rem);color:{delta_color};">{arrow}{d:+d}</span>'

    _dir = direction_word(risk_trend.delta_1d if risk_trend else None)
    level_subtitle = display_label(health.overall_risk) + (f" &middot; {_dir}" if _dir else "")
    cards = [_kpi("Risk Score", f"{score_display}{delta_str}", risk_color, level_subtitle)]

    for label, data in [("S&amp;P 500", sp500), ("Dow Jones", dow), ("NASDAQ", nasdaq)]:
        card = _index_card(label, data)
        if card:
            cards.append(card)

    if vix and vix.get("price"):
        vix_val = vix["price"]
        vix_color = "#22c55e" if vix_val < 20 else "#eab308" if vix_val < 30 else "#f97316" if vix_val < 40 else "#ef4444"
        cards.append(_kpi("VIX", f"{vix_val:.1f}", vix_color, "Fear Index"))

    if oil and oil.get("price"):
        oil_color = "#22c55e" if oil["price"] < 80 else "#eab308" if oil["price"] < 100 else "#f97316" if oil["price"] < 130 else "#ef4444"
        cards.append(_kpi("Brent Crude", f"${oil['price']:.2f}", oil_color, "per barrel"))

    kpi_class = "kpi-row kpi-6" if len(cards) >= 5 else "kpi-row"
    return (
        f'<div class="{kpi_class}">'
        + "".join(cards)
        + '</div>'
    )


def _risk_trend_chart_html(
    snapshots: list[dict],
    trend: "RiskTrend | None" = None,
) -> str:
    """Pure inline SVG line chart showing risk score trajectory over time.

    Renders color-coded background bands for risk levels and marks key
    comparison points (today, 1d ago, 1w ago, 1m ago).
    """
    if not snapshots:
        return '<div style="color:var(--text-dim);font-size:0.8rem;">No history data yet — chart will appear after 2+ daily runs.</div>'

    from datetime import date as _date, timedelta

    W, H = 600, 170
    PAD_L, PAD_R, PAD_T, PAD_B = 42, 16, 14, 28

    plot_w = W - PAD_L - PAD_R
    plot_h = H - PAD_T - PAD_B

    scores = [s.get("score_uncapped", s.get("score", 0)) for s in snapshots]
    dates = [s.get("snapshot_date", "") for s in snapshots]

    y_min = 0
    y_max = max(max(scores) * 1.15, 100)

    def _x(i: int) -> float:
        if len(scores) == 1:
            return PAD_L + plot_w / 2
        return PAD_L + (i / (len(scores) - 1)) * plot_w

    def _y(v: float) -> float:
        return PAD_T + plot_h - ((v - y_min) / (y_max - y_min)) * plot_h

    risk_bands = [
        (0, 20, "rgba(34,197,94,0.10)", "Low"),
        (20, 40, "rgba(234,179,8,0.08)", ""),
        (40, 60, "rgba(249,115,22,0.08)", ""),
        (60, 80, "rgba(239,68,68,0.08)", ""),
        (80, 100, "rgba(220,38,38,0.10)", ""),
        (100, 200, "rgba(153,27,27,0.10)", ""),
        (200, 9999, "rgba(127,29,29,0.12)", ""),
    ]
    band_rects = []
    for lo, hi, color, label in risk_bands:
        if lo >= y_max:
            break
        top = _y(min(hi, y_max))
        bot = _y(max(lo, y_min))
        if bot - top < 1:
            continue
        band_rects.append(
            f'<rect x="{PAD_L}" y="{top:.1f}" width="{plot_w}" '
            f'height="{bot - top:.1f}" fill="{color}" />'
        )

    grid_lines = []
    step = 100 if y_max > 400 else 50 if y_max > 150 else 20
    val = step
    while val < y_max:
        yp = _y(val)
        grid_lines.append(
            f'<line x1="{PAD_L}" y1="{yp:.1f}" x2="{W - PAD_R}" y2="{yp:.1f}" '
            f'stroke="rgba(255,255,255,0.07)" stroke-width="0.5" />'
        )
        grid_lines.append(
            f'<text x="{PAD_L - 4}" y="{yp + 3:.1f}" '
            f'fill="rgba(255,255,255,0.35)" font-size="9" text-anchor="end">{int(val)}</text>'
        )
        val += step

    points = [(_x(i), _y(s)) for i, s in enumerate(scores)]
    polyline_pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)

    dots = []
    for x, y in points:
        dots.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.5" fill="#60a5fa" opacity="0.7" />')

    today_str = dates[-1] if dates else ""
    try:
        today_date = _date.fromisoformat(today_str) if today_str else None
    except ValueError:
        today_date = None

    def _find_date_index(target_iso: str) -> int | None:
        for i, d in enumerate(dates):
            if d == target_iso:
                return i
        return None

    def _find_nearest_date_index(target: _date, max_drift: int = 3) -> int | None:
        for offset in range(max_drift + 1):
            idx = _find_date_index((target - timedelta(days=offset)).isoformat())
            if idx is not None:
                return idx
        return None

    markers = []
    marker_labels_below: list[tuple[float, str]] = []

    def _add_marker(idx: int, label: str, color: str, score_val: int, above: bool = True):
        x, y = points[idx]
        markers.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}" '
            f'stroke="white" stroke-width="1.5" />'
        )
        txt_y = y - 10 if above else y + 16
        markers.append(
            f'<text x="{x:.1f}" y="{txt_y:.1f}" fill="white" font-size="10" '
            f'font-weight="600" text-anchor="middle" '
            f'style="text-shadow:0 0 4px rgba(0,0,0,0.8);">{score_val}</text>'
        )
        marker_labels_below.append((x, label))

    _add_marker(len(scores) - 1, "Today", "#22c55e", scores[-1], above=True)

    if today_date and len(scores) > 1:
        d1 = _find_nearest_date_index(today_date - timedelta(days=1))
        if d1 is not None and d1 != len(scores) - 1:
            _add_marker(d1, "1d", "#60a5fa", scores[d1], above=scores[d1] > scores[-1])

        w1 = _find_nearest_date_index(today_date - timedelta(days=7))
        if w1 is not None and w1 != len(scores) - 1 and w1 != d1:
            neighbors = [scores[-1]]
            if d1 is not None:
                neighbors.append(scores[d1])
            _add_marker(w1, "1w", "#f59e0b", scores[w1], above=scores[w1] > max(neighbors))

        m1 = _find_nearest_date_index(today_date - timedelta(days=30))
        if m1 is not None and m1 != len(scores) - 1 and m1 != d1 and m1 != w1:
            _add_marker(m1, "1m", "#a78bfa", scores[m1], above=True)

    date_labels = []
    if len(dates) <= 10:
        shown = set(range(len(dates)))
    else:
        shown = {0, len(dates) - 1}
        for i in range(1, len(dates) - 1):
            if i % max(1, len(dates) // 6) == 0:
                shown.add(i)
    for i in sorted(shown):
        x = _x(i)
        short = dates[i][5:] if len(dates[i]) >= 10 else dates[i]
        date_labels.append(
            f'<text x="{x:.1f}" y="{H - 4:.1f}" fill="rgba(255,255,255,0.4)" '
            f'font-size="9" text-anchor="middle">{short}</text>'
        )

    for x, lbl in marker_labels_below:
        date_labels.append(
            f'<text x="{x:.1f}" y="{H - 14:.1f}" fill="rgba(255,255,255,0.6)" '
            f'font-size="8" font-weight="600" text-anchor="middle">{lbl}</text>'
        )

    svg_parts = [
        f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" '
        f'style="width:100%;height:auto;max-height:180px;font-family:inherit;">',
        "".join(band_rects),
        "".join(grid_lines),
        f'<polyline points="{polyline_pts}" fill="none" stroke="#60a5fa" '
        f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round" />',
        "".join(dots),
        "".join(markers),
        "".join(date_labels),
        "</svg>",
    ]

    return f'<div style="margin:0.5rem 0;">{"".join(svg_parts)}</div>'


def _section_risk_summary(
    health: MarketHealthReport,
    risk_color: str,
    conf_color: str,
    guidance: dict,
    daily_snapshots: list[dict] | None = None,
    risk_trend: "RiskTrend | None" = None,
) -> str:
    uncapped = _health_uncapped_score(health)
    score_display = f"{uncapped}" if uncapped > 100 else f"{health.score}"
    score_label = f"Score (uncapped)" if uncapped > 100 else "Score / 100"
    chart = _risk_trend_chart_html(daily_snapshots or [], risk_trend)
    return f"""
<div class="risk-banner">
  <div>
    <div class="level" style="color: {risk_color}">{display_label(health.overall_risk)}</div>
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
    {chart}
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


def _baseline_day_label(iso_date: str | None) -> str:
    """Human-friendly label for the 1d baseline date: 'yesterday', 'Friday', etc."""
    if not iso_date:
        return "yesterday"
    try:
        from datetime import date as _date
        baseline = _date.fromisoformat(iso_date)
        today = datetime.now(ZoneInfo("America/New_York")).date()
        diff = (today - baseline).days
        if diff <= 1:
            return "yesterday"
        return baseline.strftime("%A")  # "Friday", "Thursday", etc.
    except (ValueError, TypeError):
        return "yesterday"


def _snapshot_narrative(health: MarketHealthReport, trend: RiskTrend | None) -> str:
    """One plain-English sentence summarising today's score, movement, and a link to history."""
    uncapped = _health_uncapped_score(health)
    parts: list[str] = [f"Today&rsquo;s risk score is <strong>{uncapped}</strong> (uncapped raw total)."]

    if trend:
        movements: list[str] = []
        if trend.delta_1d is not None:
            direction = "up" if trend.delta_1d > 0 else "down" if trend.delta_1d < 0 else "flat"
            day_label = _baseline_day_label(trend.prev_1d_date)
            movements.append(f"<strong>{direction} {abs(trend.delta_1d)} points</strong> from {day_label}")
        if trend.delta_1w is not None:
            direction = "up" if trend.delta_1w > 0 else "down" if trend.delta_1w < 0 else "flat"
            movements.append(f"{direction} {abs(trend.delta_1w)} over the past week")
        if movements:
            parts.append("That&rsquo;s " + ", and ".join(movements) + ".")

    parts.append(
        'For how this compares to past economic crises, see '
        '<a href="#historical" style="color:var(--cyan);">Historical Parallels</a> below.'
    )
    return (
        '<p style="font-size:0.85rem;color:var(--text-dim);line-height:1.55;'
        'margin:0.65rem 0 0.3rem;">'
        + " ".join(parts)
        + "</p>"
    )


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


def _section_projection(proj: object) -> str:
    """Render the forward-looking risk projection chip."""
    direction = getattr(proj, 'direction', 'stable')
    confidence = getattr(proj, 'confidence', 0.0)
    factors = getattr(proj, 'factors', [])
    color = getattr(proj, 'color_var', 'var(--text-dim)')
    label = getattr(proj, 'label', 'STABLE')

    icon = {"worsening": "&#9650;", "stable": "&#9644;", "improving": "&#9660;"}.get(direction, "&#9644;")
    conf_pct = f"{confidence:.0%}"

    factor_html = ""
    if factors:
        items = "".join(f"<li>{escape(f)}</li>" for f in factors[:5])
        factor_html = f"<ul style='margin:0.4rem 0 0 1rem;padding:0;font-size:0.78rem;color:var(--text-dim);'>{items}</ul>"

    return (
        f'<div style="margin:0.75rem 0 0.5rem;padding:0.65rem 0.85rem;'
        f'background:var(--surface);border:1px solid var(--border);border-radius:0.6rem;">'
        f'<div style="display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap;">'
        f'<span style="font-size:0.82rem;color:var(--text-dim);">Forward outlook:</span>'
        f'<span style="font-size:1rem;color:{color};font-weight:700;">{icon} {label}</span>'
        f'<span style="font-size:0.72rem;color:var(--text-dim);">(confidence {conf_pct})</span>'
        f'</div>'
        f'{factor_html}'
        f'</div>'
    )


def _section_risk_score_reader_context(health: MarketHealthReport, risk_trend: RiskTrend | None = None) -> str:
    """Plain-language explanation: today\'s score in context, methodology, named level, history."""
    uncapped = _health_uncapped_score(health)
    capped = health.score
    level = display_label(health.overall_risk)

    movement_lines: list[str] = []
    if risk_trend:
        if risk_trend.delta_1d is not None:
            prev = uncapped - risk_trend.delta_1d
            direction = "up" if risk_trend.delta_1d > 0 else "down" if risk_trend.delta_1d < 0 else "unchanged"
            movement_lines.append(
                f"Yesterday&rsquo;s snapshot was <strong>{prev}</strong> "
                f"({direction} <strong>{abs(risk_trend.delta_1d)}</strong> points)."
            )
        if risk_trend.delta_1w is not None:
            prev_w = uncapped - risk_trend.delta_1w
            movement_lines.append(f"One week ago it was <strong>{prev_w}</strong>.")
        if risk_trend.delta_1m is not None:
            prev_m = uncapped - risk_trend.delta_1m
            movement_lines.append(f"One month ago it was <strong>{prev_m}</strong>.")
    movement_html = " ".join(movement_lines) if movement_lines else "No prior snapshot is available yet for comparison."

    return f"""
<div id="risk-explainer" class="card" style="border-left:3px solid var(--cyan);margin-bottom:1.25rem;">
<h3 style="margin:0 0 0.5rem 0;font-size:0.95rem;color:var(--text);">Understanding this risk score</h3>
<div style="font-size:0.82rem;color:var(--text-dim);line-height:1.6;">
<p style="margin:0 0 0.65rem 0;">
<strong style="color:var(--text);">1. Where the score is now</strong> &mdash;
Today&rsquo;s raw risk score is <strong>{uncapped}</strong>.
{movement_html}
The <strong>score number and its direction</strong> are more informative than the named level when conditions stay at the top of the scale.
</p>
<p style="margin:0 0 0.65rem 0;">
<strong style="color:var(--text);">2. How we calculate it</strong> &mdash;
Each detected signal adds <strong>points</strong> (leading macro/fundamental signals are weighted <strong>1.5&times;</strong> vs lagging technical signals).
Signals span VIX, drawdowns, death crosses, macro (FRED), fundamentals, and breadth.
The <strong>raw total</strong> is <strong>{uncapped}</strong>; we also show a <strong>0&ndash;100 capped</strong> score ({capped}) for side-by-side comparison.
</p>
<p style="margin:0 0 0.65rem 0;">
<strong style="color:var(--text);">3. What the named level means</strong> &mdash;
The level <strong style="color:var(--text);">{level}</strong> maps the raw score to a bucket on this model&rsquo;s scale
(Low&nbsp;&rarr;&nbsp;Moderate&nbsp;&rarr;&nbsp;Elevated&nbsp;&rarr;&nbsp;High&nbsp;&rarr;&nbsp;Critical&nbsp;&rarr;&nbsp;Severe&nbsp;&rarr;&nbsp;Extreme&nbsp;&rarr;&nbsp;Catastrophic).
It tells you <strong>how many signals are stacking</strong>, not what will happen next.
It is a <strong>model severity label</strong>, not a forecast, not a bank rating, and not personalized advice.
</p>
<p style="margin:0;">
<strong style="color:var(--text);">4. Snapshots and history</strong> &mdash;
Each weekday build saves a daily snapshot.
The <strong>trend chips</strong> above show how today&rsquo;s score compares to prior snapshots (1&nbsp;day, 1&nbsp;week, 1&nbsp;month).
History accumulates automatically across builds so you can track direction over time.
</p>
</div>
</div>
"""


def _section_risk_legend(health: MarketHealthReport) -> str:
    active_slug = health.overall_risk

    levels = [
        ("heavy_stress", "HEAVY STRESS", "raw sum 200+", "#b91c1c",
         "Deep signal convergence &mdash; many rules firing across all layers "
         "(technical + macro + fundamentals). This measures <strong>stacked stress in this model</strong>; "
         "it is <strong>not</strong> a prediction of collapse and <strong>not</strong> financial advice."),
        ("severe_stress", "SEVERE STRESS", "150–199", "#7f1d1d",
         "Broad stress across financial, commodity, and macro dimensions. "
         "Maximum defensive posture: cash, short-duration treasuries, zero equity exposure."),
        ("compounding_stress", "COMPOUNDING STRESS", "100–149", "#991b1b",
         "More signals firing than the baseline anticipated &mdash; conditions are layering. "
         "The score exceeds 100 because risk signals compound beyond the original scale."),
        ("acute_stress", "ACUTE STRESS", "80–99", "#dc2626",
         "Multiple severe risk signals stacking simultaneously. Defensive posture &mdash; "
         "avoid new positions, prioritize capital preservation, increase cash allocation."),
        ("high", "HIGH", "60–79", "#ef4444",
         "Significant risk signals across multiple categories. Reduce exposure to risk assets, "
         "tighten stop-losses, and size any new positions very small (0.5–1% max)."),
        ("elevated", "ELEVATED", "40–59", "#f97316",
         "Several warning signals are active. Proceed with caution, favor quality over speculation, "
         "and keep position sizes moderate (1–3%)."),
        ("moderate", "MODERATE", "20–39", "#eab308",
         "A few risk signals are present but the market is broadly functional. "
         "Normal investing with standard position sizing (3–5%)."),
        ("low", "LOW", "0–19", "#22c55e",
         "Minimal risk signals detected. Market conditions are calm. Standard investing with full position sizing."),
    ]

    level_rows = []
    for slug, name, score_range, color, description in levels:
        is_active = slug == active_slug
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
<tr><td><span style="color:#d97706;font-weight:600;">Elevated</span></td>
<td style="text-align:right;padding-right:1.5rem;">+5 pts</td>
<td style="text-align:right;font-weight:600;">+7 pts</td></tr>
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

    inflation_cats = {"inflation_components", "inflation_expectations"}
    rows: list[str] = []
    for cat in category_order:
        _append_category_block(cat, by_cat.pop(cat, None) or [])
    for cat in sorted(by_cat.keys()):
        if cat in inflation_cats:
            continue
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
<thead><tr><th>Indicator</th><th style="text-align:right;width:4.5rem;">Value</th><th class="col-m-hide" style="text-align:right">Change</th><th style="width:3.5rem;">Signal</th><th class="col-m-hide">Assessment</th></tr></thead>
<tbody>{"".join(rows)}</tbody>
</table>
</div>
{alerts}""",
        section_id="macro",
    )


def _section_inflation(macro_data: MacroSnapshot) -> str:
    """Dedicated consumer inflation breakdown: CPI components + expectations."""
    sig_cls = {"critical": "tag-critical", "warning": "tag-warning", "bearish": "tag-warning",
               "bullish": "tag-strong", "neutral": "tag-info"}

    component_ids = {
        "CUSR0000SAF11", "CUSR0000SEFV", "CUSR0000SAH1", "CUSR0000SEHA",
        "CPIENGSL", "CPIMEDSL", "CUSR0000SETA02", "CPILFESL",
    }
    expectation_ids = {
        "T10YIEM", "MICH", "MEDCPIM158SFRBCLE", "PCETRIM12M159SFRBDAL",
    }

    components = [ind for ind in macro_data.indicators if ind.series_id in component_ids]
    expectations = [ind for ind in macro_data.indicators if ind.series_id in expectation_ids]
    headline = next((ind for ind in macro_data.indicators if ind.series_id == "CPIAUCSL"), None)

    if not components and not expectations:
        return ""

    # -- Headline vs Core gauge --
    core = next((ind for ind in components if ind.series_id == "CPILFESL"), None)
    gauge_html = ""
    if headline and core and headline.yoy_change is not None and core.yoy_change is not None:
        h_yoy = headline.yoy_change
        c_yoy = core.yoy_change
        h_color = "var(--red)" if h_yoy > 4 else "var(--yellow)" if h_yoy > 3 else "var(--green)"
        c_color = "var(--red)" if c_yoy > 4 else "var(--yellow)" if c_yoy > 3 else "var(--green)"
        gauge_html = (
            '<div style="display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:0.75rem;">'
            f'<div class="card" style="flex:1;min-width:140px;text-align:center;">'
            f'<div style="font-size:0.75rem;color:var(--text-dim);">Headline CPI (YoY)</div>'
            f'<div style="font-size:1.6rem;font-weight:700;color:{h_color};">{h_yoy:+.1f}%</div></div>'
            f'<div class="card" style="flex:1;min-width:140px;text-align:center;">'
            f'<div style="font-size:0.75rem;color:var(--text-dim);">Core CPI (YoY)</div>'
            f'<div style="font-size:1.6rem;font-weight:700;color:{c_color};">{c_yoy:+.1f}%</div></div>'
            '</div>'
        )

    # -- Component table --
    comp_rows: list[str] = []
    for ind in components:
        signal_class = sig_cls.get(ind.signal, "tag-info")
        yoy_str = f"{ind.yoy_change:+.1f}%" if ind.yoy_change is not None else "—"
        yoy_extra = ""
        if ind.yoy_change is not None:
            if ind.yoy_change > 4:
                yoy_extra = "color:var(--red);font-weight:600;"
            elif ind.yoy_change > 3:
                yoy_extra = "color:var(--yellow);"
        change_str = f"{ind.change:+,.2f}" if ind.change is not None else "—"
        comp_rows.append(
            f"<tr><td><strong>{escape(ind.name)}</strong></td>"
            f"<td style='text-align:right;white-space:nowrap;'>{ind.value:,.2f}</td>"
            f"<td style='text-align:right;white-space:nowrap;{yoy_extra}'>{yoy_str}</td>"
            f"<td class='col-m-hide' style='text-align:right'>{change_str}</td>"
            f"<td style='white-space:nowrap;'><span class='tag {signal_class}'>{ind.signal}</span></td>"
            f"<td class='col-m-hide' style='color:var(--text-dim)'>{escape(ind.description)}</td></tr>"
        )

    comp_html = ""
    if comp_rows:
        comp_html = (
            '<div class="card table-scroll wide-min sticky-first-col table-edge-hint">'
            '<table><thead><tr>'
            '<th>Component</th><th style="text-align:right;width:5.5rem;">Index</th>'
            '<th style="text-align:right;width:5rem;">YoY%</th>'
            '<th class="col-m-hide" style="text-align:right">MoM Chg</th>'
            '<th style="width:4.5rem;">Signal</th><th class="col-m-hide">Assessment</th>'
            f'</tr></thead><tbody>{"".join(comp_rows)}</tbody></table></div>'
        )

    # -- Expectations sub-table --
    exp_rows: list[str] = []
    for ind in expectations:
        signal_class = sig_cls.get(ind.signal, "tag-info")
        exp_rows.append(
            f"<tr><td><strong>{escape(ind.name)}</strong></td>"
            f"<td style='text-align:right;white-space:nowrap;'>{ind.value:.2f}%</td>"
            f"<td style='white-space:nowrap;'><span class='tag {signal_class}'>{ind.signal}</span></td>"
            f"<td class='col-m-hide' style='color:var(--text-dim)'>{escape(ind.description)}</td></tr>"
        )

    exp_html = ""
    if exp_rows:
        exp_html = (
            '<div style="margin-top:0.75rem;">'
            '<div style="font-size:0.8rem;font-weight:600;color:var(--text);margin-bottom:0.35rem;">'
            'Inflation Expectations &amp; Alternative Measures</div>'
            '<div class="card table-scroll wide-min sticky-first-col table-edge-hint">'
            '<table><thead><tr>'
            '<th>Measure</th><th style="text-align:right;width:5rem;">Value</th>'
            '<th style="width:4.5rem;">Signal</th><th class="col-m-hide">Assessment</th>'
            f'</tr></thead><tbody>{"".join(exp_rows)}</tbody></table></div></div>'
        )

    # -- Summary tag --
    all_inds = components + expectations
    hot_count = sum(1 for ind in all_inds if ind.signal in ("critical", "warning"))
    if hot_count:
        summary = f' — <span style="color:var(--yellow);">{hot_count} elevated</span>'
    else:
        summary = ' — <span style="color:var(--green);">contained</span>'

    methodology = (
        '<div style="font-size:0.8rem;color:var(--text-dim);line-height:1.45;margin-bottom:0.75rem;'
        'padding:0.55rem 0.65rem;background:var(--surface2);border-radius:0.5rem;border:1px solid var(--border);">'
        '<strong style="color:var(--text);">Where inflation hits hardest.</strong> '
        'CPI components reveal which categories are driving headline inflation. '
        'Shelter (~36% of CPI) is the stickiest; food and energy are the most volatile. '
        'Expectations measures show whether inflation is becoming self-reinforcing. '
        'YoY% is computed from the 12-month FRED data window. All data from official BLS/Fed sources.'
        '</div>'
    )

    return _collapsible(
        f"Consumer Inflation Breakdown{summary}",
        methodology + gauge_html + comp_html + exp_html,
        section_id="inflation",
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
        "high": "var(--red)", "acute_stress": "var(--red)",
        "compounding_stress": "var(--red)", "severe_stress": "var(--red)", "heavy_stress": "var(--red)",
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
Market risk is currently <strong style="color:{market_risk_color};">{display_label(health.overall_risk)}</strong>
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


def _factor_chip(factor: str, is_match: bool = False) -> str:
    """Render a single factor as a colored chip."""
    colors = {
        "commodity_shock": "#f97316", "geopolitical": "#ef4444", "stagflation": "#dc2626",
        "banking_credit": "#a855f7", "speculation_leverage": "#eab308", "fed_policy": "#3b82f6",
        "trade_war": "#6366f1", "supply_chain": "#f59e0b", "external_shock": "#06b6d4",
        "structural_market": "#8b5cf6",
    }
    color = colors.get(factor, "var(--text-dim)")
    label = FACTOR_LABELS.get(factor, factor)
    border = f"border:1px solid {color};"
    bg = f"background:{color}22;" if is_match else "background:var(--surface);"
    return (
        f'<span style="display:inline-block;{bg}{border}color:{color};'
        f'border-radius:0.35rem;padding:0.15rem 0.45rem;font-size:0.72rem;'
        f'font-weight:600;white-space:nowrap;">{escape(label)}</span>'
    )


def _section_historical_parallels(
    sp500_price: float | None,
    macro_data: object | None = None,
    cascade_active_count: int = 0,
    bottom_estimate: object | None = None,
) -> str:
    """Public-safe section comparing current situation to historical crashes."""
    peak = 6900
    all_crashes = get_all_crashes(sp500_price, macro_data, cascade_active_count)
    current_event = next((c for c in all_crashes if c.name.startswith("2026")), None)
    current_factors = current_event.crisis_factors if current_event else set()
    if current_event:
        decline_pct = current_event.decline_pct
    elif sp500_price:
        decline_pct = ((sp500_price - peak) / peak) * 100
    else:
        decline_pct = -7.5

    rows = []
    for crash in all_crashes:
        if crash.name.startswith("2026"):
            continue
        rec = f"{crash.months_to_recovery:.0f} months" if crash.months_to_recovery else "—"
        overlap = crash.crisis_factors & current_factors
        overlap_count = len(overlap)
        total_current = len(current_factors) if current_factors else 1
        if overlap_count >= 3:
            match_color = "var(--red)"
        elif overlap_count >= 2:
            match_color = "var(--orange)"
        elif overlap_count >= 1:
            match_color = "var(--yellow)"
        else:
            match_color = "var(--text-dim)"
        match_cell = f'<span style="color:{match_color};font-weight:600;">{overlap_count}/{total_current}</span>'
        rows.append(
            f"<tr><td>{escape(crash.name)}</td>"
            f"<td style='text-align:right;color:var(--red);'>{crash.decline_pct:.1f}%</td>"
            f"<td style='text-align:right;'>{crash.days_to_bottom}</td>"
            f"<td style='text-align:right;'>{rec}</td>"
            f"<td style='text-align:center;'>{match_cell}</td></tr>"
        )

    current_bar_pct = min(abs(decline_pct) / 90 * 100, 100)
    comparison = crash_comparison_for_dashboard(
        sp500_price or (peak * (1 + decline_pct / 100)), peak,
        macro=macro_data, cascade_active_count=cascade_active_count,
    )
    best_match = comparison.get("best_match")
    match_name = best_match.name if best_match else "1973-74 Oil Crisis"
    best_overlap = len(best_match.crisis_factors & current_factors) if best_match else 0
    total_f = len(current_factors) if current_factors else 0

    current_chips = " ".join(_factor_chip(f) for f in sorted(current_factors))
    dna_rows = []
    top_matches = comparison.get("similar_crashes", [])[:3]
    for crash in top_matches:
        overlap = crash.crisis_factors & current_factors
        chips = " ".join(
            _factor_chip(f, is_match=(f in overlap)) for f in sorted(crash.crisis_factors)
        )
        ol = len(overlap)
        dna_rows.append(
            f'<div style="display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap;">'
            f'<span style="min-width:10rem;font-size:0.82rem;color:var(--text);">{escape(crash.name)}</span>'
            f'<span style="font-size:0.75rem;color:var(--text-dim);">({ol}/{total_f})</span>'
            f'{chips}</div>'
        )

    crisis_dna_html = f"""<div style="margin-bottom:1.25rem;padding:0.75rem;background:var(--surface);border:1px solid var(--border);border-radius:0.6rem;">
<div style="font-size:0.85rem;font-weight:600;color:var(--text);margin-bottom:0.5rem;">Crisis DNA — 2026 active factors <span style="font-size:0.72rem;color:var(--text-dim);font-weight:400;">(inferred from live data each run)</span></div>
<div style="display:flex;flex-wrap:wrap;gap:0.35rem;margin-bottom:0.75rem;">{current_chips}</div>
<div style="font-size:0.78rem;color:var(--text-dim);margin-bottom:0.5rem;">Top historical matches by factor overlap:</div>
<div style="display:flex;flex-direction:column;gap:0.4rem;">{"".join(dna_rows)}</div>
</div>"""

    bottom_html = ""
    if bottom_estimate and hasattr(bottom_estimate, 'base_decline'):
        be = bottom_estimate
        analogs_text = ", ".join(be.analogs_used[:3]) if be.analogs_used else "insufficient data"

        def _zone(color: str, label: str, decline: float, level: float, days: int, is_now: bool = False) -> str:
            border_top = f"border-top:3px solid {color};"
            opacity = "opacity:0.85;" if not is_now and label != "Base Case" else ""
            size_cls = "estimate-val-lg" if label == "Base Case" else "estimate-val"
            days_label = "today" if is_now else f"~{days} days"
            return (
                f'<div class="estimate-zone" style="background:var(--surface);'
                f'{border_top}{opacity}">'
                f'<div style="font-size:0.7rem;font-weight:600;color:{color};text-transform:uppercase;'
                f'letter-spacing:0.04em;margin-bottom:0.3rem;">{escape(label)}</div>'
                f'<div class="{size_cls}" style="font-weight:700;color:{color};">{decline:.1f}%</div>'
                f'<div style="font-size:0.82rem;color:var(--text);margin-top:0.15rem;">S&amp;P ~{level:,.0f}</div>'
                f'<div style="font-size:0.72rem;color:var(--text-dim);margin-top:0.15rem;">{days_label}</div>'
                f'</div>'
            )

        zones = [
            _zone("var(--cyan)", "Now", be.current_decline_pct, be.current_level, 0, is_now=True),
            _zone("var(--green)", "Optimistic", be.optimistic_decline, be.optimistic_level, be.optimistic_days),
            _zone("#eab308", "Base Case", be.base_decline, be.base_level, be.base_days),
            _zone("var(--red)", "Pessimistic", be.pessimistic_decline, be.pessimistic_level, be.pessimistic_days),
        ]

        arrow = '<div class="estimate-arrow">&#9654;</div>'

        bottom_html = f"""<div style="margin-bottom:1.25rem;padding:0.75rem;background:var(--surface);border:1px solid var(--border);border-radius:0.6rem;">
<div style="font-size:0.85rem;font-weight:600;color:var(--text);margin-bottom:0.6rem;">2026 Bottom Estimate <span style="font-size:0.72rem;color:var(--text-dim);font-weight:400;">(analog-weighted from factor overlap)</span></div>
<div class="estimate-row">
{arrow.join(zones)}
</div>
<div style="font-size:0.72rem;color:var(--text-dim);margin-top:0.6rem;line-height:1.4;">
Based on: {escape(analogs_text)}. Each analog weighted by crisis factor overlap (confidence: {be.confidence:.0%}).
This is not a prediction — it shows where similar historical crises ended.
</div>
</div>"""

    return _collapsible(
        f'Crisis Context: Historical Parallels — {decline_pct:+.1f}%<span class="section-detail">, strongest overlap: {escape(match_name)} ({best_overlap}/{total_f} factors)</span>',
        f"""<div class="card">
{crisis_dna_html}
{bottom_html}
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
<thead><tr><th>Crash</th><th style="text-align:right">Decline</th><th style="text-align:right">Days to Bottom</th><th style="text-align:right">Recovery</th><th style="text-align:center">Match</th></tr></thead>
<tbody>
{"".join(rows)}
</tbody>
</table>
</div>
<div style="margin-top:0.75rem;font-size:0.78rem;color:var(--text-dim);line-height:1.5;">
<strong>How matching works:</strong> Each crash is tagged with causal factors (commodity shock, geopolitical, etc.) based on
historical sources (IMF, NBER, Federal Reserve History). The "Match" column shows how many of 2026's active factors
overlap with each historical crash. Rankings update dynamically as conditions change.
</div>
<div style="margin-top:0.75rem;font-size:0.85rem;color:var(--text-dim);line-height:1.6;">
<strong>Key finding:</strong> Across 8 major US crashes (1907-2020), the market recovered every time.
The only crash where early withdrawal would have been correct was 1929 — under conditions (no FDIC, no SEC, no Fed backstop)
that cannot recur in the modern financial system. Average oil-shock recovery: {comparison.get("avg_oil_crash_recovery_months", 0):.0f} months.
</div>
</div>""",
        section_id="historical",
    )


def _section_supply_chain(cascade_stages: list | None = None) -> str:
    """Public-safe supply chain risk monitor — driven by live data when available."""
    from datetime import date as _date

    has_anchored_dates = (
        cascade_stages
        and any(getattr(s, "date_range_start", None) for s in cascade_stages)
    )

    if cascade_stages:
        stage_rows = []
        for stage in cascade_stages:
            coverage = f"{stage.inputs_received}/{stage.inputs_expected}" if hasattr(stage, "inputs_expected") else ""
            stress_pct = f"{stage.stress_score:.0%}" if hasattr(stage, "stress_score") else f"{stage.confidence:.0%}"

            # Model-vs-actual annotations
            timeline_note = ""
            if has_anchored_dates and hasattr(stage, "model_should_be_active"):
                if stage.model_should_be_active and stage.status == "not_started":
                    timeline_note = '<div style="font-size:0.7rem;color:var(--orange);margin-top:0.2rem;">Model expects activity in this window</div>'
                elif stage.status == "active" and not stage.model_should_be_active:
                    if stage.date_range_start and _date.today() < stage.date_range_start:
                        timeline_note = '<div style="font-size:0.7rem;color:var(--cyan);margin-top:0.2rem;">Ahead of model timeline</div>'
                    elif stage.date_range_end and _date.today() > stage.date_range_end:
                        timeline_note = '<div style="font-size:0.7rem;color:var(--text-dim);margin-top:0.2rem;">Past model window — still active</div>'

            first_activated = ""
            if hasattr(stage, "first_activated_date") and stage.first_activated_date:
                first_activated = (
                    f'<div style="font-size:0.65rem;color:var(--text-dim);margin-top:0.15rem;">'
                    f'First activated: {stage.first_activated_date.strftime("%b %-d, %Y")}</div>'
                )

            if stage.status == "active":
                status_html = f'<span style="color:var(--red);font-weight:600;">ACTIVE</span>'
                meta_html = f' <span style="font-size:0.7rem;color:var(--text-dim);">(stress {stress_pct}, data {coverage})</span>'
                bg = "background:rgba(239,68,68,0.08);"
            elif stage.status == "projected":
                status_html = '<span style="color:var(--yellow);font-weight:600;">Projected</span>'
                meta_html = f' <span style="font-size:0.7rem;color:var(--text-dim);">(stress {stress_pct}, data {coverage})</span>' if stage.confidence >= 0.2 else ''
                bg = "background:rgba(234,179,8,0.05);"
            else:
                status_html = '<span style="color:var(--text-dim);">—</span>'
                meta_html = f' <span style="font-size:0.65rem;color:var(--text-dim);">({coverage})</span>' if coverage else ''
                bg = ""

            evidence_html = ""
            if stage.evidence:
                evidence_items = "".join(f"<li>{escape(e)}</li>" for e in stage.evidence[:5])
                evidence_html = f"<ul style='margin:0.25rem 0 0 1rem;padding:0;font-size:0.75rem;color:var(--text-dim);'>{evidence_items}</ul>"

            stage_rows.append(
                f'<tr style="{bg}"><td style="white-space:nowrap;font-weight:600;">{escape(stage.timeframe)}</td>'
                f"<td><strong>{escape(stage.name)}</strong><br>"
                f"<span style='font-size:0.8rem;color:var(--text-dim);'>{escape(stage.description)}</span>"
                f"{evidence_html}{timeline_note}{first_activated}</td>"
                f"<td style='white-space:nowrap;'>{status_html}{meta_html}</td></tr>"
            )
    else:
        stage_rows = [
            '<tr><td colspan="3" style="text-align:center;color:var(--text-dim);padding:1.5rem;">Supply chain data unavailable this run.</td></tr>'
        ]

    active_count = sum(1 for s in (cascade_stages or []) if s.status == "active")
    summary = ""
    if active_count >= 3:
        summary = '<div style="padding:0.5rem 0.75rem;background:rgba(239,68,68,0.12);border-radius:0.5rem;margin-bottom:1rem;font-size:0.85rem;"><strong style="color:var(--red);">Broad cascade underway</strong> — {n} of {t} stages active. Supply chain stress is spreading across sectors.</div>'.format(n=active_count, t=len(cascade_stages or []))
    elif active_count >= 2:
        summary = '<div style="padding:0.5rem 0.75rem;background:rgba(234,179,8,0.1);border-radius:0.5rem;margin-bottom:1rem;font-size:0.85rem;"><strong style="color:var(--yellow);">Cascade building</strong> — {n} stages active. Watch for downstream activation.</div>'.format(n=active_count)

    # Timeline mode badge
    elapsed_badge = ""
    if has_anchored_dates and cascade_stages:
        first_stage = cascade_stages[0]
        if first_stage.date_range_start:
            crisis_start = first_stage.date_range_start
            elapsed = (_date.today() - crisis_start).days
            elapsed_badge = (
                f'<div style="padding:0.4rem 0.75rem;background:rgba(6,182,212,0.1);border-radius:0.5rem;'
                f'margin-bottom:0.75rem;font-size:0.8rem;color:var(--cyan);">'
                f'Day {elapsed} of crisis (since {crisis_start.strftime("%b %-d, %Y")})'
                f'</div>'
            )
    timeline_note_text = ""
    if not has_anchored_dates:
        timeline_note_text = (
            ' <span style="font-size:0.75rem;color:var(--text-dim);">'
            '(Timelines are hypothetical — no active Strait disruption anchored)</span>'
        )

    return _collapsible(
        'Crisis Context: Supply Chain Cascade<span class="section-detail"> — Strait of Hormuz</span>',
        f"""<div class="card">
{summary}{elapsed_badge}
<div style="font-size:0.85rem;color:var(--text-dim);margin-bottom:1rem;line-height:1.5;">
The Strait of Hormuz carries ~21% of global oil, ~25% of global LNG, and hosts the world's largest helium processing
facility at Ras Laffan, Qatar. Disruption creates a cascading timeline of impacts far beyond oil prices.
Statuses are evaluated from live market data each run.{timeline_note_text}
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
