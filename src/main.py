"""Financial Agent CLI — Market trends and collapse risk analysis.

Usage:
    python -m src init              # Initialize database
    python -m src scan              # Quick market scan (data + risk check, no AI)
    python -m src analyze           # Full AI-powered analysis with all data layers
    python -m src risk              # Risk assessment only
    python -m src predictions       # View prediction tracker
"""

import argparse
from datetime import datetime

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table

from .analysis.accuracy import check_predictions, display_predictions
from .analysis.ai_analyst import analyze_market_trends
from .analysis.memory import build_trend_context
from .analysis.risk import MarketHealthReport, assess_market_health, get_position_guidance
from .config import load_config
from .data.crypto import fetch_crypto_data
from .data.database import get_session, init_db
from .data.forex import fetch_forex_data
from .data.fundamentals import fetch_fundamentals_batch
from .data.macro import fetch_macro_data
from .data.models import Alert, AnalysisReport, MarketSnapshot
from .data.stocks import fetch_market_indices, fetch_multiple
from .report import generate_report

console = Console()

LIMITATIONS_FOOTER = (
    "[dim]Limitations: Cannot predict black swan events. Correlations may break down in crises. "
    "Free data sources may have delays. This is analysis, not financial advice.[/dim]"
)


def collect_all_data(config: dict) -> dict:
    watchlist = config.get("watchlist", {})

    console.print("\n[bold]Fetching Market Indices...[/bold]")
    indices = fetch_market_indices(config.get("market_indices", []))

    console.print("\n[bold]Fetching Stocks...[/bold]")
    stocks = fetch_multiple(watchlist.get("stocks", []), asset_type="stock")

    console.print("\n[bold]Fetching ETFs...[/bold]")
    etfs = fetch_multiple(watchlist.get("etfs", []), asset_type="etf")

    crypto = []
    if watchlist.get("crypto"):
        console.print("\n[bold]Fetching Crypto...[/bold]")
        crypto = fetch_crypto_data(watchlist["crypto"])

    console.print("\n[bold]Fetching Forex...[/bold]")
    forex = fetch_forex_data(watchlist.get("forex", []))

    return {
        "indices": indices,
        "stocks": stocks,
        "etfs": etfs,
        "crypto": crypto,
        "forex": forex,
    }


def save_snapshots(market_data: dict, session):
    for category in ["indices", "stocks", "etfs", "crypto", "forex"]:
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


def save_alerts(health: MarketHealthReport, session):
    for signal in health.signals:
        if signal.severity in ("warning", "critical"):
            alert = Alert(
                alert_type=signal.name.lower().replace(" ", "_"),
                severity=signal.severity,
                ticker=signal.ticker,
                message=signal.message,
            )
            session.add(alert)
    session.commit()


def display_market_overview(market_data: dict):
    table = Table(title="Market Overview", show_header=True, header_style="bold cyan")
    table.add_column("Ticker", style="bold")
    table.add_column("Price", justify="right")
    table.add_column("1D %", justify="right")
    table.add_column("1W %", justify="right")
    table.add_column("1M %", justify="right")
    table.add_column("RSI", justify="right")
    table.add_column("Signal", justify="center")

    for category in ["indices", "stocks", "etfs", "crypto", "forex"]:
        items = market_data.get(category, [])
        if items:
            table.add_section()
            for item in items:
                change_1d = item.get("change_pct_1d")
                change_1w = item.get("change_pct_1w")
                change_1m = item.get("change_pct_1m")
                rsi = item.get("rsi_14")

                table.add_row(
                    item["ticker"],
                    f"${item['price']:,.2f}" if item.get("price") else "N/A",
                    f"[{_change_style(change_1d)}]{change_1d:+.2f}%[/{_change_style(change_1d)}]" if change_1d is not None else "—",
                    f"[{_change_style(change_1w)}]{change_1w:+.2f}%[/{_change_style(change_1w)}]" if change_1w is not None else "—",
                    f"[{_change_style(change_1m)}]{change_1m:+.2f}%[/{_change_style(change_1m)}]" if change_1m is not None else "—",
                    f"{rsi:.0f}" if rsi else "—",
                    _get_signal_indicator(item),
                )

    console.print(table)


def display_risk_report(health: MarketHealthReport):
    risk_colors = {
        "low": "green", "moderate": "yellow", "elevated": "dark_orange",
        "high": "red", "critical": "bold red",
    }
    color = risk_colors.get(health.overall_risk, "white")
    conf_colors = {"high": "green", "medium": "yellow", "low": "red"}
    conf_color = conf_colors.get(health.confidence, "white")

    console.print(Panel(
        f"[{color}]Risk Level: {health.overall_risk.upper()}[/{color}]  |  "
        f"Score: {health.score}/100  |  "
        f"Confidence: [{conf_color}]{health.confidence.upper()}[/{conf_color}]  |  "
        f"Critical: {health.critical_count}  |  Warnings: {health.warning_count}  |  "
        f"Leading Signals: {health.leading_signal_count}",
        title="Market Health Assessment",
        border_style=color,
    ))

    if health.data_sources_missing:
        console.print(
            f"[dim]Data gaps: {', '.join(health.data_sources_missing)} — "
            f"confidence limited by missing data[/dim]"
        )

    guidance = get_position_guidance(health.overall_risk)
    console.print(
        f"[dim]Position guidance at {health.overall_risk.upper()} risk: "
        f"Max new position: {guidance['max_position']} | "
        f"Stop-loss: {guidance['stop_loss']}[/dim]"
    )

    if health.signals:
        table = Table(show_header=True, header_style="bold")
        table.add_column("Sev", style="bold", width=8)
        table.add_column("Type", width=7)
        table.add_column("Cat", width=12)
        table.add_column("Signal")
        table.add_column("Detail")

        for signal in sorted(health.signals, key=lambda s: (
            {"critical": 0, "warning": 1, "info": 2}.get(s.severity, 3),
            0 if s.signal_type == "leading" else 1,
        )):
            sev_style = {"critical": "bold red", "warning": "yellow", "info": "dim"}.get(signal.severity, "white")
            type_label = "[bold cyan]LEAD[/bold cyan]" if signal.signal_type == "leading" else "[dim]lag[/dim]"
            table.add_row(
                f"[{sev_style}]{signal.severity.upper()}[/{sev_style}]",
                type_label,
                signal.category,
                signal.name,
                signal.message,
            )

        console.print(table)


def display_macro_summary(macro_data):
    if not macro_data:
        return
    console.print("\n[bold]Macroeconomic Indicators (FRED)[/bold]")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Indicator")
    table.add_column("Value", justify="right")
    table.add_column("Change", justify="right")
    table.add_column("Signal", justify="center")
    table.add_column("Assessment")

    signal_styles = {
        "critical": "bold red", "warning": "yellow", "bearish": "red",
        "bullish": "green", "neutral": "dim",
    }

    for ind in macro_data.indicators:
        style = signal_styles.get(ind.signal, "white")
        table.add_row(
            ind.name,
            f"{ind.value:,.2f}",
            f"{ind.change:+,.2f}" if ind.change is not None else "—",
            f"[{style}]{ind.signal.upper()}[/{style}]",
            ind.description,
        )

    console.print(table)

    if macro_data.yield_curve_inverted:
        console.print("[bold red]⚠ YIELD CURVE INVERTED — recession signal active[/bold red]")
    if macro_data.credit_stress:
        console.print("[bold red]⚠ CREDIT STRESS DETECTED — corporate distress feared[/bold red]")


def _change_style(value: float | None) -> str:
    if value is None:
        return "dim"
    if value >= 2:
        return "bold green"
    if value > 0:
        return "green"
    if value <= -3:
        return "bold red"
    if value < 0:
        return "red"
    return "white"


def _get_signal_indicator(item: dict) -> str:
    signals = []
    rsi = item.get("rsi_14")
    if rsi and rsi >= 70:
        signals.append("[red]OB[/red]")
    elif rsi and rsi <= 30:
        signals.append("[green]OS[/green]")
    ma50 = item.get("fifty_day_ma")
    ma200 = item.get("two_hundred_day_ma")
    if ma50 and ma200:
        if ma50 < ma200:
            signals.append("[red]DC[/red]")
        elif ma50 > ma200 * 1.02:
            signals.append("[green]GC[/green]")
    return " ".join(signals) if signals else "—"


# --- Commands ---

def cmd_init():
    console.print("[bold]Initializing database...[/bold]")
    init_db()
    console.print("[green]Database initialized successfully.[/green]")


def cmd_scan():
    config = load_config()
    console.print(Panel(
        "[bold]Financial Agent — Quick Market Scan[/bold]\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        border_style="blue",
    ))

    market_data = collect_all_data(config)
    display_market_overview(market_data)

    console.print("\n[bold]Fetching Macro Data (FRED)...[/bold]")
    macro_data = fetch_macro_data()
    display_macro_summary(macro_data)

    console.print("\n[bold]Running risk checks...[/bold]")
    health = assess_market_health(market_data, config.get("risk_thresholds", {}), macro_data=macro_data)
    display_risk_report(health)

    init_db()
    session = get_session()
    save_snapshots(market_data, session)
    save_alerts(health, session)
    session.close()

    console.print(f"\n{LIMITATIONS_FOOTER}")
    console.print("[dim]Data saved. Run 'analyze' for full AI-powered analysis.[/dim]")


def cmd_analyze():
    config = load_config()
    console.print(Panel(
        "[bold]Financial Agent — Full Market Analysis[/bold]\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        "Powered by Claude AI + Investing Knowledge Base\n"
        "Data: Technical + Macro (FRED) + Fundamentals",
        border_style="blue",
    ))

    market_data = collect_all_data(config)
    display_market_overview(market_data)

    console.print("\n[bold]Fetching Macro Data (FRED)...[/bold]")
    macro_data = fetch_macro_data()
    display_macro_summary(macro_data)

    console.print("\n[bold]Fetching Fundamentals...[/bold]")
    stock_symbols = config.get("watchlist", {}).get("stocks", [])
    fundamentals_data = fetch_fundamentals_batch(stock_symbols) if stock_symbols else {}

    console.print("\n[bold]Running risk checks (3-layer)...[/bold]")
    health = assess_market_health(
        market_data, config.get("risk_thresholds", {}),
        macro_data=macro_data, fundamentals_data=fundamentals_data,
    )
    display_risk_report(health)

    console.print("\n[bold]Building historical context...[/bold]")
    trend_context = build_trend_context()

    console.print("\n[bold]Running AI analysis...[/bold]")
    report = analyze_market_trends(
        market_data,
        macro_data=macro_data,
        fundamentals_data=fundamentals_data,
        trend_context=trend_context,
    )

    console.print(Panel(
        Markdown(report["full_report"]),
        title=f"AI Market Analysis — Risk: {report['risk_level'].upper()} | Confidence: {health.confidence.upper()}",
        border_style="blue",
        padding=(1, 2),
    ))

    init_db()
    session = get_session()
    save_snapshots(market_data, session)
    save_alerts(health, session)
    analysis = AnalysisReport(
        report_type=report["report_type"],
        summary=report["summary"],
        full_report=report["full_report"],
        risk_level=report["risk_level"],
        confidence=health.confidence,
    )
    session.add(analysis)
    session.commit()
    session.close()

    console.print(f"\n{LIMITATIONS_FOOTER}")
    console.print("[dim]Analysis saved to database.[/dim]")


def cmd_risk():
    config = load_config()
    console.print(Panel(
        "[bold]Financial Agent — Risk Assessment[/bold]\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        border_style="red",
    ))

    console.print("\n[bold]Fetching Market Indices...[/bold]")
    indices = fetch_market_indices(config.get("market_indices", []))

    console.print("\n[bold]Fetching Key ETFs...[/bold]")
    etfs = fetch_multiple(config["watchlist"].get("etfs", []), asset_type="etf")

    console.print("\n[bold]Fetching Macro Data (FRED)...[/bold]")
    macro_data = fetch_macro_data()
    display_macro_summary(macro_data)

    market_data = {"indices": indices, "etfs": etfs, "stocks": [], "crypto": [], "forex": []}
    health = assess_market_health(market_data, config.get("risk_thresholds", {}), macro_data=macro_data)
    display_risk_report(health)
    console.print(f"\n{LIMITATIONS_FOOTER}")


def cmd_predictions():
    console.print(Panel(
        "[bold]Financial Agent — Prediction Tracker[/bold]\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        border_style="cyan",
    ))
    check_predictions()
    display_predictions()


def cmd_report(output: str | None = None, no_open: bool = False):
    generate_report(output_path=output, open_browser=not no_open)


def main():
    parser = argparse.ArgumentParser(
        description="Financial Agent — Market trends and collapse risk analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Commands:
  init          Initialize the database
  scan          Quick market scan (data + macro + risk, no AI)
  analyze       Full AI-powered analysis (all 3 data layers)
  risk          Risk assessment only (indices + ETFs + macro)
  report        Generate static HTML report (opens in browser)
  predictions   View prediction tracker and accuracy
        """,
    )
    parser.add_argument("command", choices=["init", "scan", "analyze", "risk", "report", "predictions"],
                        help="Command to run")
    parser.add_argument("--output", "-o", help="Output path for report (report command only)")
    parser.add_argument("--no-open", action="store_true", help="Don't open report in browser (report command only)")
    args = parser.parse_args()

    if args.command == "report":
        cmd_report(output=args.output, no_open=args.no_open)
    else:
        commands = {
            "init": cmd_init,
            "scan": cmd_scan,
            "analyze": cmd_analyze,
            "risk": cmd_risk,
            "predictions": cmd_predictions,
        }
        commands[args.command]()


if __name__ == "__main__":
    main()
