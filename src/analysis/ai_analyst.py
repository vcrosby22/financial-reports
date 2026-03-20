"""Claude-powered market analysis engine.

Sends structured market data to Claude and receives analysis focused on:
1. Market trends — what's moving and why
2. Collapse risk — systemic warning signals
3. Opportunities — what the data suggests is worth watching

Enhanced with:
- Macroeconomic data from FRED (yield curve, credit spreads, etc.)
- Fundamental data (earnings revisions, insider activity)
- Historical trend context (what changed since last analysis)
- Confidence self-rating requirements
"""

from pathlib import Path
from typing import TYPE_CHECKING

from anthropic import Anthropic
from rich.console import Console

from ..config import get_settings

if TYPE_CHECKING:
    from .risk import MarketHealthReport

console = Console()


def format_engine_risk_for_prompt(health: "MarketHealthReport") -> str:
    """Summarize rule-based risk for Claude reconciliation (same run / same data)."""
    top = health.score_contributions[:10]
    lines = [
        "=== RULE-BASED RISK ENGINE (same session data) ===",
        f"Engine label: **{health.overall_risk.upper()}** | Score: **{health.score}/100** (capped)",
        f"Raw sum before cap: **{health.score_uncapped}**",
        "",
        "Top contributors to the numeric score:",
    ]
    for c in top:
        lines.append(
            f"- {c.name} [{c.category}, {c.severity}, {c.signal_type}]: +{c.points} pts"
        )
    lines.extend([
        "",
        "If your **RISK ASSESSMENT** below differs from the engine label, explain briefly "
        "(e.g. conflicting macro vs prices, threshold skepticism, or breadth vs severity).",
        "=== END ENGINE SUMMARY ===",
        "",
    ])
    return "\n".join(lines)

KNOWLEDGE_PATH = Path(__file__).parent.parent.parent / "INVESTING_KNOWLEDGE.md"

SYSTEM_PROMPT = """You are a senior financial analyst AI. Your job is to analyze real market data and provide clear, actionable insights. You are advising a personal investor who is building their investing knowledge.

Your analysis framework draws from proven investing wisdom:
- Warren Buffett: focus on business quality, economic moats, long-term value
- Benjamin Graham: margin of safety, Mr. Market's mood swings
- Howard Marks: second-level thinking — what does the consensus believe, and where might it be wrong?
- Ray Dalio: macro cycles, uncorrelated risks, all-weather thinking
- Behavioral finance: flag when market behavior shows signs of herding, euphoria, or panic

IMPORTANT RULES:
1. Base ALL analysis on the data provided. Never invent or recall market data from memory.
2. Clearly separate facts (what the data shows) from interpretation (what it might mean).
3. Assign risk levels: LOW, MODERATE, ELEVATED, HIGH, CRITICAL.
4. Be direct and specific. No filler. Name specific tickers when relevant.
5. This is analysis, NOT financial advice. Frame everything as "the data suggests" not "you should buy/sell."

UNCERTAINTY AND CONFIDENCE RULES (CRITICAL):
6. For EVERY major claim, rate your confidence: HIGH (4+ data points agree), MEDIUM (2-3 agree), LOW (single signal or inference).
7. Explicitly state what data is MISSING from your analysis that would change your assessment.
8. When signals CONTRADICT each other, call it out — don't paper over it.
9. Never say "clearly" or "obviously" — if it were clear, you wouldn't need to say it.
10. If you don't have enough data to make a determination, say "insufficient data" rather than guessing.

INHERENT LIMITATIONS (always acknowledge):
- This system cannot predict black swan events (pandemics, wars, regulatory shocks)
- In true market crises, correlations break down — "diversified" assets may all fall together
- Free data sources (yfinance) may have delays or accuracy issues
- Position sizing guidance is conservative and should not be the sole basis for decisions

DISCLAIMER: Always include that this is AI-generated analysis for educational purposes, not financial advice. Risk scores reflect available data and have inherent limitations."""


def _load_knowledge_context() -> str:
    """Load the collapse risk indicators section from the knowledge base."""
    if not KNOWLEDGE_PATH.exists():
        return ""
    content = KNOWLEDGE_PATH.read_text()
    collapse_section = ""
    in_section = False
    for line in content.split("\n"):
        if "## 7. Market Collapse Risk Indicators" in line:
            in_section = True
        elif in_section and line.startswith("## ") and "Collapse" not in line:
            break
        if in_section:
            collapse_section += line + "\n"
    return collapse_section


def _build_market_data_prompt(market_data: dict) -> str:
    """Format collected market data into a structured prompt for Claude."""
    sections = []

    if market_data.get("indices"):
        sections.append("=== MARKET INDICES ===")
        for item in market_data["indices"]:
            line = f"{item['ticker']}: ${item['price']}"
            if item.get("change_pct_1d") is not None:
                line += f" | 1D: {item['change_pct_1d']:+.2f}%"
            if item.get("change_pct_1w") is not None:
                line += f" | 1W: {item['change_pct_1w']:+.2f}%"
            if item.get("change_pct_1m") is not None:
                line += f" | 1M: {item['change_pct_1m']:+.2f}%"
            if item.get("rsi_14") is not None:
                line += f" | RSI: {item['rsi_14']:.1f}"
            sections.append(line)

    for category, label in [("stocks", "STOCKS"), ("etfs", "ETFs"), ("crypto", "CRYPTO"), ("forex", "FOREX")]:
        items = market_data.get(category, [])
        if items:
            sections.append(f"\n=== {label} ===")
            for item in items:
                line = f"{item['ticker']}: ${item['price']}"
                if item.get("change_pct_1d") is not None:
                    line += f" | 1D: {item['change_pct_1d']:+.2f}%"
                if item.get("change_pct_1w") is not None:
                    line += f" | 1W: {item['change_pct_1w']:+.2f}%"
                if item.get("change_pct_1m") is not None:
                    line += f" | 1M: {item['change_pct_1m']:+.2f}%"
                if item.get("pe_ratio") is not None:
                    line += f" | P/E: {item['pe_ratio']:.1f}"
                if item.get("rsi_14") is not None:
                    line += f" | RSI: {item['rsi_14']:.1f}"
                if item.get("fifty_day_ma") and item.get("two_hundred_day_ma"):
                    ma_status = "ABOVE" if item["fifty_day_ma"] > item["two_hundred_day_ma"] else "BELOW"
                    line += f" | 50MA {ma_status} 200MA"
                sections.append(line)

    return "\n".join(sections)


def _build_fundamentals_prompt(fundamentals_data: dict) -> str:
    """Format fundamental data for Claude."""
    if not fundamentals_data:
        return ""

    lines = ["\n=== FUNDAMENTAL DATA ==="]
    for ticker, fund in fundamentals_data.items():
        parts = [f"{ticker}:"]
        parts.append(f"Health={fund.fundamental_health}")
        if fund.eps_revision_trend != "neutral":
            parts.append(f"EPS_Revisions={fund.eps_revision_trend}")
        if fund.insider_signal != "neutral":
            parts.append(f"Insiders={fund.insider_signal}")
        if fund.debt_to_equity is not None:
            parts.append(f"D/E={fund.debt_to_equity:.2f}")
        if fund.roe is not None:
            parts.append(f"ROE={fund.roe:.1%}")
        if fund.upside_to_mean_target is not None:
            parts.append(f"Analyst_Upside={fund.upside_to_mean_target:+.1f}%")
        parts.append(f"Data_Completeness={fund.data_completeness:.0%}")
        lines.append(" | ".join(parts))

    return "\n".join(lines)


def analyze_market_trends(
    market_data: dict,
    macro_data=None,
    fundamentals_data: dict | None = None,
    trend_context: str = "",
    engine_risk_prompt: str = "",
) -> dict:
    """Run a full market trend analysis via Claude."""
    settings = get_settings()
    if not settings.anthropic_api_key:
        return {
            "report_type": "market_trends",
            "summary": "No API key configured — cannot run AI analysis.",
            "full_report": "Set ANTHROPIC_API_KEY in .env to enable Claude-powered analysis.",
            "risk_level": "unknown",
        }

    client = Anthropic(api_key=settings.anthropic_api_key)
    data_prompt = _build_market_data_prompt(market_data)
    knowledge_context = _load_knowledge_context()
    fundamentals_prompt = _build_fundamentals_prompt(fundamentals_data) if fundamentals_data else ""
    macro_prompt = macro_data.to_prompt_text() if macro_data else ""

    data_completeness_note = _build_completeness_note(macro_data, fundamentals_data)

    engine_block = f"{engine_risk_prompt}\n" if engine_risk_prompt else ""

    user_prompt = f"""Analyze the following market data. For EVERY major conclusion, state your confidence level (HIGH/MEDIUM/LOW).

{engine_block}{data_completeness_note}

{f"=== HISTORICAL CONTEXT ==={chr(10)}{trend_context}" if trend_context else "No historical data available — this is the first analysis."}

1. **MARKET OVERVIEW** — What is the overall market doing? Bull, bear, or sideways? What's driving it? State your confidence.

2. **KEY TRENDS** — Identify the 3-5 most significant trends. Distinguish between LEADING indicators (macro, earnings revisions, insider activity) and LAGGING indicators (RSI, moving averages). Leading indicators deserve more weight.

3. **RISK ASSESSMENT** — Rate overall market risk as LOW / MODERATE / ELEVATED / HIGH / CRITICAL.
   - Check VIX, death crosses, RSI extremes, divergences
   - Check macro indicators: yield curve, credit spreads, unemployment claims
   - Check fundamentals: earnings revision trends, insider activity patterns
   - State what data is MISSING that would change your assessment
   - State your confidence in the risk rating

4. **COLLAPSE WARNING SIGNALS** — Check against these known indicators:
{knowledge_context}

5. **OPPORTUNITIES** — Apply second-level thinking. For each opportunity:
   - State the thesis
   - State the risk (what could go wrong)
   - Rate confidence (HIGH/MEDIUM/LOW)
   - Suggest position sizing: Low Risk (3-5%), Medium (1-3%), High (0.5-1%)

6. **WATCHLIST ALERTS** — Flag anything needing attention. Include position sizing guidance.

7. **WHAT I DON'T KNOW** — Explicitly list data gaps and how they limit this analysis.

Here is the current market data:

{data_prompt}
{macro_prompt}
{fundamentals_prompt}"""

    console.print("[bold blue]Sending data to Claude for analysis...[/bold blue]")

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    full_report = response.content[0].text
    risk_level = _extract_risk_level(full_report)

    summary_lines = []
    for line in full_report.split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and not stripped.startswith("*"):
            summary_lines.append(stripped)
            if len(summary_lines) >= 3:
                break
    summary = " ".join(summary_lines)[:500]

    return {
        "report_type": "market_trends",
        "summary": summary,
        "full_report": full_report,
        "risk_level": risk_level,
    }


def _build_completeness_note(macro_data, fundamentals_data) -> str:
    """Tell Claude what data it has and what's missing."""
    present = ["Technical price data (yfinance)"]
    missing = []

    if macro_data:
        present.append(f"Macroeconomic data (FRED — {len(macro_data.indicators)} indicators)")
    else:
        missing.append("Macroeconomic data (FRED — yield curve, credit spreads, unemployment)")

    if fundamentals_data:
        present.append(f"Fundamental data ({len(fundamentals_data)} stocks — earnings, insiders, health)")
    else:
        missing.append("Fundamental data (earnings revisions, insider activity, analyst targets)")

    missing.extend([
        "Options flow / put-call ratio (would need paid data)",
        "Real-time news sentiment",
        "Institutional positioning data",
    ])

    lines = ["=== DATA COMPLETENESS ==="]
    lines.append("AVAILABLE: " + "; ".join(present))
    lines.append("MISSING: " + "; ".join(missing))
    lines.append("Adjust your confidence levels based on what data is present vs missing.")
    return "\n".join(lines)


def _extract_risk_level(report: str) -> str:
    report_upper = report.upper()
    for level in ["CRITICAL", "HIGH", "ELEVATED", "MODERATE", "LOW"]:
        if level in report_upper:
            idx = report_upper.index(level)
            surrounding = report_upper[max(0, idx - 50):idx + 50]
            if "RISK" in surrounding or "ASSESSMENT" in surrounding or "LEVEL" in surrounding or "OVERALL" in surrounding:
                return level.lower()
    return "unknown"
