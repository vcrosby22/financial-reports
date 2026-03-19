"""Fetch forex data via yfinance (currency pairs as tickers)."""

from rich.console import Console

from .stocks import fetch_ticker_data

console = Console()

FOREX_TICKER_MAP = {
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "USDJPY=X",
    "USD/CHF": "USDCHF=X",
    "AUD/USD": "AUDUSD=X",
    "USD/CAD": "USDCAD=X",
}


def fetch_forex_data(pairs: list[str]) -> list[dict]:
    """Fetch forex pair data using yfinance ticker format."""
    results = []
    for pair in pairs:
        yf_symbol = FOREX_TICKER_MAP.get(pair, pair.replace("/", "") + "=X")
        console.print(f"  Fetching {pair} ({yf_symbol})...", style="dim")
        data = fetch_ticker_data(yf_symbol, period="3mo")
        if data:
            data["ticker"] = pair.replace("/", "")
            data["name"] = pair
            data["asset_type"] = "forex"
            results.append(data)
    return results
