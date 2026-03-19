"""Fetch stock, ETF, commodity, and index data via yfinance."""

from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from rich.console import Console

console = Console()


def fetch_ticker_data(symbol: str, period: str = "3mo") -> dict | None:
    """Fetch current and historical data for a single ticker."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}
        hist = ticker.history(period=period)

        if hist.empty:
            console.print(f"[yellow]No data returned for {symbol}[/yellow]")
            return None

        current_price = hist["Close"].iloc[-1]

        change_1d = _pct_change(hist, 1)
        change_1w = _pct_change(hist, 5)
        change_1m = _pct_change(hist, 21)

        rsi = _compute_rsi(hist["Close"], period=14)

        return {
            "ticker": symbol,
            "name": info.get("shortName") or info.get("longName") or symbol,
            "price": round(current_price, 2),
            "open_price": round(hist["Open"].iloc[-1], 2),
            "high": round(hist["High"].iloc[-1], 2),
            "low": round(hist["Low"].iloc[-1], 2),
            "volume": int(hist["Volume"].iloc[-1]) if hist["Volume"].iloc[-1] else 0,
            "market_cap": info.get("marketCap"),
            "change_pct_1d": round(change_1d, 2) if change_1d else None,
            "change_pct_1w": round(change_1w, 2) if change_1w else None,
            "change_pct_1m": round(change_1m, 2) if change_1m else None,
            "pe_ratio": info.get("trailingPE"),
            "pb_ratio": info.get("priceToBook"),
            "dividend_yield": info.get("dividendYield"),
            "fifty_day_ma": info.get("fiftyDayAverage"),
            "two_hundred_day_ma": info.get("twoHundredDayAverage"),
            "rsi_14": round(rsi, 2) if rsi else None,
            "history": hist,
            "info": info,
        }
    except Exception as e:
        console.print(f"[red]Error fetching {symbol}: {e}[/red]")
        return None


def fetch_multiple(symbols: list[str], asset_type: str = "stock", period: str = "3mo") -> list[dict]:
    """Fetch data for multiple tickers."""
    results = []
    for symbol in symbols:
        console.print(f"  Fetching {symbol}...", style="dim")
        data = fetch_ticker_data(symbol, period=period)
        if data:
            data["asset_type"] = asset_type
            results.append(data)
    return results


def fetch_market_indices(indices: list[str], period: str = "3mo") -> list[dict]:
    """Fetch major market indices including VIX."""
    return fetch_multiple(indices, asset_type="index", period=period)


def _pct_change(hist: pd.DataFrame, days: int) -> float | None:
    if len(hist) < days + 1:
        return None
    current = hist["Close"].iloc[-1]
    previous = hist["Close"].iloc[-(days + 1)]
    return ((current - previous) / previous) * 100


def _compute_rsi(prices: pd.Series, period: int = 14) -> float | None:
    if len(prices) < period + 1:
        return None
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()

    last_loss = loss.iloc[-1]
    if last_loss == 0:
        return 100.0

    rs = gain.iloc[-1] / last_loss
    return 100 - (100 / (1 + rs))
