"""Cryptocurrency quotes: yfinance (RSI, 1D/1W/1M, MAs, signals) with ccxt fallback."""

from __future__ import annotations

from datetime import datetime, timezone

from rich.console import Console

from .stocks import fetch_ticker_data

console = Console()

COINBASE_PAIR_MAP = {
    "BTC/USDT": "BTC/USD",
    "ETH/USDT": "ETH/USD",
    "SOL/USDT": "SOL/USD",
}

CRYPTO_NAMES = {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"}

# Explicit overrides; unknown BASE/QUOTE pairs fall back to BASE-USD when quote is USD-like.
YFINANCE_SYMBOL_OVERRIDES: dict[str, str] = {
    "BTC/USDT": "BTC-USD",
    "ETH/USDT": "ETH-USD",
    "SOL/USDT": "SOL-USD",
    "BTC/USD": "BTC-USD",
    "ETH/USD": "ETH-USD",
    "SOL/USD": "SOL-USD",
}


def _canonical_pair(symbol: str) -> str:
    return COINBASE_PAIR_MAP.get(symbol, symbol)


def pair_to_yfinance_symbol(symbol: str) -> str | None:
    """Map config pair (e.g. BTC/USDT) to a Yahoo Finance ticker (e.g. BTC-USD)."""
    raw = _canonical_pair(symbol)
    if raw in YFINANCE_SYMBOL_OVERRIDES:
        return YFINANCE_SYMBOL_OVERRIDES[raw]
    if "/" not in raw:
        return None
    base, quote = raw.split("/", 1)
    qu = quote.upper()
    if qu in ("USDT", "USD", "USDC", "DAI"):
        return f"{base}-USD"
    return f"{base}-{quote}"


def _quote_time_from_history(hist) -> datetime | None:
    if hist is None or len(hist.index) == 0:
        return None
    try:
        ts = hist.index[-1]
        if hasattr(ts, "tzinfo") and ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        elif hasattr(ts, "tz_convert"):
            ts = ts.tz_convert("UTC")
        return ts.to_pydatetime()
    except (TypeError, ValueError, OSError):
        return None


def _from_yfinance(config_symbol: str) -> dict | None:
    yf_sym = pair_to_yfinance_symbol(config_symbol)
    if not yf_sym:
        return None
    console.print(f"  Fetching {config_symbol} (yfinance {yf_sym})...", style="dim")
    raw = fetch_ticker_data(yf_sym, period="3mo")
    if not raw:
        return None
    hist = raw.get("history")
    quote_time = _quote_time_from_history(hist)
    raw.pop("history", None)
    raw.pop("info", None)

    base = yf_sym.split("-")[0].upper()
    raw["ticker"] = yf_sym.replace("-", "")
    raw["name"] = CRYPTO_NAMES.get(base, raw.get("name", base))
    raw["asset_type"] = "crypto"
    raw["quote_time"] = quote_time
    return raw


def _init_ccxt_exchange(exchange_id: str):
    try:
        import ccxt

        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({"enableRateLimit": True})
        exchange.load_markets()
        return exchange
    except Exception as e:
        console.print(f"[red]Failed to connect to {exchange_id}: {e}[/red]")
        return None


def _from_ccxt(exchange, config_symbol: str) -> dict | None:
    symbol = _canonical_pair(config_symbol)
    try:
        console.print(f"  Fetching {symbol} (ccxt fallback)...", style="dim")
        ticker = exchange.fetch_ticker(symbol)
        base = symbol.split("/")[0]
        ts_ms = ticker.get("timestamp")
        quote_time = None
        if ts_ms is not None:
            try:
                quote_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            except (OSError, OverflowError, ValueError, TypeError):
                quote_time = None
        pct = ticker.get("percentage")
        if pct is not None:
            try:
                pct = round(float(pct), 2)
            except (TypeError, ValueError):
                pct = None
        return {
            "ticker": symbol.replace("/", ""),
            "name": CRYPTO_NAMES.get(base, base),
            "asset_type": "crypto",
            "quote_time": quote_time,
            "price": ticker.get("last"),
            "open_price": ticker.get("open"),
            "high": ticker.get("high"),
            "low": ticker.get("low"),
            "volume": ticker.get("baseVolume"),
            "market_cap": None,
            "change_pct_1d": pct,
            "change_pct_1w": None,
            "change_pct_1m": None,
            "pe_ratio": None,
            "pb_ratio": None,
            "dividend_yield": None,
            "fifty_day_ma": None,
            "two_hundred_day_ma": None,
            "rsi_14": None,
        }
    except Exception as e:
        console.print(f"[red]Error fetching {symbol}: {e}[/red]")
        return None


def fetch_crypto_data(symbols: list[str], exchange_id: str = "coinbase") -> list[dict]:
    """Fetch crypto rows with RSI / 1D–1M changes / MAs via yfinance; ccxt fallback for price only.

    Yahoo symbols (e.g. BTC-USD) align with the same metrics as equities in ``stocks.fetch_ticker_data``.
    """
    results: list[dict] = []
    ccxt_ex = None
    for sym in symbols:
        row = _from_yfinance(sym)
        if row is None:
            if ccxt_ex is None:
                ccxt_ex = _init_ccxt_exchange(exchange_id)
            if ccxt_ex is not None:
                row = _from_ccxt(ccxt_ex, sym)
        if row:
            results.append(row)
    return results
