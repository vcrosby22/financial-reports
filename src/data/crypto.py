"""Fetch cryptocurrency data via ccxt (Binance by default)."""

from datetime import datetime, timezone

from rich.console import Console

console = Console()


COINBASE_PAIR_MAP = {
    "BTC/USDT": "BTC/USD",
    "ETH/USDT": "ETH/USD",
    "SOL/USDT": "SOL/USD",
}


def fetch_crypto_data(symbols: list[str], exchange_id: str = "coinbase") -> list[dict]:
    """Fetch current price data for crypto pairs.

    Args:
        symbols: List of trading pairs like ["BTC/USDT", "ETH/USDT"]
        exchange_id: Exchange to use (default: coinbase)
    """
    try:
        import ccxt

        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({"enableRateLimit": True})
        exchange.load_markets()
    except Exception as e:
        console.print(f"[red]Failed to connect to {exchange_id}: {e}[/red]")
        return []

    results = []
    for symbol in symbols:
        symbol = COINBASE_PAIR_MAP.get(symbol, symbol)
        try:
            console.print(f"  Fetching {symbol}...", style="dim")
            ticker = exchange.fetch_ticker(symbol)

            CRYPTO_NAMES = {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"}
            base = symbol.split("/")[0]
            ts_ms = ticker.get("timestamp")
            quote_time = None
            if ts_ms is not None:
                try:
                    quote_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                except (OSError, OverflowError, ValueError, TypeError):
                    quote_time = None
            results.append({
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
                "change_pct_1d": ticker.get("percentage"),
                "change_pct_1w": None,
                "change_pct_1m": None,
                "pe_ratio": None,
                "pb_ratio": None,
                "dividend_yield": None,
                "fifty_day_ma": None,
                "two_hundred_day_ma": None,
                "rsi_14": None,
            })
        except Exception as e:
            console.print(f"[red]Error fetching {symbol}: {e}[/red]")

    return results
