"""
Async Tick Fetcher for finda Pro-Grade
High-performance async tick data with Bid/Ask microstructure
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Optional
import logging

try:
    import ccxt.async_support as ccxt_async
except ImportError:
    ccxt_async = None

from .schemas import Tick, TickResponse
from .config import settings, normalize_symbol, logger
from .cache_manager import cache_manager
from .async_ohlcv import provider_health


async def fetch_dukascopy_tick_async(
    symbol: str,
    start: datetime,
    end: datetime
) -> pd.DataFrame:
    """Async Dukascopy tick fetcher - uses executor for sync library."""
    import time
    start_time = time.time()
    
    try:
        from dukascopy_python import fetch, INTERVAL_TICK, OFFER_SIDE_BID
        
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(
            None,
            lambda: fetch(symbol.upper(), INTERVAL_TICK, OFFER_SIDE_BID, start=start, end=end)
        )
        
        latency = (time.time() - start_time) * 1000
        provider_health.mark_healthy("dukascopy", latency)
        
        if df is None or df.empty:
            return pd.DataFrame()
        
        df = df.reset_index()
        
        # Standardize columns
        result = pd.DataFrame({
            "time": df.get("timestamp", df.index) if "timestamp" in df.columns else df.index,
            "bid": df.get("bidPrice", [None] * len(df)),
            "ask": df.get("askPrice", [None] * len(df)),
            "bid_volume": df.get("bidVolume", [0] * len(df)),
            "ask_volume": df.get("askVolume", [0] * len(df)),
            "volume": df.get("bidVolume", [0] * len(df))
        })
        
        # Ensure UTC
        if result["time"].dt.tz is None:
            result["time"] = result["time"].dt.tz_localize("UTC")
        
        return result
    
    except Exception as e:
        provider_health.mark_unhealthy("dukascopy", str(e))
        raise


async def fetch_binance_tick_async(
    symbol: str,
    start: datetime,
    end: datetime
) -> pd.DataFrame:
    """Async Binance trade (tick) fetcher."""
    if ccxt_async is None:
        raise ImportError("ccxt.async_support not available")
    
    exchange = ccxt_async.binance({"enableRateLimit": True})
    
    try:
        import time
        start_time = time.time()
        
        binance_symbol = symbol.replace("/", "").upper()
        if "USDT" not in binance_symbol and "USD" in binance_symbol:
            binance_symbol = binance_symbol.replace("USD", "USDT")
        
        since_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        
        all_trades = []
        
        while since_ms < end_ms:
            trades = await exchange.fetch_trades(binance_symbol, since=since_ms, limit=1000)
            if not trades:
                break
            
            for t in trades:
                if t["timestamp"] > end_ms:
                    break
                all_trades.append(t)
            
            if len(trades) < 1000:
                break
            since_ms = trades[-1]["timestamp"] + 1
        
        latency = (time.time() - start_time) * 1000
        provider_health.mark_healthy("binance", latency)
        
        if not all_trades:
            return pd.DataFrame()
        
        # Build DataFrame with bid/ask inferred from trade side
        rows = []
        for t in all_trades:
            side = t.get("side")
            rows.append({
                "time": datetime.fromtimestamp(t["timestamp"] / 1000, tz=timezone.utc),
                "bid": t["price"] if side == "sell" else None,
                "ask": t["price"] if side == "buy" else None,
                "bid_volume": t["amount"] if side == "sell" else 0,
                "ask_volume": t["amount"] if side == "buy" else 0,
                "volume": t["amount"]
            })
        
        return pd.DataFrame(rows)
    
    except Exception as e:
        provider_health.mark_unhealthy("binance", str(e))
        raise
    finally:
        await exchange.close()


async def fetch_alpaca_tick_async(
    symbol: str,
    start: datetime,
    end: datetime
) -> pd.DataFrame:
    """Async Alpaca trade fetcher."""
    import time
    start_time = time.time()
    
    if not settings.alpaca_api_key or not settings.alpaca_secret_key:
        raise ValueError("Alpaca API keys not configured")
    
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockTradesRequest
        
        client = StockHistoricalDataClient(settings.alpaca_api_key, settings.alpaca_secret_key)
        
        request = StockTradesRequest(
            symbol_or_symbols=symbol.upper(),
            start=start,
            end=end
        )
        
        loop = asyncio.get_event_loop()
        trades = await loop.run_in_executor(None, lambda: client.get_stock_trades(request).df)
        
        latency = (time.time() - start_time) * 1000
        provider_health.mark_healthy("alpaca", latency)
        
        if trades.empty:
            return pd.DataFrame()
        
        if isinstance(trades.index, pd.MultiIndex):
            trades = trades.loc[symbol.upper()]
        
        trades = trades.reset_index()
        
        result = pd.DataFrame({
            "time": trades["timestamp"],
            "bid": trades["price"],
            "ask": trades["price"],
            "bid_volume": [0] * len(trades),
            "ask_volume": [0] * len(trades),
            "volume": trades.get("size", [0] * len(trades))
        })
        
        # Ensure UTC
        if result["time"].dt.tz is None:
            result["time"] = result["time"].dt.tz_localize("UTC")
        else:
            result["time"] = result["time"].dt.tz_convert("UTC")
        
        return result
    
    except Exception as e:
        provider_health.mark_unhealthy("alpaca", str(e))
        raise


async def fetch_tick_unified_async(
    symbol: str,
    start: datetime,
    end: datetime,
    provider: Optional[str] = None,
    use_cache: bool = True
) -> tuple[pd.DataFrame, str, bool]:
    """
    Unified async tick fetcher with smart fallback.
    
    Returns:
        (DataFrame, provider_name, from_cache)
    """
    normalized_symbol = normalize_symbol(symbol)
    
    # Check cache first
    if use_cache:
        cached_df = cache_manager.check_cache(
            normalized_symbol, "tick", "tick", start, end
        )
        if cached_df is not None:
            return cached_df, "cache", True
    
    # Provider selection
    if provider:
        providers = [provider]
    else:
        from .config import get_provider_for_symbol
        primary = get_provider_for_symbol(normalized_symbol)
        providers = [primary] + [p for p in ["dukascopy", "binance", "alpaca"] if p != primary]
    
    last_error = None
    
    for prov in providers:
        try:
            if prov == "dukascopy":
                df = await fetch_dukascopy_tick_async(normalized_symbol, start, end)
            elif prov == "binance":
                df = await fetch_binance_tick_async(normalized_symbol, start, end)
            elif prov == "alpaca":
                df = await fetch_alpaca_tick_async(normalized_symbol, start, end)
            else:
                continue
            
            if not df.empty:
                if use_cache:
                    cache_manager.save_cache(df, normalized_symbol, "tick", "tick", start, end)
                return df, prov, False
        
        except Exception as e:
            last_error = e
            logger.warning(f"{prov} tick fetch failed: {e}")
            continue
    
    if last_error:
        raise last_error
    
    return pd.DataFrame(), "none", False


def df_to_ticks(df: pd.DataFrame) -> List[Tick]:
    """Convert DataFrame to list of Tick models."""
    ticks = []
    for _, row in df.iterrows():
        ticks.append(Tick(
            time=row["time"],
            bid=row.get("bid"),
            ask=row.get("ask"),
            bid_volume=row.get("bid_volume", 0),
            ask_volume=row.get("ask_volume", 0),
            volume=row.get("volume", 0)
        ))
    return ticks
