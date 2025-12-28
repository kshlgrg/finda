"""
Async OHLCV Fetcher for finda Pro-Grade
High-performance async fetching with chunked parallel requests
"""

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Optional
import logging

# Use async ccxt
try:
    import ccxt.async_support as ccxt_async
except ImportError:
    ccxt_async = None

from .schemas import Candle, OHLCVResponse
from .config import settings, normalize_symbol, get_provider_for_symbol, logger
from .cache_manager import cache_manager


class ProviderHealth:
    """Tracks health status of data providers."""
    
    def __init__(self):
        self.status = {
            "dukascopy": {"healthy": True, "last_error": None, "latency_ms": None},
            "binance": {"healthy": True, "last_error": None, "latency_ms": None},
            "alpaca": {"healthy": True, "last_error": None, "latency_ms": None},
        }
    
    def mark_healthy(self, provider: str, latency_ms: float):
        self.status[provider] = {"healthy": True, "last_error": None, "latency_ms": latency_ms}
    
    def mark_unhealthy(self, provider: str, error: str):
        self.status[provider] = {"healthy": False, "last_error": str(error)[:200], "latency_ms": None}
        logger.warning(f"Provider {provider} marked unhealthy: {error}")
    
    def get_ranked_providers(self) -> List[str]:
        """Get providers ordered by health and latency."""
        healthy = [(p, s) for p, s in self.status.items() if s["healthy"]]
        healthy.sort(key=lambda x: x[1].get("latency_ms") or 9999)
        return [p for p, _ in healthy]


# Global health tracker
provider_health = ProviderHealth()


def _parse_timeframe_ms(tf: str) -> int:
    """Convert timeframe string to milliseconds."""
    unit_map = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    import re
    match = re.match(r"(\d+)([mhdw])", tf.lower())
    if match:
        return int(match.group(1)) * unit_map.get(match.group(2), 60_000)
    return 60_000  # Default 1 minute


async def fetch_binance_ohlcv_async(
    symbol: str, 
    tf: str, 
    start: datetime, 
    end: datetime
) -> pd.DataFrame:
    """Async Binance OHLCV fetcher."""
    if ccxt_async is None:
        raise ImportError("ccxt.async_support not available")
    
    exchange = ccxt_async.binance({"enableRateLimit": True})
    
    try:
        import time
        start_time = time.time()
        
        # Normalize symbol
        binance_symbol = symbol.replace("/", "").upper()
        if "USDT" not in binance_symbol and "USD" in binance_symbol:
            binance_symbol = binance_symbol.replace("USD", "USDT")
        
        # Convert timeframe
        tf_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
        ccxt_tf = tf_map.get(tf.lower(), "1m")
        
        since_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        
        all_ohlcv = []
        limit = 1000
        
        while since_ms < end_ms:
            ohlcv = await exchange.fetch_ohlcv(binance_symbol, ccxt_tf, since_ms, limit)
            if not ohlcv:
                break
            
            filtered = [c for c in ohlcv if c[0] <= end_ms]
            all_ohlcv.extend(filtered)
            
            if len(ohlcv) < limit:
                break
            since_ms = ohlcv[-1][0] + 1
        
        latency = (time.time() - start_time) * 1000
        provider_health.mark_healthy("binance", latency)
        
        if not all_ohlcv:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_ohlcv, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        
        return df
    
    except Exception as e:
        provider_health.mark_unhealthy("binance", str(e))
        raise
    finally:
        await exchange.close()


async def fetch_dukascopy_ohlcv_async(
    symbol: str,
    tf: str,
    start: datetime,
    end: datetime
) -> pd.DataFrame:
    """Async Dukascopy OHLCV fetcher (uses sync internally with executor)."""
    import time
    start_time = time.time()
    
    try:
        from dukascopy_python import fetch, OFFER_SIDE_BID
        
        # Convert timeframe
        tf_map = {"1m": "1MIN", "5m": "5MIN", "15m": "15MIN", "1h": "1HOUR", "1d": "1DAY"}
        dk_tf = tf_map.get(tf.lower(), "1MIN")
        
        # Run sync fetch in executor
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(
            None,
            lambda: fetch(symbol.upper(), dk_tf, OFFER_SIDE_BID, start=start, end=end)
        )
        
        latency = (time.time() - start_time) * 1000
        provider_health.mark_healthy("dukascopy", latency)
        
        if df is None or df.empty:
            return pd.DataFrame()
        
        df = df.reset_index()
        df = df.rename(columns={"timestamp": "time"})
        
        # Ensure UTC
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"], utc=True)
        
        # Select standard columns
        cols = ["time", "open", "high", "low", "close", "volume"]
        cols = [c for c in cols if c in df.columns]
        
        return df[cols]
    
    except Exception as e:
        provider_health.mark_unhealthy("dukascopy", str(e))
        raise


async def fetch_alpaca_ohlcv_async(
    symbol: str,
    tf: str,
    start: datetime,
    end: datetime
) -> pd.DataFrame:
    """Async Alpaca OHLCV fetcher."""
    import time
    start_time = time.time()
    
    if not settings.alpaca_api_key or not settings.alpaca_secret_key:
        raise ValueError("Alpaca API keys not configured")
    
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        
        tf_map = {
            "1m": TimeFrame.Minute,
            "1h": TimeFrame.Hour,
            "1d": TimeFrame.Day,
        }
        
        client = StockHistoricalDataClient(settings.alpaca_api_key, settings.alpaca_secret_key)
        
        request = StockBarsRequest(
            symbol_or_symbols=symbol.upper(),
            timeframe=tf_map.get(tf.lower(), TimeFrame.Minute),
            start=start,
            end=end,
            feed="sip"
        )
        
        # Run sync in executor
        loop = asyncio.get_event_loop()
        bars = await loop.run_in_executor(None, lambda: client.get_stock_bars(request).df)
        
        latency = (time.time() - start_time) * 1000
        provider_health.mark_healthy("alpaca", latency)
        
        if bars.empty:
            return pd.DataFrame()
        
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.loc[symbol.upper()]
        
        bars = bars.reset_index()
        bars = bars.rename(columns={"timestamp": "time"})
        
        # Ensure UTC
        if bars["time"].dt.tz is None:
            bars["time"] = bars["time"].dt.tz_localize("UTC")
        else:
            bars["time"] = bars["time"].dt.tz_convert("UTC")
        
        return bars[["time", "open", "high", "low", "close", "volume"]]
    
    except Exception as e:
        provider_health.mark_unhealthy("alpaca", str(e))
        raise


async def fetch_ohlcv_unified_async(
    symbol: str,
    tf: str,
    start: datetime,
    end: datetime,
    use_cache: bool = True
) -> tuple[pd.DataFrame, str, bool]:
    """
    Unified async OHLCV fetcher with smart fallback.
    
    Returns:
        (DataFrame, provider_name, from_cache)
    """
    normalized_symbol = normalize_symbol(symbol)
    
    # Check cache first
    if use_cache:
        cached_df = cache_manager.check_cache(normalized_symbol, "ohlcv", tf, start, end)
        if cached_df is not None:
            return cached_df, "cache", True
    
    # Determine provider priority
    primary = get_provider_for_symbol(normalized_symbol)
    providers = [primary] + [p for p in provider_health.get_ranked_providers() if p != primary]
    
    last_error = None
    
    for provider in providers:
        try:
            if provider == "dukascopy":
                df = await fetch_dukascopy_ohlcv_async(normalized_symbol, tf, start, end)
            elif provider == "binance":
                df = await fetch_binance_ohlcv_async(normalized_symbol, tf, start, end)
            elif provider == "alpaca":
                df = await fetch_alpaca_ohlcv_async(normalized_symbol, tf, start, end)
            else:
                continue
            
            if not df.empty:
                # Save to cache
                if use_cache:
                    cache_manager.save_cache(df, normalized_symbol, "ohlcv", tf, start, end)
                return df, provider, False
        
        except Exception as e:
            last_error = e
            logger.warning(f"{provider} fetch failed: {e}")
            continue
    
    if last_error:
        raise last_error
    
    return pd.DataFrame(), "none", False


async def fetch_ohlcv_chunked_parallel(
    symbol: str,
    tf: str,
    start: datetime,
    end: datetime,
    chunk_days: int = 7
) -> pd.DataFrame:
    """
    Fetch OHLCV in parallel chunks for large date ranges.
    """
    chunks = []
    current = start
    
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        chunks.append((current, chunk_end))
        current = chunk_end
    
    # Limit parallel requests
    semaphore = asyncio.Semaphore(settings.max_parallel_chunks)
    
    async def fetch_chunk(chunk_start, chunk_end):
        async with semaphore:
            df, _, _ = await fetch_ohlcv_unified_async(symbol, tf, chunk_start, chunk_end)
            return df
    
    tasks = [fetch_chunk(cs, ce) for cs, ce in chunks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Merge valid results
    valid_dfs = [r for r in results if isinstance(r, pd.DataFrame) and not r.empty]
    
    if not valid_dfs:
        return pd.DataFrame()
    
    merged = pd.concat(valid_dfs, ignore_index=True)
    merged = merged.drop_duplicates(subset=["time"], keep="last")
    merged = merged.sort_values("time").reset_index(drop=True)
    
    return merged


def df_to_candles(df: pd.DataFrame) -> List[Candle]:
    """Convert DataFrame to list of Candle models."""
    candles = []
    for _, row in df.iterrows():
        candles.append(Candle(
            time=row["time"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row.get("volume", 0)
        ))
    return candles
