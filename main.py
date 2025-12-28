"""
Finda Pro-Grade Data Engine
FastAPI Application with Async Endpoints

v2.0.0 - Institutional-grade financial data pipeline
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from typing import Optional, List
import logging

from finda.schemas import (
    Candle, Tick, OHLCVResponse, TickResponse,
    MarketsResponse, ProviderStatus, MarketMetadata
)
from finda.config import settings, logger
from finda.async_ohlcv import (
    fetch_ohlcv_unified_async, 
    fetch_ohlcv_chunked_parallel,
    df_to_candles,
    provider_health
)
from finda.async_tick import (
    fetch_tick_unified_async,
    df_to_ticks
)
from finda.cache_manager import cache_manager
from finda.live_streamer import calculate_notional, get_contract_size

# Initialize FastAPI
app = FastAPI(
    title="Finda Pro-Grade Data Engine",
    description="Institutional-grade async financial data pipeline with caching",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def parse_datetime(s: str) -> datetime:
    """Parse flexible datetime string."""
    # Try ISO format first
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except:
        pass
    
    # Try hyphen-separated format: 2025-08-16-22-00-00
    parts = [int(p) for p in s.split("-")]
    while len(parts) < 6:
        parts.append(0)
    return datetime(*parts)


@app.get("/ohlcv", response_model=OHLCVResponse)
async def get_ohlcv(
    symbol: str = Query(..., example="EUR/USD"),
    tf: str = Query(..., example="1m"),
    start: str = Query(..., example="2025-01-01"),
    end: str = Query(..., example="2025-01-02"),
    use_cache: bool = Query(True, description="Use Parquet cache"),
    parallel: bool = Query(False, description="Use parallel chunked fetching")
):
    """
    Fetch OHLCV candle data.
    
    - Async with smart fallback across providers
    - Parquet caching for performance
    - Parallel chunked fetching for large ranges
    """
    try:
        start_dt = parse_datetime(start)
        end_dt = parse_datetime(end)
        
        if parallel:
            df = await fetch_ohlcv_chunked_parallel(symbol, tf, start_dt, end_dt)
            provider = "parallel"
            cached = False
        else:
            df, provider, cached = await fetch_ohlcv_unified_async(
                symbol, tf, start_dt, end_dt, use_cache=use_cache
            )
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {symbol}")
        
        candles = df_to_candles(df)
        
        return OHLCVResponse(
            symbol=symbol,
            timeframe=tf,
            provider=provider,
            count=len(candles),
            data=candles,
            cached=cached
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"OHLCV error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tick", response_model=TickResponse)
async def get_tick(
    symbol: str = Query(..., example="EUR/USD"),
    start: str = Query(..., example="2025-01-01"),
    end: str = Query(..., example="2025-01-01-01-00-00"),
    provider: Optional[str] = Query(None, example="dukascopy"),
    use_cache: bool = Query(True)
):
    """
    Fetch tick-level Bid/Ask data.
    
    - True microstructure data from Dukascopy
    - Trade-inferred bid/ask from Binance
    - Smart fallback across providers
    """
    try:
        start_dt = parse_datetime(start)
        end_dt = parse_datetime(end)
        
        df, prov, cached = await fetch_tick_unified_async(
            symbol, start_dt, end_dt, provider=provider, use_cache=use_cache
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No tick data for {symbol}")
        
        ticks = df_to_ticks(df)
        
        return TickResponse(
            symbol=symbol,
            provider=prov,
            count=len(ticks),
            data=ticks,
            cached=cached
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Tick error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/markets", response_model=MarketsResponse)
async def get_markets():
    """
    Get available markets and provider health status.
    
    - Lists supported symbols
    - Shows provider health and latency
    - Displays supported timeframes
    """
    providers = []
    for name, status in provider_health.status.items():
        providers.append(ProviderStatus(
            name=name,
            healthy=status["healthy"],
            last_error=status.get("last_error"),
            latency_ms=status.get("latency_ms")
        ))
    
    # Sample symbols
    symbols = [
        "EUR/USD", "GBP/USD", "USD/JPY",
        "BTC/USDT", "ETH/USDT",
        "AAPL", "TSLA", "MSFT"
    ]
    
    timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
    
    return MarketsResponse(
        providers=providers,
        available_symbols=symbols,
        supported_timeframes=timeframes
    )


@app.get("/cache/stats")
async def get_cache_stats():
    """
    Get cache statistics.
    
    - Hit/miss ratio
    - Total cached data size
    - File count
    """
    return cache_manager.get_stats()


@app.delete("/cache/clear")
async def clear_cache(symbol: Optional[str] = Query(None)):
    """
    Clear cache files.
    
    - Optionally filter by symbol
    """
    deleted = cache_manager.clear_cache(symbol)
    return {"deleted": deleted, "symbol": symbol or "all"}


@app.get("/notional")
async def calculate_notional_value(
    price: float = Query(..., example=1.1000),
    size: float = Query(..., example=1.0),
    symbol: str = Query("EUR/USD", example="EUR/USD")
):
    """
    Calculate notional trade value.
    
    - Auto-detects contract size based on symbol
    - Forex: 100,000 units per lot
    - Crypto: 1 unit per lot
    """
    contract_size = get_contract_size(symbol)
    notional = calculate_notional(price, size, contract_size)
    
    return {
        "symbol": symbol,
        "price": price,
        "size": size,
        "contract_size": contract_size,
        "notional_value": notional
    }


@app.get("/health")
async def health_check():
    """API health check."""
    return {
        "status": "healthy",
        "version": "2.0.0",
        "cache_enabled": settings.cache_enabled,
        "providers": list(provider_health.status.keys())
    }


# Legacy sync endpoints preserved for backward compatibility
from finda.ohlcv_fetcher import fetch_unified_ohclv
from finda.tick_fetcher import fetch_dukascopy_ticks, fetch_binance_ticks

@app.get("/legacy/ohlcv")
def get_ohlcv_legacy(
    symbol: str = Query(...),
    tf: str = Query(...),
    start: str = Query(...),
    end: str = Query(...)
):
    """Legacy synchronous OHLCV endpoint (deprecated)."""
    try:
        data = fetch_unified_ohclv(
            symbol, tf, start, end,
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key
        )
        if data is None:
            return {"error": "No data"}
        
        opens, highs, lows, closes, volumes, times = data
        return {
            "symbol": symbol,
            "timeframe": tf,
            "data": [
                {"time": str(times[i]), "open": opens[i], "high": highs[i], 
                 "low": lows[i], "close": closes[i], "volume": volumes[i]}
                for i in range(len(times))
            ]
        }
    except Exception as e:
        return {"error": str(e)}
