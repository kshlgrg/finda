"""
Finda Pro-Grade Data Engine v2.0.0
Institutional-grade async financial data pipeline
"""

__version__ = "2.0.0"
__author__ = "Kushal Garg"

# Core schemas
from .schemas import (
    Candle,
    Tick,
    OHLCVResponse,
    TickResponse,
    MarketMetadata,
    ProviderStatus,
    MarketsResponse
)

# Config
from .config import settings, normalize_symbol, get_provider_for_symbol

# Cache
from .cache_manager import CacheManager, cache_manager

# Async fetchers (primary API)
from .async_ohlcv import (
    fetch_ohlcv_unified_async,
    fetch_ohlcv_chunked_parallel,
    fetch_binance_ohlcv_async,
    fetch_dukascopy_ohlcv_async,
    fetch_alpaca_ohlcv_async,
    df_to_candles,
    provider_health,
    ProviderHealth
)

from .async_tick import (
    fetch_tick_unified_async,
    fetch_dukascopy_tick_async,
    fetch_binance_tick_async,
    fetch_alpaca_tick_async,
    df_to_ticks
)

# Live streaming
from .live_streamer import (
    LiveStreamer,
    live_streamer,
    calculate_notional,
    get_contract_size
)

# Legacy sync fetchers (deprecated but maintained)
from .ohlcv_fetcher import (
    fetch_unified_ohclv,
    fetch_dukascopy_ohclv,
    fetch_binance_ohclv,
    fetch_alpaca_ohclv
)

from .tick_fetcher import (
    fetch_unified_tick,
    fetch_dukascopy_ticks,
    fetch_binance_ticks,
    fetch_alpaca_ticks
)


# Convenience class
class Finda:
    """
    Main interface for finda data engine.
    
    Example:
        from finda import Finda
        f = Finda()
        df = await f.get_candles("EUR/USD", "1m", "2024-01-01", "2024-01-02")
    """
    
    def __init__(self):
        self.cache = cache_manager
        self.health = provider_health
    
    async def get_candles(
        self, 
        symbol: str, 
        tf: str, 
        start: str, 
        end: str,
        use_cache: bool = True
    ):
        """Fetch OHLCV candles async."""
        from datetime import datetime
        start_dt = datetime.fromisoformat(start) if isinstance(start, str) else start
        end_dt = datetime.fromisoformat(end) if isinstance(end, str) else end
        
        df, provider, cached = await fetch_ohlcv_unified_async(
            symbol, tf, start_dt, end_dt, use_cache=use_cache
        )
        return df
    
    async def get_ticks(
        self,
        symbol: str,
        start: str,
        end: str,
        use_cache: bool = True
    ):
        """Fetch tick data async."""
        from datetime import datetime
        start_dt = datetime.fromisoformat(start) if isinstance(start, str) else start
        end_dt = datetime.fromisoformat(end) if isinstance(end, str) else end
        
        df, provider, cached = await fetch_tick_unified_async(
            symbol, start_dt, end_dt, use_cache=use_cache
        )
        return df
    
    def get_candles_sync(self, symbol: str, tf: str, start: str, end: str):
        """Sync wrapper for candles (legacy compatibility)."""
        result = fetch_unified_ohclv(
            symbol, tf, start, end,
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key
        )
        return result


__all__ = [
    # Version
    "__version__",
    
    # Main class
    "Finda",
    
    # Schemas
    "Candle",
    "Tick",
    "OHLCVResponse",
    "TickResponse",
    "MarketMetadata",
    
    # Config
    "settings",
    
    # Cache
    "cache_manager",
    "CacheManager",
    
    # Async fetchers
    "fetch_ohlcv_unified_async",
    "fetch_tick_unified_async",
    "fetch_ohlcv_chunked_parallel",
    
    # Health
    "provider_health",
    
    # Live streaming
    "LiveStreamer",
    "live_streamer",
    "calculate_notional",
    
    # Legacy sync
    "fetch_unified_ohclv",
    "fetch_unified_tick",
]
