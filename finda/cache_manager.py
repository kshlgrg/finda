"""
Cache Manager for finda
High-performance Parquet-based caching with smart merging
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Callable, Awaitable, List
import hashlib
import logging

from .config import settings

logger = logging.getLogger("finda.cache")


class CacheManager:
    """Manages Parquet-based caching for time-series data."""
    
    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = Path(cache_dir or settings.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {"hits": 0, "misses": 0}
    
    def get_cache_key(self, symbol: str, data_type: str, tf: str, 
                      start: datetime, end: datetime) -> Path:
        """Generate cache file path."""
        safe_symbol = symbol.replace("/", "_").replace(" ", "_")
        date_key = f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
        filename = f"{safe_symbol}_{data_type}_{tf}_{date_key}.parquet"
        return self.cache_dir / filename
    
    def get_cache_hash(self, symbol: str, tf: str, start: datetime, end: datetime) -> str:
        """Generate hash for cache validation."""
        key = f"{symbol}_{tf}_{start.isoformat()}_{end.isoformat()}"
        return hashlib.md5(key.encode()).hexdigest()[:12]
    
    def check_cache(self, symbol: str, data_type: str, tf: str,
                    start: datetime, end: datetime) -> Optional[pd.DataFrame]:
        """Check if cached data exists and is valid."""
        if not settings.cache_enabled:
            return None
        
        cache_path = self.get_cache_key(symbol, data_type, tf, start, end)
        
        if cache_path.exists():
            # Check TTL
            file_age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)
            if file_age < timedelta(hours=settings.cache_ttl_hours):
                try:
                    df = pd.read_parquet(cache_path)
                    self.stats["hits"] += 1
                    logger.debug(f"Cache HIT: {cache_path.name}")
                    return df
                except Exception as e:
                    logger.warning(f"Cache read error: {e}")
        
        self.stats["misses"] += 1
        return None
    
    def save_cache(self, df: pd.DataFrame, symbol: str, data_type: str, 
                   tf: str, start: datetime, end: datetime) -> bool:
        """Save data to cache."""
        if not settings.cache_enabled or df.empty:
            return False
        
        cache_path = self.get_cache_key(symbol, data_type, tf, start, end)
        
        try:
            df.to_parquet(cache_path, index=False)
            logger.debug(f"Cache SAVE: {cache_path.name}")
            return True
        except Exception as e:
            logger.warning(f"Cache save error: {e}")
            return False
    
    def merge_data(self, existing: pd.DataFrame, new: pd.DataFrame, 
                   time_col: str = "time") -> pd.DataFrame:
        """Merge existing and new data, removing duplicates."""
        if existing.empty:
            return new
        if new.empty:
            return existing
        
        combined = pd.concat([existing, new], ignore_index=True)
        combined = combined.drop_duplicates(subset=[time_col], keep="last")
        combined = combined.sort_values(time_col).reset_index(drop=True)
        
        return combined
    
    async def get_or_fetch(
        self, 
        symbol: str, 
        data_type: str,
        tf: str, 
        start: datetime, 
        end: datetime,
        fetch_func: Callable[..., Awaitable[pd.DataFrame]]
    ) -> tuple[pd.DataFrame, bool]:
        """
        Get data from cache or fetch from API.
        
        Returns:
            (DataFrame, cached: bool)
        """
        # Check cache first
        cached_df = self.check_cache(symbol, data_type, tf, start, end)
        if cached_df is not None:
            return cached_df, True
        
        # Fetch from API
        df = await fetch_func(symbol, tf, start, end)
        
        # Save to cache
        if not df.empty:
            self.save_cache(df, symbol, data_type, tf, start, end)
        
        return df, False
    
    def clear_cache(self, symbol: Optional[str] = None) -> int:
        """Clear cache files. Returns count of deleted files."""
        deleted = 0
        pattern = f"{symbol.replace('/', '_')}*" if symbol else "*"
        
        for f in self.cache_dir.glob(f"{pattern}.parquet"):
            f.unlink()
            deleted += 1
        
        logger.info(f"Cleared {deleted} cache files")
        return deleted
    
    def get_stats(self) -> dict:
        """Get cache statistics."""
        cache_files = list(self.cache_dir.glob("*.parquet"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        hit_rate = 0.0
        if (self.stats["hits"] + self.stats["misses"]) > 0:
            hit_rate = self.stats["hits"] / (self.stats["hits"] + self.stats["misses"])
        
        return {
            "hits": self.stats["hits"],
            "misses": self.stats["misses"],
            "hit_rate": hit_rate,
            "file_count": len(cache_files),
            "total_size_mb": round(total_size / 1024 / 1024, 2)
        }


# Global cache manager instance
cache_manager = CacheManager()
