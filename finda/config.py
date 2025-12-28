"""
Centralized Configuration for finda
Reads from environment variables and .env file
"""

from pydantic_settings import BaseSettings
from typing import Optional
from pathlib import Path
import logging


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # API Keys (from .env or environment)
    alpaca_api_key: Optional[str] = None
    alpaca_secret_key: Optional[str] = None
    
    # Cache settings
    cache_dir: str = ".finda_cache"
    cache_enabled: bool = True
    cache_ttl_hours: int = 24
    
    # Logging
    log_level: str = "INFO"
    
    # Performance
    max_parallel_chunks: int = 5
    chunk_size_days: int = 7
    request_timeout_seconds: int = 30
    
    # Provider priorities (fallback order)
    provider_priority: list = ["dukascopy", "binance", "alpaca"]
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


# Global settings instance
settings = Settings()

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("finda")


# Symbol mapping for unified routing
SYMBOL_MAP = {
    # Forex - route to Dukascopy
    "EURUSD": "EUR/USD",
    "EUR/USD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "GBP/USD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "USD/JPY": "USD/JPY",
    
    # Crypto - route to Binance
    "BTCUSDT": "BTC/USDT",
    "BTC/USDT": "BTC/USDT",
    "BTCUSD": "BTC/USD",
    "BTC/USD": "BTC/USD",
    "ETHUSDT": "ETH/USDT",
    "ETH/USDT": "ETH/USDT",
    
    # Stocks - route to Alpaca
    "AAPL": "AAPL",
    "TSLA": "TSLA",
    "MSFT": "MSFT",
}


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol to canonical form."""
    key = symbol.upper().replace(" ", "")
    return SYMBOL_MAP.get(key, symbol.upper())


def get_provider_for_symbol(symbol: str) -> str:
    """Determine best provider for a symbol."""
    normalized = normalize_symbol(symbol)
    
    # Forex pairs
    if "/" in normalized and any(c in normalized for c in ["EUR", "GBP", "USD", "JPY", "CHF", "AUD", "CAD"]):
        if "USDT" not in normalized:
            return "dukascopy"
    
    # Crypto
    if any(c in normalized for c in ["BTC", "ETH", "USDT"]):
        return "binance"
    
    # Default to Alpaca for stocks
    return "alpaca"


# Ensure cache directory exists
Path(settings.cache_dir).mkdir(parents=True, exist_ok=True)
