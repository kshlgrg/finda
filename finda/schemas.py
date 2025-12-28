"""
Pydantic Schemas for finda Pro-Grade Data Engine
Type-safe models for Candles, Ticks, and Market Metadata
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


class Candle(BaseModel):
    """OHLCV candle with strict typing."""
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class Tick(BaseModel):
    """Tick-level bid/ask data."""
    time: datetime
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_volume: float = 0.0
    ask_volume: float = 0.0
    volume: float = 0.0
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class MarketMetadata(BaseModel):
    """Metadata about a market/symbol."""
    symbol: str
    provider: str
    timeframes: List[str] = Field(default_factory=list)
    is_healthy: bool = True
    last_error: Optional[str] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class OHLCVResponse(BaseModel):
    """API response for OHLCV data."""
    symbol: str
    timeframe: str
    provider: str
    count: int
    data: List[Candle]
    cached: bool = False


class TickResponse(BaseModel):
    """API response for tick data."""
    symbol: str
    provider: str
    count: int
    data: List[Tick]
    cached: bool = False


class ProviderStatus(BaseModel):
    """Health status of a data provider."""
    name: str
    healthy: bool
    last_error: Optional[str] = None
    latency_ms: Optional[float] = None
    last_checked: datetime = Field(default_factory=datetime.utcnow)


class MarketsResponse(BaseModel):
    """Response for /markets endpoint."""
    providers: List[ProviderStatus]
    available_symbols: List[str]
    supported_timeframes: List[str]
