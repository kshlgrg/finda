"""
Live Streamer for finda Pro-Grade
WebSocket-based real-time tick and candle feeds
"""

import asyncio
from datetime import datetime, timezone
from typing import Callable, Optional, Awaitable
import logging

try:
    import ccxt.async_support as ccxt_async
except ImportError:
    ccxt_async = None

from .schemas import Tick, Candle
from .config import settings, logger


class LiveStreamer:
    """
    Unified WebSocket interface for real-time market data.
    
    Supports:
    - Binance trade/ticker streams via ccxt
    - Alpaca stock quotes (beta)
    """
    
    def __init__(self):
        self._running = False
        self._exchange = None
        self._tasks = []
    
    async def start(self):
        """Initialize WebSocket connection."""
        if ccxt_async is None:
            raise ImportError("ccxt.async_support required for live streaming")
        
        self._exchange = ccxt_async.binance({
            "enableRateLimit": True,
        })
        self._running = True
        logger.info("LiveStreamer started")
    
    async def stop(self):
        """Close WebSocket connection."""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
        
        if self._exchange:
            await self._exchange.close()
            self._exchange = None
        
        logger.info("LiveStreamer stopped")
    
    async def subscribe_trades(
        self, 
        symbol: str, 
        callback: Callable[[Tick], Awaitable[None]]
    ):
        """
        Subscribe to real-time trade stream.
        
        Args:
            symbol: Trading pair (e.g., BTC/USDT)
            callback: Async function to call with each tick
        """
        if not self._running:
            await self.start()
        
        binance_symbol = symbol.replace("/", "").upper()
        
        async def stream_loop():
            while self._running:
                try:
                    # Use watch_trades for WebSocket streaming
                    trades = await self._exchange.watch_trades(binance_symbol)
                    
                    for trade in trades:
                        tick = Tick(
                            time=datetime.fromtimestamp(
                                trade["timestamp"] / 1000, 
                                tz=timezone.utc
                            ),
                            bid=trade["price"] if trade.get("side") == "sell" else None,
                            ask=trade["price"] if trade.get("side") == "buy" else None,
                            volume=trade["amount"]
                        )
                        await callback(tick)
                
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Trade stream error: {e}")
                    await asyncio.sleep(1)
        
        task = asyncio.create_task(stream_loop())
        self._tasks.append(task)
        
        return task
    
    async def subscribe_ticker(
        self,
        symbol: str,
        callback: Callable[[dict], Awaitable[None]]
    ):
        """
        Subscribe to real-time ticker (bid/ask) stream.
        
        Args:
            symbol: Trading pair
            callback: Async function to call with ticker data
        """
        if not self._running:
            await self.start()
        
        binance_symbol = symbol.replace("/", "").upper()
        
        async def stream_loop():
            while self._running:
                try:
                    ticker = await self._exchange.watch_ticker(binance_symbol)
                    
                    await callback({
                        "symbol": symbol,
                        "bid": ticker.get("bid"),
                        "ask": ticker.get("ask"),
                        "last": ticker.get("last"),
                        "volume": ticker.get("baseVolume"),
                        "time": datetime.now(timezone.utc)
                    })
                
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Ticker stream error: {e}")
                    await asyncio.sleep(1)
        
        task = asyncio.create_task(stream_loop())
        self._tasks.append(task)
        
        return task
    
    async def subscribe_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        callback: Callable[[Candle], Awaitable[None]]
    ):
        """
        Subscribe to real-time candle stream.
        
        Args:
            symbol: Trading pair
            timeframe: e.g., "1m", "5m"
            callback: Async function to call with each candle
        """
        if not self._running:
            await self.start()
        
        binance_symbol = symbol.replace("/", "").upper()
        
        async def stream_loop():
            while self._running:
                try:
                    ohlcv = await self._exchange.watch_ohlcv(
                        binance_symbol, timeframe
                    )
                    
                    for bar in ohlcv:
                        candle = Candle(
                            time=datetime.fromtimestamp(
                                bar[0] / 1000, 
                                tz=timezone.utc
                            ),
                            open=bar[1],
                            high=bar[2],
                            low=bar[3],
                            close=bar[4],
                            volume=bar[5]
                        )
                        await callback(candle)
                
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"OHLCV stream error: {e}")
                    await asyncio.sleep(1)
        
        task = asyncio.create_task(stream_loop())
        self._tasks.append(task)
        
        return task


# Global instance
live_streamer = LiveStreamer()


# Helper functions
def calculate_notional(price: float, size: float, contract_size: float = 1.0) -> float:
    """
    Calculate notional value of a trade.
    
    Args:
        price: Current price
        size: Position size
        contract_size: Contract multiplier (100000 for forex, 1 for crypto)
    
    Returns:
        Notional value in quote currency
    """
    return price * size * contract_size


def get_contract_size(symbol: str) -> float:
    """Get contract size for a symbol."""
    symbol_upper = symbol.upper()
    
    # Forex standard lot = 100,000 units
    if any(c in symbol_upper for c in ["EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]):
        if "/" in symbol_upper or len(symbol_upper) == 6:
            return 100_000.0
    
    # Crypto = 1 unit
    return 1.0
