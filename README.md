# Finda Pro-Grade Data Engine v2.0

**Institutional-grade async financial data pipeline** with Parquet caching, multi-provider fallback, and WebSocket streaming.

## Features

- ⚡ **Async Architecture** - Non-blocking I/O with `asyncio` and `ccxt.async_support`
- 💾 **Parquet Caching** - High-performance local cache with smart data merging
- 🔄 **Smart Fallback** - Auto-switch providers on failure with health tracking
- 📊 **Pydantic Models** - Type-safe Candle, Tick, and MarketMetadata schemas
- 🔌 **WebSocket Streaming** - Real-time tick and candle subscriptions
- 🌍 **Multi-Provider** - Dukascopy (Forex), Binance (Crypto), Alpaca (Stocks)

## Installation

```bash
pip install finda
```

With Alpaca support:
```bash
pip install finda[alpaca]
```

## Quick Start

### Async (Recommended)
```python
import asyncio
from finda import Finda

async def main():
    f = Finda()
    df = await f.get_candles("EUR/USD", "1m", "2024-01-01", "2024-01-02")
    print(df)

asyncio.run(main())
```

### FastAPI Server
```bash
uvicorn main:app --reload
```

Endpoints:
- `GET /ohlcv` - OHLCV candles with caching
- `GET /tick` - Tick-level Bid/Ask data
- `GET /markets` - Provider health & available symbols
- `GET /cache/stats` - Cache hit/miss statistics

### Legacy Sync
```python
from finda import fetch_unified_ohclv
o, h, l, c, v, t = fetch_unified_ohclv("EUR/USD", "1m", "2024-01-01", "2024-01-02")
```

## Configuration

Create `.env` file:
```env
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
CACHE_DIR=.finda_cache
LOG_LEVEL=INFO
```

## Data Sources

| Provider | Asset Class | Data Types |
|----------|-------------|------------|
| Dukascopy | Forex | OHLCV, Bid/Ask Ticks |
| Binance | Crypto | OHLCV, Trades |
| Alpaca | Stocks | OHLCV, Trades |

## License

MIT
