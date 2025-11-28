from fastapi import FastAPI, Query
from dotenv import load_dotenv
from finda.ohlcv_fetcher import fetch_unified_ohclv
from finda.tick_fetcher import fetch_dukascopy_ticks, fetch_binance_ticks, fetch_alpaca_ticks

import os

# Load secrets from .env
load_dotenv()
API_KEY = os.getenv("ALPACA_API_KEY", "")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

# Import your actual fetching functions from your finda package/module
# If not ready as a package, copy them here temporarily for quick testing
from finda.ohlcv_fetcher import fetch_unified_ohclv
from finda.tick_fetcher import fetch_dukascopy_ticks, fetch_binance_ticks, fetch_alpaca_ticks

app = FastAPI()

@app.get("/ohlcv")
def get_ohlcv(
    symbol: str = Query(..., example="BTC/USDT"),
    tf: str = Query(..., example="min1"),
    start: str = Query(..., example="2025-08-16-22-00-00"),
    end: str = Query(..., example="2025-08-17-01-00-00")
):
    """
    Unified OHLCV endpoint.
    Calls your unified function and returns data in JSON.
    """
    try:
        data = fetch_unified_ohclv(symbol, tf, start, end, api_key=API_KEY, secret_key=SECRET_KEY)
        opens, highs, lows, closes, volumes, times = data
        result = [
            {
                "time": str(times[i]),
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i],
                "volume": volumes[i]
            }
            for i in range(len(times))
        ]
        return {"symbol": symbol, "timeframe": tf, "data": result}
    except Exception as e:
        return {"error": str(e)}

@app.get("/tick")
def get_tick(
    symbol: str = Query(..., example="BTC/USDT"),
    tf: str = Query(..., example="min1"),
    start: str = Query(..., example="2025-08-16-22-00-00"),
    end: str = Query(..., example="2025-08-17-01-00-00"),
    provider: str = Query("dukascopy", example="dukascopy")  # You can choose dukascopy, binance, alpaca
):
    """
    Tick data endpoint.
    Lets you pick the provider (dukascopy, binance, alpaca) for max flexibility.
    """
    try:
        if provider == "dukascopy":
            b, a, bv, av, v, t = fetch_dukascopy_ticks(symbol, tf, start, end)
        elif provider == "binance":
            b, a, bv, av, v, t = fetch_binance_ticks(symbol, tf, start, end)
        elif provider == "alpaca":
            b, a, bv, av, v, t = fetch_alpaca_ticks(symbol, tf, start, end, API_KEY, SECRET_KEY)
        else:
            return {"error": "Unknown provider"}
        # Result for each tick is a dict; you can add more info if your function returns more fields
        result = [
            {
                "time": str(t[i]),
                "bid": b[i],
                "ask": a[i],
                "bid_volume": bv[i],
                "ask_volume": av[i],
                "volume": v[i]
            }
            for i in range(len(t))
        ]
        return {"symbol": symbol, "provider": provider, "timeframe": tf, "data": result}
    except Exception as e:
        return {"error": str(e)}

# You can add more endpoints or features as you wish!
