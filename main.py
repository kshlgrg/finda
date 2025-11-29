from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

# Import from package
from finda import fetch_unified_ohclv, fetch_unified_tick
from finda.tick_fetcher import fetch_dukascopy_ticks, fetch_binance_ticks, fetch_alpaca_ticks
from finda.exceptions import DataProviderError, FindaError

# Load secrets
load_dotenv()
API_KEY = os.getenv("ALPACA_API_KEY", "")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("finda-api")

app = FastAPI(title="Finda API", description="Unified Financial Data API", version="1.0.0")

# --- Pydantic Models ---

class OHLCVItem(BaseModel):
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: float

class OHLCVResponse(BaseModel):
    symbol: str
    timeframe: str
    data: List[OHLCVItem]

class TickItem(BaseModel):
    time: str
    bid: float
    ask: float
    bid_volume: float
    ask_volume: float
    volume: float

class TickResponse(BaseModel):
    symbol: str
    provider: str
    timeframe: str
    data: List[TickItem]

# --- Endpoints ---

@app.get("/ohlcv", response_model=OHLCVResponse)
def get_ohlcv(
    symbol: str = Query(..., example="BTC/USDT", description="Symbol to fetch"),
    tf: str = Query(..., example="min1", description="Timeframe (e.g. min1, 1h)"),
    start: str = Query(..., example="2025-08-16-22-00-00", description="Start time (YYYY-MM-DD-HH-MM-SS)"),
    end: str = Query(..., example="2025-08-17-01-00-00", description="End time")
):
    """
    Unified OHLCV endpoint.
    Automatically tries Dukascopy -> Binance -> Alpaca.
    """
    try:
        data = fetch_unified_ohclv(symbol, tf, start, end, api_key=API_KEY, secret_key=SECRET_KEY)
        opens, highs, lows, closes, volumes, times = data

        result = [
            OHLCVItem(
                time=str(t),
                open=o, high=h, low=l, close=c, volume=v
            )
            for t, o, h, l, c, v in zip(times, opens, highs, lows, closes, volumes)
        ]
        return OHLCVResponse(symbol=symbol, timeframe=tf, data=result)

    except DataProviderError as e:
        logger.error(f"Data provider error: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except FindaError as e:
        logger.error(f"Finda error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/tick", response_model=TickResponse)
def get_tick(
    symbol: str = Query(..., example="BTC/USDT"),
    tf: str = Query("tick", example="tick"),
    start: str = Query(..., example="2025-08-16-22-00-00"),
    end: str = Query(..., example="2025-08-17-01-00-00"),
    provider: Literal["unified", "dukascopy", "binance", "alpaca"] = Query("unified", description="Data provider")
):
    """
    Tick data endpoint.
    If provider is 'unified', it will attempt auto-fallback.
    """
    try:
        prov_name = provider

        if provider == "unified":
            prov_name, data = fetch_unified_tick(symbol, tf, start, end, API_KEY, SECRET_KEY)
            b, a, bv, av, v, t = data
        elif provider == "dukascopy":
            b, a, bv, av, v, t = fetch_dukascopy_ticks(symbol, tf, start, end)
        elif provider == "binance":
            b, a, bv, av, v, t = fetch_binance_ticks(symbol, tf, start, end)
        elif provider == "alpaca":
            b, a, bv, av, v, t = fetch_alpaca_ticks(symbol, tf, start, end, API_KEY, SECRET_KEY)
        else:
            raise HTTPException(status_code=400, detail="Unknown provider")

        result = [
            TickItem(
                time=str(time_val),
                bid=bid_val,
                ask=ask_val,
                bid_volume=bid_vol_val,
                ask_volume=ask_vol_val,
                volume=real_vol_val
            )
            for time_val, bid_val, ask_val, bid_vol_val, ask_vol_val, real_vol_val in zip(t, b, a, bv, av, v)
        ]

        return TickResponse(symbol=symbol, provider=prov_name, timeframe=tf, data=result)

    except DataProviderError as e:
        logger.error(f"Data provider error: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except FindaError as e:
        logger.error(f"Finda error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
