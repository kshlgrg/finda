import ccxt
import pandas as pd
from datetime import datetime
import logging
import math
from .utils import user_to_dt
from .exceptions import DataProviderError

logger = logging.getLogger(__name__)

def fetch_dukascopy_ticks(symbol, user_tf, user_start, user_end):
    from dukascopy_python import fetch, INTERVAL_TICK, OFFER_SIDE_BID
    symbol = symbol.strip().upper()
    try:
        start = user_to_dt(user_start, 'datetime')
        end = user_to_dt(user_end, 'datetime')
        # Dukascopy returns ticks (Quote data)
        df = fetch(symbol, INTERVAL_TICK, OFFER_SIDE_BID, start=start, end=end)
        if df is None or df.empty:
            raise DataProviderError(f"No Dukascopy tick data for {symbol}")

        bid = df["bidPrice"].tolist() if "bidPrice" in df else [0.0] * len(df)
        ask = df["askPrice"].tolist() if "askPrice" in df else [0.0] * len(df)
        bid_vol = df["bidVolume"].tolist() if "bidVolume" in df else [0.0] * len(df)
        ask_vol = df["askVolume"].tolist() if "askVolume" in df else [0.0] * len(df)
        real_vol = [0.0] * len(df) # Dukascopy is quote data, so no real trade volume usually
        times = list(df.index.to_pydatetime())

        return bid, ask, bid_vol, ask_vol, real_vol, times
    except ImportError:
        logger.error("dukascopy-python not installed")
        raise
    except Exception as e:
        logger.warning(f"Dukascopy tick fetch failed: {e}")
        raise DataProviderError(str(e))

def fetch_binance_ticks(symbol, user_tf, user_start, user_end):
    exchange = ccxt.binance({'enableRateLimit': True})
    symbol = symbol.strip().upper()
    binance_symbol = symbol.replace("/", "")
    try:
        since_str = user_to_dt(user_start, 'iso')
        end_str = user_to_dt(user_end, 'iso')
        since = int(datetime.fromisoformat(since_str).timestamp() * 1000)
        end_ms = int(datetime.fromisoformat(end_str).timestamp() * 1000)

        trades = []
        while since < end_ms:
            batch = exchange.fetch_trades(binance_symbol, since=since, limit=1000)
            if not batch: break
            for t in batch:
                if t['timestamp'] > end_ms: break
                trades.append(t)
            if len(batch) < 1000: break
            since = batch[-1]['timestamp'] + 1

        if not trades:
            raise DataProviderError(f"No Binance tick data for {symbol}")

        # Standardize Output
        # Binance returns TRADES. We need to map to the unified structure.
        # Unified structure seems to imply Quote data (Bid/Ask).
        # We will use Trade Price for both Bid and Ask to avoid None gaps,
        # or we could fill 0. But filling 0 creates massive price drops in charts.
        # Filling with trade price is safer for visualization, though technically inaccurate as a Quote.

        bid, ask, bid_vol, ask_vol, real_vol, times = [], [], [], [], [], []

        for t in trades:
            price = float(t['price'])
            amount = float(t['amount'])
            # side = t.get('side') # buy/sell

            # Since these are trades, we populate both bid/ask with the execution price
            # This ensures no None values.
            # Alternatively, we could keep the logic but use previous value (forward fill).
            # But here we will simple use the trade price.

            bid.append(price)
            ask.append(price)

            # Volume logic
            # If it was a buy (taker bought), it hit the ask.
            # If it was a sell (taker sold), it hit the bid.
            if t['side'] == 'buy':
                bid_vol.append(0.0)
                ask_vol.append(amount)
            else:
                bid_vol.append(amount)
                ask_vol.append(0.0)

            real_vol.append(amount)
            times.append(datetime.fromtimestamp(t['timestamp'] / 1000))

        return bid, ask, bid_vol, ask_vol, real_vol, times
    except Exception as e:
        logger.warning(f"Binance tick fetch failed: {e}")
        raise DataProviderError(str(e))

def fetch_alpaca_ticks(symbol, user_tf, user_start, user_end, api_key, secret_key):
    from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
    from alpaca.data.requests import StockTradesRequest, CryptoTradesRequest
    symbol = symbol.strip().upper()
    try:
        start = user_to_dt(user_start, 'iso')
        end = user_to_dt(user_end, 'iso')

        is_crypto = '/' in symbol or symbol in ['BTCUSD', 'ETHUSD']

        if is_crypto:
            client = CryptoHistoricalDataClient(api_key, secret_key)
            request = CryptoTradesRequest(
                symbol_or_symbols=symbol,
                start=datetime.fromisoformat(start),
                end=datetime.fromisoformat(end),
            )
            trades = client.get_crypto_trades(request).df
        else:
            client = StockHistoricalDataClient(api_key, secret_key)
            request = StockTradesRequest(
                symbol_or_symbols=symbol,
                start=datetime.fromisoformat(start),
                end=datetime.fromisoformat(end),
            )
            trades = client.get_stock_trades(request).df

        if trades.empty:
            raise DataProviderError(f"No Alpaca tick (trade) data for {symbol}")

        if isinstance(trades.index, pd.MultiIndex):
            trades = trades.loc[symbol]

        # Similar logic to Binance: Alpaca returns TRADES.
        # We fill bid/ask with trade price to ensure data integrity.

        # Note: Alpaca dataframe columns might vary (p vs price).
        # But 'price' and 'size' are standard for their latest SDK.

        # We handle potential NaN if alpaca returns it (unlikely for trade price)
        price = trades['price'].fillna(0.0).tolist() if 'price' in trades else [0.0]*len(trades)
        size = trades['size'].fillna(0.0).tolist() if 'size' in trades else [0.0]*len(trades)

        bid = price
        ask = price
        real_vol = size

        # We don't easily know side from Alpaca trades DF always (sometimes 'tks' contains conditions).
        # We'll set bid/ask vol to 0 or split evenly? Let's set to 0 to avoid false info.
        bid_vol = [0.0]*len(trades)
        ask_vol = [0.0]*len(trades)

        times = list(trades.index.to_pydatetime())
        return bid, ask, bid_vol, ask_vol, real_vol, times
    except Exception as e:
        logger.warning(f"Alpaca tick fetch failed: {e}")
        raise DataProviderError(str(e))

def fetch_unified_tick(symbol, user_tf, user_start, user_end, api_key=None, secret_key=None):
    errors = []

    # Try Dukascopy first
    try:
        logger.info(f"Attempting Dukascopy Ticks for {symbol}")
        return "dukascopy", fetch_dukascopy_ticks(symbol, user_tf, user_start, user_end)
    except Exception as e:
        errors.append(f"Dukascopy: {e}")

    # Try Binance next
    try:
        logger.info(f"Attempting Binance Ticks for {symbol}")
        return "binance", fetch_binance_ticks(symbol, user_tf, user_start, user_end)
    except Exception as e:
        errors.append(f"Binance: {e}")

    # Try Alpaca if keys present
    if api_key and secret_key:
        try:
            logger.info(f"Attempting Alpaca Ticks for {symbol}")
            return "alpaca", fetch_alpaca_ticks(symbol, user_tf, user_start, user_end, api_key, secret_key)
        except Exception as e:
            errors.append(f"Alpaca: {e}")

    # Raise error if all failed
    logger.error(f"All tick providers failed for {symbol}: {errors}")
    raise DataProviderError(f"All providers failed: {'; '.join(errors)}")
