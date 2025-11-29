import ccxt
import pandas as pd
from datetime import datetime
import logging
from .utils import user_to_dt, user_to_dukascopy_tf, user_to_binance_tf, user_to_alpaca_tf
from .exceptions import DataProviderError

logger = logging.getLogger(__name__)

def fetch_dukascopy_ohclv(symbol, user_tf, user_start, user_end):
    from dukascopy_python import fetch, OFFER_SIDE_BID
    symbol = symbol.strip().upper()
    try:
        tf = user_to_dukascopy_tf(user_tf)
        start = user_to_dt(user_start, 'datetime')
        end = user_to_dt(user_end, 'datetime')
        df = fetch(symbol, tf, OFFER_SIDE_BID, start=start, end=end)
        if df is None or df.empty:
            raise DataProviderError(f"No Dukascopy data for {symbol}")
        opens, highs, lows, closes = df["open"].tolist(), df["high"].tolist(), df["low"].tolist(), df["close"].tolist()
        volumes = df["volume"].tolist() if "volume" in df else [0] * len(df)
        times = list(df.index.to_pydatetime())
        return opens, highs, lows, closes, volumes, times
    except ImportError:
        logger.error("dukascopy-python not installed")
        raise
    except Exception as e:
        logger.warning(f"Dukascopy fetch failed: {e}")
        raise DataProviderError(str(e))

def fetch_binance_ohclv(symbol, user_tf, user_start, user_end):
    exchange = ccxt.binance({'enableRateLimit': True})
    symbol = symbol.strip().upper()
    binance_symbol = symbol.replace("/", "")
    try:
        tf = user_to_binance_tf(user_tf)
        since_str, end_str = user_to_dt(user_start, 'iso'), user_to_dt(user_end, 'iso')
        since = int(datetime.fromisoformat(since_str).timestamp() * 1000)
        end_ms = int(datetime.fromisoformat(end_str).timestamp() * 1000)
        limit, all_ohlcv = 1000, []

        while since < end_ms:
            ohlcv = exchange.fetch_ohlcv(binance_symbol, tf, since, limit)
            if not ohlcv: break
            filtered = [c for c in ohlcv if c[0] <= end_ms]
            all_ohlcv.extend(filtered)
            if len(ohlcv) < limit: break
            since = ohlcv[-1][0] + 1

        if not all_ohlcv:
            raise DataProviderError(f"No Binance data for {symbol}")

        timestamps, opens, highs, lows, closes, volumes = zip(*all_ohlcv)
        times = [datetime.fromtimestamp(t / 1000) for t in timestamps]
        return list(opens), list(highs), list(lows), list(closes), list(volumes), times
    except Exception as e:
        logger.warning(f"Binance fetch failed: {e}")
        raise DataProviderError(str(e))

def fetch_alpaca_ohclv(symbol, user_tf, user_start, user_end, api_key, secret_key):
    from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
    from alpaca.data.timeframe import TimeFrame
    symbol = symbol.strip().upper()
    try:
        tf_str = user_to_alpaca_tf(user_tf)
        # TODO: Better mapping logic in utils or here
        tf_map = {
            '1Min': TimeFrame.Minute,
            '5Min': TimeFrame(5, TimeFrame.Minute),
            '15Min': TimeFrame(15, TimeFrame.Minute),
            '30Min': TimeFrame(30, TimeFrame.Minute),
            '1Hour': TimeFrame.Hour,
            '1Day': TimeFrame.Day,
            '1Week': TimeFrame.Week
        }

        start = user_to_dt(user_start, 'iso')
        end = user_to_dt(user_end, 'iso')

        # Check if crypto
        is_crypto = '/' in symbol or symbol in ['BTCUSD', 'ETHUSD']

        if is_crypto:
            client = CryptoHistoricalDataClient(api_key, secret_key)
            request = CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf_map.get(tf_str, TimeFrame.Minute), # Default or fail?
                start=datetime.fromisoformat(start),
                end=datetime.fromisoformat(end)
            )
            bars = client.get_crypto_bars(request).df
        else:
            client = StockHistoricalDataClient(api_key, secret_key)
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf_map.get(tf_str, TimeFrame.Minute),
                start=datetime.fromisoformat(start),
                end=datetime.fromisoformat(end),
                feed='sip'
            )
            bars = client.get_stock_bars(request).df

        if bars.empty:
             raise DataProviderError(f"No Alpaca data for {symbol}")

        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.loc[symbol]

        bars = bars.sort_index()
        opens, highs, lows, closes, volumes = bars['open'].tolist(), bars['high'].tolist(), bars['low'].tolist(), bars['close'].tolist(), bars['volume'].tolist()
        times = list(bars.index.to_pydatetime())
        return opens, highs, lows, closes, volumes, times
    except Exception as e:
        logger.warning(f"Alpaca fetch failed: {e}")
        raise DataProviderError(str(e))

def fetch_unified_ohclv(symbol, user_tf, user_start, user_end, api_key=None, secret_key=None):
    errors = []

    # Try Dukascopy
    try:
        logger.info(f"Attempting Dukascopy for {symbol}")
        return fetch_dukascopy_ohclv(symbol, user_tf, user_start, user_end)
    except Exception as e:
        errors.append(f"Dukascopy: {e}")

    # Try Binance
    try:
        logger.info(f"Attempting Binance for {symbol}")
        return fetch_binance_ohclv(symbol, user_tf, user_start, user_end)
    except Exception as e:
        errors.append(f"Binance: {e}")

    # Try Alpaca
    if api_key and secret_key:
        try:
            logger.info(f"Attempting Alpaca for {symbol}")
            return fetch_alpaca_ohclv(symbol, user_tf, user_start, user_end, api_key, secret_key)
        except Exception as e:
            errors.append(f"Alpaca: {e}")
    else:
        errors.append("Alpaca: Skipped (No keys)")

    # If all failed
    error_msg = "; ".join(errors)
    logger.error(f"All providers failed for {symbol}: {error_msg}")
    raise DataProviderError(f"All providers failed: {error_msg}")
