from datetime import datetime
import re
import pandas as pd

def user_to_dt(s: str, as_type: str = 'datetime'):
    """
    Converts a user string (format YYYY-MM-DD-HH-MM-SS) to a datetime object or ISO string.
    """
    try:
        parts = [int(p) for p in s.split('-')]
        while len(parts) < 6:
            parts.append(0)
        dt = datetime(*parts)
        return dt if as_type == 'datetime' else dt.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        # Fallback for ISO format if user provides that
        try:
            dt = datetime.fromisoformat(s)
            return dt if as_type == 'datetime' else dt.isoformat()
        except ValueError:
            raise ValueError(f"Invalid date format: {s}. Expected YYYY-MM-DD-HH-MM-SS")

def parse_tf(tf: str):
    """Parses a timeframe string like '1min' or 'min1' into (unit, number)."""
    # Try unit+num (e.g. min1)
    match = re.match(r"([a-zA-Z]+)(\d+)", tf.strip().lower())
    if match:
        return match.group(1), match.group(2)
    # Try num+unit (e.g. 1min)
    match = re.match(r"(\d+)([a-zA-Z]+)", tf.strip().lower())
    if match:
        return match.group(2), match.group(1)
    return None, None

def user_to_dukascopy_tf(tf: str):
    unit, num = parse_tf(tf)
    units = {
        "min": "MIN", "m": "MIN",
        "hour": "HOUR", "h": "HOUR",
        "day": "DAY", "d": "DAY",
        "week": "WEEK", "w": "WEEK",
        "month": "MONTH", "mo": "MONTH",
        "year": "YEAR", "y": "YEAR",
        "sec": "SEC", "s": "SEC"
    }
    if unit:
        for k, v in units.items():
            if unit.startswith(k): return f"{num}{v}"
    raise ValueError(f"Unrecognized Dukascopy timeframe: '{tf}'")

def user_to_binance_tf(tf: str):
    unit, num = parse_tf(tf)
    units = {
        "min": "m", "m": "m",
        "hour": "h", "h": "h",
        "day": "d", "d": "d",
        "week": "w", "w": "w",
        "month": "M", "mo": "M",
        "sec": "s", "s": "s"
    }
    if unit:
        for k, v in units.items():
            if unit.startswith(k): return f"{num}{v}"
    raise ValueError(f"Unrecognized Binance timeframe: '{tf}'")

def user_to_alpaca_tf(tf: str):
    unit, num = parse_tf(tf)
    units = {
        "min": "Min", "m": "Min",
        "hour": "Hour", "h": "Hour",
        "day": "Day", "d": "Day",
        "week": "Week", "w": "Week",
    }
    if unit:
        for k, v in units.items():
            if unit.startswith(k): return f"{num}{v}"
    raise ValueError(f"Unrecognized Alpaca timeframe: '{tf}'")
