class FindaError(Exception):
    """Base exception for Finda"""
    pass

class DataProviderError(FindaError):
    """Raised when a data provider fails or returns no data"""
    pass

class InvalidTimeframeError(FindaError):
    """Raised when a timeframe is invalid"""
    pass
