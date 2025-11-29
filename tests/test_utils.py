import unittest
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from finda.utils import user_to_dt, parse_tf, user_to_dukascopy_tf, user_to_binance_tf, user_to_alpaca_tf

class TestUtils(unittest.TestCase):
    def test_user_to_dt(self):
        # Format: YYYY-MM-DD-HH-MM-SS
        s = "2023-01-01-12-00-00"
        dt = user_to_dt(s, 'datetime')
        self.assertEqual(dt, datetime(2023, 1, 1, 12, 0, 0))

        iso = user_to_dt(s, 'iso')
        self.assertEqual(iso, "2023-01-01T12:00:00")

        # Partial
        s2 = "2023-01-01"
        dt2 = user_to_dt(s2, 'datetime')
        self.assertEqual(dt2, datetime(2023, 1, 1, 0, 0, 0))

    def test_parse_tf(self):
        self.assertEqual(parse_tf("1min"), ("min", "1"))
        self.assertEqual(parse_tf("min1"), ("min", "1"))
        self.assertEqual(parse_tf("1h"), ("h", "1"))
        self.assertEqual(parse_tf("H1"), ("h", "1")) # Case insensitive

    def test_provider_tfs(self):
        self.assertEqual(user_to_dukascopy_tf("1h"), "1HOUR")
        self.assertEqual(user_to_binance_tf("1h"), "1h")
        self.assertEqual(user_to_alpaca_tf("1h"), "1Hour")

        self.assertEqual(user_to_dukascopy_tf("15min"), "15MIN")
        self.assertEqual(user_to_binance_tf("15m"), "15m")
        self.assertEqual(user_to_alpaca_tf("15Min"), "15Min")

if __name__ == "__main__":
    unittest.main()
