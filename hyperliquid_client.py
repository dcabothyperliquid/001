import requests
import json
import time
import logging

logger = logging.getLogger(__name__)

MAINNET_URL = "https://api.hyperliquid.xyz"

class HyperliquidClient:
    def __init__(self):
        self.base_url = MAINNET_URL
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self._spot_meta_cache = None
        self._spot_meta_ts = 0

    def _post(self, payload):
        try:
            r = self.session.post(f"{self.base_url}/info", json=payload, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"API error: {e}")
            return None

    def get_spot_meta(self):
        """Get all spot pairs metadata (cached 60s)"""
        now = time.time()
        if self._spot_meta_cache and now - self._spot_meta_ts < 60:
            return self._spot_meta_cache
        data = self._post({"type": "spotMeta"})
        if data:
            self._spot_meta_cache = data
            self._spot_meta_ts = now
        return data

    def get_spot_pairs(self):
        """Return list of available spot pairs"""
        meta = self.get_spot_meta()
        if not meta:
            return []
        pairs = []
        for i, token in enumerate(meta.get('universe', [])):
            pairs.append({
                'index': i,
                'name': token.get('name', ''),
                'asset_id': 10000 + i
            })
        return pairs

    def get_all_mids(self):
        """Get current mid prices for all assets"""
        return self._post({"type": "allMids"}) or {}

    def get_spot_price(self, symbol):
        """Get current price for a spot symbol"""
        mids = self.get_all_mids()
        # Try common formats
        for key in [f"{symbol}/USDC", f"{symbol}", symbol]:
            if key in mids:
                try:
                    return float(mids[key])
                except:
                    pass
        return None

    def get_candles(self, symbol, interval, lookback=100):
        """Get OHLCV candles"""
        interval_map = {
            '1m': '1m', '3m': '3m', '5m': '5m',
            '15m': '15m', '30m': '30m',
            '1h': '1h', '2h': '2h', '3h': '3h', '4h': '4h',
            '8h': '8h', '12h': '12h', '1d': '1d'
        }
        tf = interval_map.get(interval, '1h')
        now_ms = int(time.time() * 1000)
        
        # Calculate start time based on lookback
        interval_ms = {
            '1m': 60000, '3m': 180000, '5m': 300000,
            '15m': 900000, '30m': 1800000,
            '1h': 3600000, '2h': 7200000, '3h': 10800000,
            '4h': 14400000, '8h': 28800000, '12h': 43200000,
            '1d': 86400000
        }
        ms = interval_ms.get(tf, 3600000)
        start_ms = now_ms - (lookback * ms)

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": tf,
                "startTime": start_ms,
                "endTime": now_ms
            }
        }
        return self._post(payload) or []

    def get_spot_balance(self, address):
        """Get user spot balances"""
        data = self._post({
            "type": "spotClearinghouseState",
            "user": address
        })
        if not data:
            return {}
        balances = {}
        for b in data.get('balances', []):
            balances[b['coin']] = {
                'total': float(b.get('total', 0)),
                'hold': float(b.get('hold', 0)),
                'available': float(b.get('total', 0)) - float(b.get('hold', 0))
            }
        return balances

    def get_order_book(self, symbol):
        """Get order book for a symbol"""
        return self._post({
            "type": "l2Book",
            "coin": symbol
        })
