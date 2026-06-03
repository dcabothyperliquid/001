# ─────────────────────────────────────────────────────────────────────────────
# main_merged.py  —  HyperliquidClient + BotEngine + Flask App  (single file)
# ─────────────────────────────────────────────────────────────────────────────

import os
import json
import time
import threading
import logging
from datetime import datetime
from collections import deque

import numpy as np
import requests
from flask import Flask, jsonify, request, render_template
try:
    from flask_cors import CORS
    CORS_AVAILABLE = True
except ImportError:
    CORS_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ── Try importing real SDK ────────────────────────────────────────────────────
try:
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    import eth_account
    SDK_AVAILABLE = True
    logger.info("✅ Hyperliquid SDK loaded — LIVE TRADING ENABLED")
except ImportError:
    SDK_AVAILABLE = False
    logger.warning("⚠️  hyperliquid-python-sdk not installed — running in SIMULATION mode")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

MAINNET_URL     = "https://api.hyperliquid.xyz"
DATA_FILE       = "bot_data.json"
SCAN_TIMEFRAMES = ['1h', '3h', '4h', '1d']

SIGNAL_SCORES = {
    'strong_buy':  +2,
    'buy':         +1,
    'neutral':      0,
    'sell':        -1,
    'strong_sell': -2,
}

# ═════════════════════════════════════════════════════════════════════════════
# HyperliquidClient
# ═════════════════════════════════════════════════════════════════════════════

class HyperliquidClient:
    def __init__(self):
        self.base_url = MAINNET_URL
        self.session  = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self._spot_meta_cache = None
        self._spot_meta_ts    = 0

    def _post(self, payload):
        try:
            r = self.session.post(f"{self.base_url}/info", json=payload, timeout=15)
            if r.status_code != 200:
                logger.error(f"API HTTP {r.status_code} for payload type={payload.get('type')} — body: {r.text[:200]}")
                return None
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
            self._spot_meta_ts    = now
        return data

    def get_spot_pairs(self):
        """Return list of available spot pairs"""
        meta = self.get_spot_meta()
        if not meta:
            return []
        pairs = []
        for i, token in enumerate(meta.get('universe', [])):
            pairs.append({'index': i, 'name': token.get('name', ''), 'asset_id': 10000 + i})
        return pairs

    def get_all_mids(self):
        """Get current mid prices for all assets"""
        return self._post({"type": "allMids"}) or {}

    def get_spot_price(self, symbol):
        """Get current price for a spot symbol — uses @index format for spot"""
        mids = self.get_all_mids()
        if not mids:
            return None
        # Try @index format first (correct spot format)
        try:
            meta = self.get_spot_meta()
            if meta:
                for i, token in enumerate(meta.get('universe', [])):
                    if token.get('name', '').upper() == symbol.upper():
                        spot_key = f"@{10000 + i}"
                        if spot_key in mids:
                            return float(mids[spot_key])
        except Exception as e:
            logger.error(f"Spot price @index lookup error {symbol}: {e}")
        # Fallback: try plain symbol keys
        for key in [f"{symbol}/USDC", symbol]:
            if key in mids:
                try:
                    return float(mids[key])
                except:
                    pass
        return None

    def get_candles(self, symbol, interval, lookback=100):
        """
        Get OHLCV candles for a SPOT symbol.
        Spot requires @index coin format. API response: t,o,h,l,c,v (lowercase).
        Returns list: [timestamp, open, high, low, close, volume]
        """
        interval_map = {
            '1m': '1m', '3m': '3m', '5m': '5m',
            '15m': '15m', '30m': '30m',
            '1h': '1h', '2h': '2h', '3h': '3h', '4h': '4h',
            '8h': '8h', '12h': '12h', '1d': '1d'
        }
        tf     = interval_map.get(interval, '1h')
        now_ms = int(time.time() * 1000)
        interval_ms = {
            '1m': 60000, '3m': 180000, '5m': 300000,
            '15m': 900000, '30m': 1800000,
            '1h': 3600000, '2h': 7200000, '3h': 10800000,
            '4h': 14400000, '8h': 28800000, '12h': 43200000,
            '1d': 86400000
        }
        ms       = interval_ms.get(tf, 3600000)
        start_ms = now_ms - (lookback * ms)

        # Resolve spot @index coin identifier
        coin_id = symbol
        try:
            meta = self.get_spot_meta()
            if meta:
                for i, token in enumerate(meta.get('universe', [])):
                    if token.get('name', '').upper() == symbol.upper():
                        coin_id = f"@{10000 + i}"
                        break
        except Exception as e:
            logger.warning(f"Could not resolve spot index for {symbol}: {e}")

        raw = self._post({
            "type": "candleSnapshot",
            "req": {"coin": coin_id, "interval": tf, "startTime": start_ms, "endTime": now_ms}
        })
        if not raw:
            return []

        candles = []
        for c in raw:
            if isinstance(c, dict):
                # API returns lowercase keys: t, o, h, l, c, v
                candles.append([
                    c.get('t', 0),
                    c.get('o', 0),
                    c.get('h', 0),
                    c.get('l', 0),
                    c.get('c', 0),
                    c.get('v', 0),
                ])
            elif isinstance(c, list) and len(c) >= 6:
                candles.append(c)
        return candles

    def get_spot_balance(self, address=None):
        """
        Get user spot balances.
        address optional — falls back to WALLET_ADDRESS env var.
        """
        if not address:
            address = os.environ.get('WALLET_ADDRESS', '')
        if not address:
            return {'error': 'No wallet address configured'}
        data = self._post({"type": "spotClearinghouseState", "user": address})
        if not data:
            return {}
        balances = {}
        for b in data.get('balances', []):
            balances[b['coin']] = {
                'total':     float(b.get('total', 0)),
                'hold':      float(b.get('hold', 0)),
                'available': float(b.get('total', 0)) - float(b.get('hold', 0))
            }
        return balances

    def get_order_book(self, symbol):
        """Get order book for a symbol"""
        return self._post({"type": "l2Book", "coin": symbol})


# ═════════════════════════════════════════════════════════════════════════════
# BotEngine
# ═════════════════════════════════════════════════════════════════════════════

class BotEngine:
    def __init__(self, client):
        self.client      = client   # HyperliquidClient (market data)
        self.exchange    = None     # SDK exchange (live orders)
        self.info        = None     # SDK info
        self.running     = False
        self.thread      = None
        self.wallet      = ''
        self.private_key = ''
        self.live_mode   = False

        self.coins    = {}
        self.holdings = {}
        self.trades   = []
        self.stats    = {
            'total_trades': 0, 'winning_trades': 0,
            'total_profit': 0.0, 'daily_profit': 0.0, 'start_time': None
        }
        # Events — in-memory, last 50
        self._events = deque(maxlen=50)
        self._load_data()

    # ─── EVENTS ───────────────────────────────────────────────────────────────

    def _push_event(self, event_type: str, message: str, data: dict = None):
        """event_type: 'buy' | 'sell' | 'error' | 'monitor' | 'warn'"""
        self._events.appendleft({
            'type': event_type, 'message': message,
            'data': data or {}, 'time': datetime.now().isoformat()
        })

    def get_events(self):
        """Return last 50 events (newest first)."""
        return list(self._events)

    # ─── PERSISTENCE ──────────────────────────────────────────────────────────

    def _save_data(self):
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump({
                    'coins': self.coins, 'holdings': self.holdings,
                    'trades': self.trades[-200:], 'stats': self.stats
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Save error: {e}")

    def _load_data(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE) as f:
                    d = json.load(f)
                self.coins    = d.get('coins', {})
                self.holdings = d.get('holdings', {})
                self.trades   = d.get('trades', [])
                self.stats    = d.get('stats', self.stats)
            except Exception as e:
                logger.error(f"Load error: {e}")

    # ─── SDK INIT ─────────────────────────────────────────────────────────────

    def _init_sdk(self, wallet: str, private_key: str) -> bool:
        if not SDK_AVAILABLE:
            logger.warning("SDK not available — simulation mode"); return False
        if not private_key or len(private_key) < 10:
            logger.warning("No private key — simulation mode"); return False
        try:
            account    = eth_account.Account.from_key(private_key)
            self.info  = Info("https://api.hyperliquid.xyz", skip_ws=True)
            self.exchange = Exchange(
                account, "https://api.hyperliquid.xyz",
                account_address=wallet if wallet else account.address
            )
            self.wallet    = wallet if wallet else account.address
            self.live_mode = True
            logger.info(f"✅ SDK initialized — wallet: {account.address[:10]}...")
            return True
        except Exception as e:
            logger.error(f"SDK init failed: {e}")
            self.live_mode = False; return False

    # ─── SPOT ASSET INDEX ─────────────────────────────────────────────────────

    def _get_spot_asset_index(self, symbol: str):
        try:
            meta = self.client.get_spot_meta()
            if not meta: return None
            for i, token in enumerate(meta.get('universe', [])):
                if token.get('name', '').upper() == symbol.upper():
                    return i
            return None
        except Exception as e:
            logger.error(f"Asset index error: {e}"); return None

    # ─── LIVE ORDER EXECUTION ─────────────────────────────────────────────────

    def _live_buy(self, symbol: str, usdt_amount: float, price: float):
        try:
            asset_idx = self._get_spot_asset_index(symbol)
            if asset_idx is None:
                logger.error(f"{symbol} not found on spot"); return None
            coin_str = f"@{10000 + asset_idx}"
            size     = round(usdt_amount / price, 6)
            limit_px = round(price * 1.02, 6)
            result   = self.exchange.order(
                coin_str, is_buy=True, sz=size, limit_px=limit_px,
                order_type={"limit": {"tif": "Ioc"}}, reduce_only=False
            )
            logger.info(f"LIVE BUY {symbol}: {result}")
            if result.get('status') == 'ok':
                fills     = result.get('response', {}).get('data', {}).get('statuses', [{}])
                fill      = fills[0] if fills else {}
                filled_px = float(fill.get('filled', {}).get('avgPx', price)) if fill.get('filled') else price
                return {'success': True, 'price': filled_px, 'size': size, 'raw': result}
            logger.error(f"Buy failed: {result}"); return None
        except Exception as e:
            logger.error(f"Live buy error {symbol}: {e}"); return None

    def _live_sell(self, symbol: str, amount: float, price: float):
        try:
            asset_idx = self._get_spot_asset_index(symbol)
            if asset_idx is None:
                logger.error(f"{symbol} not found on spot"); return None
            coin_str = f"@{10000 + asset_idx}"
            limit_px = round(price * 0.98, 6)
            result   = self.exchange.order(
                coin_str, is_buy=False, sz=round(amount, 6), limit_px=limit_px,
                order_type={"limit": {"tif": "Ioc"}}, reduce_only=False
            )
            logger.info(f"LIVE SELL {symbol}: {result}")
            if result.get('status') == 'ok':
                fills     = result.get('response', {}).get('data', {}).get('statuses', [{}])
                fill      = fills[0] if fills else {}
                filled_px = float(fill.get('filled', {}).get('avgPx', price)) if fill.get('filled') else price
                return {'success': True, 'price': filled_px, 'raw': result}
            logger.error(f"Sell failed: {result}"); return None
        except Exception as e:
            logger.error(f"Live sell error {symbol}: {e}"); return None

    # ─── COIN MANAGEMENT ──────────────────────────────────────────────────────

    def add_coin(self, symbol, capital, timeframe='auto', stop_loss=1.5, trailing_stop=1.0):
        """timeframe='auto' — bot scans all TFs automatically."""
        self.coins[symbol] = {
            'symbol': symbol, 'capital': capital, 'timeframe': timeframe,
            'stop_loss': stop_loss, 'trailing_stop': trailing_stop,
            'enabled': True, 'added_at': datetime.now().isoformat()
        }
        self._save_data()
        return {'success': True, 'coin': self.coins[symbol]}

    def remove_coin(self, symbol):
        if symbol in self.coins:
            del self.coins[symbol]; self._save_data(); return {'success': True}
        return {'success': False, 'error': 'Coin not found'}

    def update_coin(self, symbol, data):
        if symbol not in self.coins:
            return {'success': False, 'error': 'Coin not found'}
        for k in ['capital', 'timeframe', 'stop_loss', 'trailing_stop', 'enabled']:
            if k in data:
                self.coins[symbol][k] = data[k]
        self._save_data()
        return {'success': True, 'coin': self.coins[symbol]}

    def get_coins(self):
        result = []
        for sym, cfg in self.coins.items():
            try:
                market   = self.get_market_data(sym)
                holding  = self.holdings.get(sym)
                pnl = avg_entry = total_held = 0.0
                if holding and holding.get('entries'):
                    entries    = holding['entries']
                    total_usdt = sum(e['usdt'] for e in entries)
                    total_amt  = sum(e['amount'] for e in entries)
                    avg_entry  = total_usdt / total_amt if total_amt > 0 else 0
                    total_held = total_amt
                    cur_price  = market.get('price', 0)
                    if cur_price and avg_entry:
                        pnl = ((cur_price - avg_entry) / avg_entry) * 100
                result.append({
                    **cfg,
                    'price': market.get('price', 0),
                    'rsi': market.get('rsi', 0),
                    'macd_signal': market.get('macd_signal', 'neutral'),
                    'volume_signal': market.get('volume_signal', False),
                    'signal': market.get('signal', 'neutral'),
                    'mtf_score': market.get('mtf_score', 0),
                    'best_timeframe': market.get('best_timeframe', 'N/A'),
                    'confidence': market.get('confidence', 'low'),
                    'holding': total_held, 'avg_entry': avg_entry,
                    'pnl_pct': round(pnl, 2),
                    'peak_price': holding.get('peak_price', 0) if holding else 0
                })
            except Exception as e:
                logger.error(f"get_coins error for {sym}: {e}")
                # Return coin with safe defaults so UI doesn't crash
                result.append({
                    **cfg,
                    'price': 0, 'rsi': 50, 'macd_signal': 'neutral',
                    'volume_signal': False, 'signal': 'neutral',
                    'mtf_score': 0, 'best_timeframe': 'N/A',
                    'confidence': 'low', 'holding': 0,
                    'avg_entry': 0, 'pnl_pct': 0, 'peak_price': 0
                })
        return result

    def get_holdings(self):
        result = []
        for sym, h in self.holdings.items():
            if not h.get('entries'): continue
            entries    = h['entries']
            total_usdt = sum(e['usdt'] for e in entries)
            total_amt  = sum(e['amount'] for e in entries)
            avg_entry  = total_usdt / total_amt if total_amt > 0 else 0
            cur_price  = self.client.get_spot_price(sym) or 0
            pnl        = ((cur_price - avg_entry) / avg_entry * 100) if avg_entry else 0
            result.append({
                'symbol': sym, 'amount': total_amt, 'avg_entry': avg_entry,
                'current_price': cur_price, 'pnl_pct': round(pnl, 2),
                'pnl_usdt': round((cur_price - avg_entry) * total_amt, 4),
                'invested_usdt': total_usdt, 'current_value': cur_price * total_amt,
                'dca_count': len(entries),
                'peak_price': h.get('peak_price', 0),
                'trailing_stop_price': h.get('trailing_stop_price', 0)
            })
        return result

    def get_trade_history(self):
        return list(reversed(self.trades[-100:]))

    def get_stats(self):
        win_rate = 0
        if self.stats['total_trades'] > 0:
            win_rate = round(self.stats['winning_trades'] / self.stats['total_trades'] * 100, 1)
        return {**self.stats, 'win_rate': win_rate}

    def get_status(self):
        wallet         = self.wallet or os.environ.get('WALLET_ADDRESS', '')
        key_configured = bool(self.private_key or os.environ.get('PRIVATE_KEY', ''))
        return {
            'running': self.running, 'live_mode': self.live_mode,
            'sdk_available': SDK_AVAILABLE,
            'coins_monitored': len(self.coins),
            'active_holdings': len([h for h in self.holdings.values() if h.get('entries')]),
            'wallet_masked': (wallet[:6] + '...' + wallet[-4:]) if len(wallet) > 10 else wallet,
            'key_configured': key_configured
        }

    # ─── INDICATORS ───────────────────────────────────────────────────────────

    def _calc_rsi(self, closes, period=14):
        if len(closes) < period + 1: return 50.0
        deltas   = np.diff(closes)
        gains    = np.where(deltas > 0, deltas, 0)
        losses   = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0: return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

    def _calc_macd(self, closes, fast=12, slow=26, signal=9):
        if len(closes) < slow + signal: return 0, 0, 'neutral'
        def ema(data, n):
            k = 2 / (n + 1); r = [data[0]]
            for p in data[1:]: r.append(p * k + r[-1] * (1 - k))
            return r
        ema_f    = ema(closes, fast)
        ema_s    = ema(closes, slow)
        macd_ln  = [f - s for f, s in zip(ema_f, ema_s)]
        sig_ln   = ema(macd_ln, signal)
        hist     = [m - s for m, s in zip(macd_ln, sig_ln)]
        if len(hist) >= 2:
            if hist[-1] > 0 and hist[-1] > hist[-2]: return macd_ln[-1], sig_ln[-1], 'bullish'
            if hist[-1] < 0 and hist[-1] < hist[-2]: return macd_ln[-1], sig_ln[-1], 'bearish'
        return macd_ln[-1], sig_ln[-1], 'neutral'

    def _calc_volume_signal(self, volumes, multiplier=1.5):
        if len(volumes) < 20: return False
        return volumes[-1] > np.mean(volumes[-20:-1]) * multiplier

    def _signal_for_candles(self, candles):
        """candles index: 4=close, 5=volume → returns (signal, rsi, macd_sig, vol_sig)"""
        if not candles or len(candles) < 30: return 'neutral', 50.0, 'neutral', False
        closes  = [float(c[4]) for c in candles]
        volumes = [float(c[5]) for c in candles]
        rsi = self._calc_rsi(closes)
        _, _, macd_sig = self._calc_macd(closes)
        vol_sig = self._calc_volume_signal(volumes)
        if   rsi < 30 and macd_sig == 'bullish':               signal = 'strong_buy'
        elif rsi < 35 and (macd_sig == 'bullish' or vol_sig):  signal = 'buy'
        elif rsi > 70 and macd_sig == 'bearish':               signal = 'strong_sell'
        elif rsi > 65 and macd_sig == 'bearish':               signal = 'sell'
        else:                                                   signal = 'neutral'
        return signal, rsi, macd_sig, vol_sig

    # ─── MULTI-TIMEFRAME SCAN ─────────────────────────────────────────────────

    def _mtf_scan(self, symbol: str) -> dict:
        """
        Scan 1h + 3h + 4h + 1d.
        Score system:  strong_buy=+2  buy=+1  neutral=0  sell=-1  strong_sell=-2
        Total >= 3  → BUY high    | >= 2 → BUY medium
        Total <= -3 → SELL high   | <= -2 → SELL medium
        Position sizing:  score 4 → 100%  |  3 → 75%  |  2 → 50%
        """
        tf_results  = {}
        total_score = 0
        best_tf     = '1h'
        best_score  = 0

        for tf in SCAN_TIMEFRAMES:
            candles             = self.client.get_candles(symbol, tf, lookback=60)
            signal, rsi, macd, vol = self._signal_for_candles(candles)
            score               = SIGNAL_SCORES.get(signal, 0)
            tf_results[tf]      = {'signal': signal, 'score': score, 'rsi': rsi, 'macd': macd, 'vol': vol}
            total_score        += score
            if abs(score) > abs(best_score):
                best_score = score; best_tf = tf

        if   total_score >= 3:  direction, confidence = 'buy',  'high'
        elif total_score >= 2:  direction, confidence = 'buy',  'medium'
        elif total_score <= -3: direction, confidence = 'sell', 'high'
        elif total_score <= -2: direction, confidence = 'sell', 'medium'
        else:                   direction, confidence = 'neutral', 'low'

        abs_s = abs(total_score)
        capital_pct = 1.0 if abs_s >= 4 else 0.75 if abs_s == 3 else 0.50 if abs_s == 2 else 0.0

        best = tf_results[best_tf]
        return {
            'total_score': total_score, 'confidence': confidence,
            'direction': direction, 'best_timeframe': best_tf,
            'capital_pct': capital_pct,
            'tf_breakdown': {tf: v['signal'] for tf, v in tf_results.items()},
            'rsi': best['rsi'], 'macd_signal': best['macd'], 'volume_signal': best['vol'],
            'signal': direction if direction != 'neutral' else 'neutral'
        }

    # ─── MARKET DATA ──────────────────────────────────────────────────────────

    def get_market_data(self, symbol):
        try:
            mtf   = self._mtf_scan(symbol)
            price = self.client.get_spot_price(symbol) or 0
            if not price:
                candles = self.client.get_candles(symbol, '1h', lookback=5)
                if candles: price = float(candles[-1][4])
            return {
                'price': price,
                'rsi': mtf['rsi'], 'macd_signal': mtf['macd_signal'],
                'volume_signal': mtf['volume_signal'], 'signal': mtf['signal'],
                'mtf_score': mtf['total_score'], 'best_timeframe': mtf['best_timeframe'],
                'confidence': mtf['confidence'], 'capital_pct': mtf['capital_pct'],
                'tf_breakdown': mtf['tf_breakdown'],
            }
        except Exception as e:
            logger.error(f"Market data error {symbol}: {e}")
            return {
                'price': 0, 'rsi': 50, 'macd_signal': 'neutral',
                'volume_signal': False, 'signal': 'neutral',
                'mtf_score': 0, 'best_timeframe': 'N/A',
                'confidence': 'low', 'capital_pct': 0.0, 'tf_breakdown': {}
            }

    # ─── TRADING ──────────────────────────────────────────────────────────────

    def _execute_buy(self, symbol, capital, price, reason):
        # Balance check
        if self.live_mode:
            usdc_bal = self.client.get_spot_balance().get('USDC', {}).get('available', 0)
            if usdc_bal < capital:
                msg = f"Low balance: {usdc_bal:.2f} available, need {capital:.2f} USDC"
                logger.warning(f"[{symbol}] {msg}")
                self._push_event('warn', msg, {'symbol': symbol, 'available': usdc_bal, 'required': capital})
                if usdc_bal < 1:
                    self._push_event('error', f"Insufficient balance for {symbol} — skip", {'symbol': symbol})
                    return None
                capital = usdc_bal * 0.99

        actual_price = price; order_id = None; mode_tag = 'SIM'

        if self.live_mode:
            result = self._live_buy(symbol, capital, price)
            if result:
                actual_price = result['price']
                order_id     = str(result.get('raw', {}).get('response', {}).get('data', {}).get('statuses', [{}])[0].get('resting', {}).get('oid', ''))
                mode_tag     = 'LIVE'
                logger.info(f"✅ LIVE BUY {symbol} @ {actual_price}")
            else:
                logger.error(f"Live buy failed {symbol}")
                self._push_event('error', f"Live buy failed {symbol}", {'symbol': symbol, 'price': price})
                return None
        else:
            logger.info(f"[SIM] BUY {symbol} @ {actual_price}")

        amount = capital / actual_price
        trade  = {
            'type': 'BUY', 'symbol': symbol, 'price': actual_price,
            'amount': amount, 'usdt': capital, 'reason': reason,
            'mode': mode_tag, 'order_id': order_id,
            'time': datetime.now().isoformat(), 'pnl': None
        }
        if symbol not in self.holdings:
            self.holdings[symbol] = {'entries': [], 'peak_price': actual_price, 'trailing_stop_price': 0}
        self.holdings[symbol]['entries'].append(
            {'price': actual_price, 'amount': amount, 'usdt': capital, 'time': trade['time']}
        )
        self.holdings[symbol]['peak_price'] = max(self.holdings[symbol].get('peak_price', actual_price), actual_price)
        trail_pct = self.coins.get(symbol, {}).get('trailing_stop', 1.0) / 100
        self.holdings[symbol]['trailing_stop_price'] = actual_price * (1 - trail_pct)

        self.trades.append(trade)
        self._push_event('buy', f"[{mode_tag}] BUY {symbol} @ {actual_price:.6f} — {reason}",
                         {'symbol': symbol, 'price': actual_price, 'usdt': capital, 'reason': reason})
        logger.info(f"[{mode_tag}] BUY {symbol} @ {actual_price:.6f} | {capital:.2f} USDC | {reason}")
        self._save_data()
        return trade

    def _execute_sell(self, symbol, price, reason):
        if symbol not in self.holdings or not self.holdings[symbol].get('entries'):
            return None
        entries    = self.holdings[symbol]['entries']
        total_usdt = sum(e['usdt'] for e in entries)
        total_amt  = sum(e['amount'] for e in entries)
        avg_entry  = total_usdt / total_amt if total_amt > 0 else price
        actual_price = price; order_id = None; mode_tag = 'SIM'

        if self.live_mode:
            result = self._live_sell(symbol, total_amt, price)
            if result:
                actual_price = result['price']
                order_id     = str(result.get('raw', {}).get('response', {}).get('data', {}).get('statuses', [{}])[0].get('resting', {}).get('oid', ''))
                mode_tag     = 'LIVE'
                logger.info(f"✅ LIVE SELL {symbol} @ {actual_price}")
            else:
                logger.error(f"Live sell failed {symbol}")
                self._push_event('error', f"Live sell failed {symbol}", {'symbol': symbol, 'price': price})
                return None
        else:
            logger.info(f"[SIM] SELL {symbol} @ {actual_price}")

        pnl_usdt = (actual_price - avg_entry) * total_amt
        pnl_pct  = (actual_price - avg_entry) / avg_entry * 100
        trade    = {
            'type': 'SELL', 'symbol': symbol, 'price': actual_price,
            'amount': total_amt, 'usdt': actual_price * total_amt,
            'avg_entry': avg_entry, 'pnl_usdt': round(pnl_usdt, 4),
            'pnl_pct': round(pnl_pct, 2), 'reason': reason,
            'mode': mode_tag, 'order_id': order_id,
            'dca_count': len(entries), 'time': datetime.now().isoformat()
        }
        self.stats['total_trades'] += 1
        self.stats['total_profit']  += pnl_usdt
        if pnl_usdt > 0: self.stats['winning_trades'] += 1

        self.holdings[symbol] = {'entries': [], 'peak_price': 0, 'trailing_stop_price': 0}
        self.trades.append(trade)
        self._push_event('sell',
            f"[{mode_tag}] SELL {symbol} @ {actual_price:.6f} | PnL: {pnl_pct:.2f}% ({pnl_usdt:.4f} USDC)",
            {'symbol': symbol, 'price': actual_price, 'pnl_usdt': round(pnl_usdt, 4),
             'pnl_pct': round(pnl_pct, 2), 'reason': reason})
        logger.info(f"[{mode_tag}] SELL {symbol} @ {actual_price:.6f} | PnL: {pnl_pct:.2f}% ({pnl_usdt:.4f} USDC) | {reason}")
        self._save_data()
        return trade

    def manual_sell(self, symbol, amount=None):
        price = self.client.get_spot_price(symbol) or 0
        if not price: return {'success': False, 'error': 'Could not fetch price'}
        trade = self._execute_sell(symbol, price, 'manual')
        return {'success': True, 'trade': trade} if trade else {'success': False, 'error': 'No holdings or live sell failed'}

    def _check_trailing_stop(self, symbol, current_price):
        h = self.holdings.get(symbol)
        if not h or not h.get('entries'): return False
        trail_pct = self.coins.get(symbol, {}).get('trailing_stop', 1.0) / 100
        if current_price > h.get('peak_price', 0):
            h['peak_price']          = current_price
            h['trailing_stop_price'] = current_price * (1 - trail_pct)
        return current_price <= h.get('trailing_stop_price', 0) and h['trailing_stop_price'] > 0

    def _check_stop_loss(self, symbol, current_price):
        h = self.holdings.get(symbol)
        if not h or not h.get('entries'): return False
        sl_pct     = self.coins.get(symbol, {}).get('stop_loss', 1.5) / 100
        entries    = h['entries']
        total_usdt = sum(e['usdt'] for e in entries)
        total_amt  = sum(e['amount'] for e in entries)
        avg_entry  = total_usdt / total_amt if total_amt > 0 else current_price
        return current_price <= avg_entry * (1 - sl_pct)

    # ─── BOT LOOP ─────────────────────────────────────────────────────────────

    def _bot_loop(self):
        logger.info(f"Bot loop started — {'LIVE' if self.live_mode else 'SIMULATION'}")
        while self.running:
            try:
                for symbol, cfg in list(self.coins.items()):
                    if not cfg.get('enabled', True): continue
                    self._process_coin(symbol, cfg)
                time.sleep(30)
            except Exception as e:
                logger.error(f"Bot loop error: {e}")
                self._push_event('error', f"Bot loop error: {e}", {})
                time.sleep(5)

    def _process_coin(self, symbol, cfg):
        """Multi-timeframe auto scan — position size auto from MTF score."""
        try:
            market      = self.get_market_data(symbol)
            price       = market.get('price', 0)
            if not price: return

            mtf_score   = market.get('mtf_score', 0)
            direction   = market.get('signal', 'neutral')
            confidence  = market.get('confidence', 'low')
            capital_pct = market.get('capital_pct', 0.0)
            best_tf     = market.get('best_timeframe', '?')

            trade_capital = round(cfg.get('capital', 10) * capital_pct, 4)
            has_holding   = bool(self.holdings.get(symbol, {}).get('entries'))

            self._push_event('monitor',
                f"{symbol} | score={mtf_score} | {direction} | conf={confidence} | best_tf={best_tf} | price={price:.6f}",
                {'symbol': symbol, 'score': mtf_score, 'direction': direction, 'confidence': confidence, 'price': price})

            if has_holding:
                if self._check_trailing_stop(symbol, price):
                    entries    = self.holdings[symbol]['entries']
                    avg_entry  = sum(e['usdt'] for e in entries) / sum(e['amount'] for e in entries)
                    if price > avg_entry:
                        self._execute_sell(symbol, price, 'trailing_stop'); return

                if self._check_stop_loss(symbol, price):
                    self._execute_sell(symbol, price, 'stop_loss'); return

                if direction == 'sell' and mtf_score <= -2:
                    entries   = self.holdings[symbol]['entries']
                    avg_entry = sum(e['usdt'] for e in entries) / sum(e['amount'] for e in entries)
                    if price > avg_entry * 1.005:
                        self._execute_sell(symbol, price, f'mtf_sell_score_{mtf_score}'); return

                # DCA
                if direction == 'buy' and mtf_score >= 2:
                    entries   = self.holdings[symbol]['entries']
                    avg_entry = sum(e['usdt'] for e in entries) / sum(e['amount'] for e in entries)
                    if price < avg_entry * 0.97 and len(entries) < 5 and trade_capital > 0:
                        self._execute_buy(symbol, trade_capital, price, f'dca_mtf_score_{mtf_score}_{best_tf}')
            else:
                if direction == 'buy' and trade_capital > 0:
                    self._execute_buy(symbol, trade_capital, price, f'mtf_score_{mtf_score}_{confidence}_{best_tf}')

        except Exception as e:
            logger.error(f"Process coin error {symbol}: {e}")
            self._push_event('error', f"Process error {symbol}: {e}", {'symbol': symbol})

    # ─── START / STOP ─────────────────────────────────────────────────────────

    def start(self, wallet=None, private_key=None):
        if self.running:
            return {'success': False, 'error': 'Bot already running'}
        wallet      = wallet      or os.environ.get('WALLET_ADDRESS', '')
        private_key = private_key or os.environ.get('PRIVATE_KEY', '')
        self.wallet = wallet; self.private_key = private_key
        live = self._init_sdk(wallet, private_key)
        mode = 'LIVE' if live else 'SIMULATION'
        self.running = True
        self.stats['start_time'] = datetime.now().isoformat()
        self.thread = threading.Thread(target=self._bot_loop, daemon=True)
        self.thread.start()
        self._push_event('monitor', f"Bot started in {mode} mode", {'mode': mode})
        return {'success': True, 'message': f'Bot started in {mode} mode', 'mode': mode}

    def stop(self):
        self.running = False; self.live_mode = False; self.exchange = None
        self._push_event('monitor', "Bot stopped", {})
        return {'success': True, 'message': 'Bot stopped'}


# ═════════════════════════════════════════════════════════════════════════════
# Flask App
# ═════════════════════════════════════════════════════════════════════════════

app        = Flask(__name__)
if CORS_AVAILABLE:
    CORS(app)
client     = HyperliquidClient()
bot_engine = BotEngine(client)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/coins', methods=['GET'])
def get_coins():
    return jsonify(bot_engine.get_coins())

@app.route('/api/coins', methods=['POST'])
def add_coin():
    data          = request.json or {}
    symbol        = data.get('symbol', '').upper().strip()
    capital       = float(data.get('capital', 10))
    timeframe     = data.get('timeframe', 'auto')   # 'auto' default — MTF scan handles it
    stop_loss     = float(data.get('stop_loss', 1.5))
    trailing_stop = float(data.get('trailing_stop', 1.0))
    return jsonify(bot_engine.add_coin(symbol, capital, timeframe, stop_loss, trailing_stop))

@app.route('/api/coins/<symbol>', methods=['DELETE'])
def remove_coin(symbol):
    return jsonify(bot_engine.remove_coin(symbol.upper()))

@app.route('/api/coins/<symbol>', methods=['PUT'])
def update_coin(symbol):
    return jsonify(bot_engine.update_coin(symbol.upper(), request.json))

@app.route('/api/market/<symbol>', methods=['GET'])
def get_market_data(symbol):
    return jsonify(bot_engine.get_market_data(symbol.upper()))

@app.route('/api/balance', methods=['GET'])
def get_balance():
    """wallet param optional — falls back to WALLET_ADDRESS env var."""
    wallet = request.args.get('wallet', '') or None
    return jsonify(client.get_spot_balance(wallet))

@app.route('/api/holdings', methods=['GET'])
def get_holdings():
    return jsonify(bot_engine.get_holdings())

@app.route('/api/trades', methods=['GET'])
def get_trades():
    return jsonify(bot_engine.get_trade_history())

@app.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify(bot_engine.get_stats())

@app.route('/api/bot/start', methods=['POST'])
def start_bot():
    """wallet + private_key optional — bot reads WALLET_ADDRESS / PRIVATE_KEY from ENV."""
    data        = request.json or {}
    wallet      = data.get('wallet', '') or None
    private_key = data.get('private_key', '') or None
    return jsonify(bot_engine.start(wallet, private_key))

@app.route('/api/bot/stop', methods=['POST'])
def stop_bot():
    return jsonify(bot_engine.stop())

@app.route('/api/bot/status', methods=['GET'])
def bot_status():
    return jsonify(bot_engine.get_status())

@app.route('/api/events', methods=['GET'])
def get_events():
    """
    Returns last 50 bot events.
    Optional filter: ?type=buy | sell | error | monitor | warn
    """
    events      = bot_engine.get_events()
    filter_type = request.args.get('type', '').lower()
    if filter_type:
        events = [e for e in events if e.get('type') == filter_type]
    return jsonify(events)

@app.route('/api/sell/<symbol>', methods=['POST'])
def manual_sell(symbol):
    data   = request.json or {}
    amount = data.get('amount', None)
    return jsonify(bot_engine.manual_sell(symbol.upper(), amount))

@app.route('/api/spot/pairs', methods=['GET'])
def get_spot_pairs():
    return jsonify(client.get_spot_pairs())

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
