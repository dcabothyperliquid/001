import time
import threading
import logging
import json
import os
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

DATA_FILE = "bot_data.json"

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


class BotEngine:
    def __init__(self, client):
        self.client = client          # HyperliquidClient (for market data)
        self.exchange = None          # Real SDK exchange (for orders)
        self.info = None              # Real SDK info
        self.running = False
        self.thread = None
        self.wallet = ''
        self.private_key = ''
        self.live_mode = False        # True when SDK + key available

        # Coin configs
        self.coins = {}
        # Holdings
        self.holdings = {}
        # Trade history
        self.trades = []
        # Stats
        self.stats = {
            'total_trades': 0,
            'winning_trades': 0,
            'total_profit': 0.0,
            'daily_profit': 0.0,
            'start_time': None
        }
        self._load_data()

    # ─── PERSISTENCE ──────────────────────────────────────────────────────────

    def _save_data(self):
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump({
                    'coins': self.coins,
                    'holdings': self.holdings,
                    'trades': self.trades[-200:],
                    'stats': self.stats
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Save error: {e}")

    def _load_data(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE) as f:
                    d = json.load(f)
                self.coins = d.get('coins', {})
                self.holdings = d.get('holdings', {})
                self.trades = d.get('trades', [])
                self.stats = d.get('stats', self.stats)
            except Exception as e:
                logger.error(f"Load error: {e}")

    # ─── SDK INIT ─────────────────────────────────────────────────────────────

    def _init_sdk(self, wallet: str, private_key: str) -> bool:
        """Initialize Hyperliquid SDK with real credentials."""
        if not SDK_AVAILABLE:
            logger.warning("SDK not available — simulation mode")
            return False
        if not private_key or len(private_key) < 10:
            logger.warning("No private key provided — simulation mode")
            return False
        try:
            account = eth_account.Account.from_key(private_key)
            self.info = Info("https://api.hyperliquid.xyz", skip_ws=True)
            self.exchange = Exchange(
                account,
                "https://api.hyperliquid.xyz",
                account_address=wallet if wallet else account.address
            )
            self.live_mode = True
            logger.info(f"✅ SDK initialized — wallet: {account.address[:10]}...")
            return True
        except Exception as e:
            logger.error(f"SDK init failed: {e}")
            self.live_mode = False
            return False

    # ─── SPOT ASSET INDEX ─────────────────────────────────────────────────────

    def _get_spot_asset_index(self, symbol: str) -> int | None:
        """Get Hyperliquid spot asset index for a symbol (e.g. SOL → index)."""
        try:
            meta = self.client.get_spot_meta()
            if not meta:
                return None
            for i, token in enumerate(meta.get('universe', [])):
                if token.get('name', '').upper() == symbol.upper():
                    return i
            return None
        except Exception as e:
            logger.error(f"Asset index lookup error: {e}")
            return None

    # ─── LIVE ORDER EXECUTION ─────────────────────────────────────────────────

    def _live_buy(self, symbol: str, usdt_amount: float, price: float) -> dict | None:
        """Place real spot market buy via SDK."""
        try:
            asset_idx = self._get_spot_asset_index(symbol)
            if asset_idx is None:
                logger.error(f"Symbol {symbol} not found on Hyperliquid spot")
                return None

            coin_str = f"@{10000 + asset_idx}"   # Hyperliquid spot format
            size = round(usdt_amount / price, 6)  # coin amount

            # Market buy = aggressive limit at 2% above current price
            limit_px = round(price * 1.02, 6)

            result = self.exchange.order(
                coin_str,
                is_buy=True,
                sz=size,
                limit_px=limit_px,
                order_type={"limit": {"tif": "Ioc"}},  # Immediate-or-cancel = market
                reduce_only=False
            )
            logger.info(f"LIVE BUY {symbol}: {result}")
            if result.get('status') == 'ok':
                # Extract actual fill price if available
                fills = result.get('response', {}).get('data', {}).get('statuses', [{}])
                fill = fills[0] if fills else {}
                filled_px = float(fill.get('filled', {}).get('avgPx', price)) if fill.get('filled') else price
                return {'success': True, 'price': filled_px, 'size': size, 'raw': result}
            else:
                logger.error(f"Buy order failed: {result}")
                return None
        except Exception as e:
            logger.error(f"Live buy error {symbol}: {e}")
            return None

    def _live_sell(self, symbol: str, amount: float, price: float) -> dict | None:
        """Place real spot market sell via SDK."""
        try:
            asset_idx = self._get_spot_asset_index(symbol)
            if asset_idx is None:
                logger.error(f"Symbol {symbol} not found on Hyperliquid spot")
                return None

            coin_str = f"@{10000 + asset_idx}"

            # Market sell = aggressive limit at 2% below current price
            limit_px = round(price * 0.98, 6)

            result = self.exchange.order(
                coin_str,
                is_buy=False,
                sz=round(amount, 6),
                limit_px=limit_px,
                order_type={"limit": {"tif": "Ioc"}},
                reduce_only=False
            )
            logger.info(f"LIVE SELL {symbol}: {result}")
            if result.get('status') == 'ok':
                fills = result.get('response', {}).get('data', {}).get('statuses', [{}])
                fill = fills[0] if fills else {}
                filled_px = float(fill.get('filled', {}).get('avgPx', price)) if fill.get('filled') else price
                return {'success': True, 'price': filled_px, 'raw': result}
            else:
                logger.error(f"Sell order failed: {result}")
                return None
        except Exception as e:
            logger.error(f"Live sell error {symbol}: {e}")
            return None

    # ─── COIN MANAGEMENT ──────────────────────────────────────────────────────

    def add_coin(self, symbol, capital, timeframe, stop_loss, trailing_stop):
        self.coins[symbol] = {
            'symbol': symbol,
            'capital': capital,
            'timeframe': timeframe,
            'stop_loss': stop_loss,
            'trailing_stop': trailing_stop,
            'enabled': True,
            'added_at': datetime.now().isoformat()
        }
        self._save_data()
        return {'success': True, 'coin': self.coins[symbol]}

    def remove_coin(self, symbol):
        if symbol in self.coins:
            del self.coins[symbol]
            self._save_data()
            return {'success': True}
        return {'success': False, 'error': 'Coin not found'}

    def update_coin(self, symbol, data):
        if symbol not in self.coins:
            return {'success': False, 'error': 'Coin not found'}
        allowed = ['capital', 'timeframe', 'stop_loss', 'trailing_stop', 'enabled']
        for k in allowed:
            if k in data:
                self.coins[symbol][k] = data[k]
        self._save_data()
        return {'success': True, 'coin': self.coins[symbol]}

    def get_coins(self):
        result = []
        for sym, cfg in self.coins.items():
            market = self.get_market_data(sym)
            holding = self.holdings.get(sym)
            pnl = 0.0
            avg_entry = 0.0
            total_held = 0.0
            if holding and holding.get('entries'):
                entries = holding['entries']
                total_usdt = sum(e['usdt'] for e in entries)
                total_amt = sum(e['amount'] for e in entries)
                avg_entry = total_usdt / total_amt if total_amt > 0 else 0
                total_held = total_amt
                cur_price = market.get('price', 0)
                if cur_price and avg_entry:
                    pnl = ((cur_price - avg_entry) / avg_entry) * 100
            result.append({
                **cfg,
                'price': market.get('price', 0),
                'rsi': market.get('rsi', 0),
                'macd_signal': market.get('macd_signal', 'neutral'),
                'volume_signal': market.get('volume_signal', False),
                'signal': market.get('signal', 'neutral'),
                'holding': total_held,
                'avg_entry': avg_entry,
                'pnl_pct': round(pnl, 2),
                'peak_price': holding.get('peak_price', 0) if holding else 0
            })
        return result

    def get_holdings(self):
        result = []
        for sym, h in self.holdings.items():
            if not h.get('entries'):
                continue
            entries = h['entries']
            total_usdt = sum(e['usdt'] for e in entries)
            total_amt = sum(e['amount'] for e in entries)
            avg_entry = total_usdt / total_amt if total_amt > 0 else 0
            cur_price = self.client.get_spot_price(sym) or 0
            pnl = ((cur_price - avg_entry) / avg_entry * 100) if avg_entry else 0
            result.append({
                'symbol': sym,
                'amount': total_amt,
                'avg_entry': avg_entry,
                'current_price': cur_price,
                'pnl_pct': round(pnl, 2),
                'pnl_usdt': round((cur_price - avg_entry) * total_amt, 4),
                'invested_usdt': total_usdt,
                'current_value': cur_price * total_amt,
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
        return {
            'running': self.running,
            'live_mode': self.live_mode,
            'sdk_available': SDK_AVAILABLE,
            'coins_monitored': len(self.coins),
            'active_holdings': len([h for h in self.holdings.values() if h.get('entries')]),
            'wallet': self.wallet[:8] + '...' if self.wallet else ''
        }

    # ─── INDICATORS ───────────────────────────────────────────────────────────

    def _calc_rsi(self, closes, period=14):
        if len(closes) < period + 1:
            return 50.0
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

    def _calc_macd(self, closes, fast=12, slow=26, signal=9):
        if len(closes) < slow + signal:
            return 0, 0, 'neutral'
        def ema(data, n):
            k = 2 / (n + 1)
            result = [data[0]]
            for p in data[1:]:
                result.append(p * k + result[-1] * (1 - k))
            return result
        ema_fast = ema(closes, fast)
        ema_slow = ema(closes, slow)
        macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
        signal_line = ema(macd_line, signal)
        hist = [m - s for m, s in zip(macd_line, signal_line)]
        if len(hist) >= 2:
            if hist[-1] > 0 and hist[-1] > hist[-2]:
                return macd_line[-1], signal_line[-1], 'bullish'
            elif hist[-1] < 0 and hist[-1] < hist[-2]:
                return macd_line[-1], signal_line[-1], 'bearish'
        return macd_line[-1], signal_line[-1], 'neutral'

    def _calc_volume_signal(self, volumes, multiplier=1.5):
        if len(volumes) < 20:
            return False
        avg_vol = np.mean(volumes[-20:-1])
        return volumes[-1] > avg_vol * multiplier

    def get_market_data(self, symbol):
        try:
            tf = self.coins.get(symbol, {}).get('timeframe', '1h')
            candles = self.client.get_candles(symbol, tf, lookback=60)
            if not candles or len(candles) < 30:
                price = self.client.get_spot_price(symbol) or 0
                return {'price': price, 'rsi': 50, 'macd_signal': 'neutral',
                        'volume_signal': False, 'signal': 'neutral'}

            closes = [float(c[4]) for c in candles]
            volumes = [float(c[5]) for c in candles]
            price = closes[-1]

            rsi = self._calc_rsi(closes)
            _, _, macd_sig = self._calc_macd(closes)
            vol_sig = self._calc_volume_signal(volumes)

            signal = 'neutral'
            if rsi < 30 and macd_sig == 'bullish':
                signal = 'strong_buy'
            elif rsi < 35 and (macd_sig == 'bullish' or vol_sig):
                signal = 'buy'
            elif rsi > 70 and macd_sig == 'bearish':
                signal = 'strong_sell'
            elif rsi > 65 and macd_sig == 'bearish':
                signal = 'sell'

            return {
                'price': price,
                'rsi': rsi,
                'macd_signal': macd_sig,
                'volume_signal': vol_sig,
                'signal': signal,
                'candles_count': len(candles)
            }
        except Exception as e:
            logger.error(f"Market data error {symbol}: {e}")
            return {'price': 0, 'rsi': 50, 'macd_signal': 'neutral',
                    'volume_signal': False, 'signal': 'neutral'}

    # ─── TRADING ──────────────────────────────────────────────────────────────

    def _execute_buy(self, symbol, capital, price, reason):
        actual_price = price
        order_id = None
        mode_tag = 'SIM'

        if self.live_mode:
            result = self._live_buy(symbol, capital, price)
            if result:
                actual_price = result['price']
                order_id = str(result.get('raw', {}).get('response', {}).get('data', {}).get('statuses', [{}])[0].get('resting', {}).get('oid', ''))
                mode_tag = 'LIVE'
                logger.info(f"✅ LIVE BUY {symbol} @ {actual_price}")
            else:
                logger.error(f"Live buy failed for {symbol}, skipping")
                return None
        else:
            logger.info(f"[SIM] BUY {symbol} @ {actual_price}")

        amount = capital / actual_price
        trade = {
            'type': 'BUY',
            'symbol': symbol,
            'price': actual_price,
            'amount': amount,
            'usdt': capital,
            'reason': reason,
            'mode': mode_tag,
            'order_id': order_id,
            'time': datetime.now().isoformat(),
            'pnl': None
        }
        if symbol not in self.holdings:
            self.holdings[symbol] = {'entries': [], 'peak_price': actual_price, 'trailing_stop_price': 0}
        self.holdings[symbol]['entries'].append({
            'price': actual_price, 'amount': amount, 'usdt': capital, 'time': trade['time']
        })
        self.holdings[symbol]['peak_price'] = max(self.holdings[symbol].get('peak_price', actual_price), actual_price)
        cfg = self.coins.get(symbol, {})
        trail_pct = cfg.get('trailing_stop', 1.0) / 100
        self.holdings[symbol]['trailing_stop_price'] = actual_price * (1 - trail_pct)

        self.trades.append(trade)
        logger.info(f"[{mode_tag}] BUY {symbol} @ {actual_price:.6f} | {capital} USDC | {reason}")
        self._save_data()
        return trade

    def _execute_sell(self, symbol, price, reason):
        if symbol not in self.holdings or not self.holdings[symbol].get('entries'):
            return None
        entries = self.holdings[symbol]['entries']
        total_usdt = sum(e['usdt'] for e in entries)
        total_amt = sum(e['amount'] for e in entries)
        avg_entry = total_usdt / total_amt if total_amt > 0 else price
        actual_price = price
        order_id = None
        mode_tag = 'SIM'

        if self.live_mode:
            result = self._live_sell(symbol, total_amt, price)
            if result:
                actual_price = result['price']
                order_id = str(result.get('raw', {}).get('response', {}).get('data', {}).get('statuses', [{}])[0].get('resting', {}).get('oid', ''))
                mode_tag = 'LIVE'
                logger.info(f"✅ LIVE SELL {symbol} @ {actual_price}")
            else:
                logger.error(f"Live sell failed for {symbol}, skipping")
                return None
        else:
            logger.info(f"[SIM] SELL {symbol} @ {actual_price}")

        pnl_usdt = (actual_price - avg_entry) * total_amt
        pnl_pct = (actual_price - avg_entry) / avg_entry * 100

        trade = {
            'type': 'SELL',
            'symbol': symbol,
            'price': actual_price,
            'amount': total_amt,
            'usdt': actual_price * total_amt,
            'avg_entry': avg_entry,
            'pnl_usdt': round(pnl_usdt, 4),
            'pnl_pct': round(pnl_pct, 2),
            'reason': reason,
            'mode': mode_tag,
            'order_id': order_id,
            'dca_count': len(entries),
            'time': datetime.now().isoformat()
        }
        self.stats['total_trades'] += 1
        self.stats['total_profit'] += pnl_usdt
        if pnl_usdt > 0:
            self.stats['winning_trades'] += 1

        self.holdings[symbol] = {'entries': [], 'peak_price': 0, 'trailing_stop_price': 0}
        self.trades.append(trade)
        logger.info(f"[{mode_tag}] SELL {symbol} @ {actual_price:.6f} | PnL: {pnl_pct:.2f}% ({pnl_usdt:.4f} USDC) | {reason}")
        self._save_data()
        return trade

    def manual_sell(self, symbol, amount=None):
        price = self.client.get_spot_price(symbol) or 0
        if not price:
            return {'success': False, 'error': 'Could not fetch price'}
        trade = self._execute_sell(symbol, price, 'manual')
        if trade:
            return {'success': True, 'trade': trade}
        return {'success': False, 'error': 'No holdings or live sell failed'}

    def _check_trailing_stop(self, symbol, current_price):
        h = self.holdings.get(symbol)
        if not h or not h.get('entries'):
            return False
        cfg = self.coins.get(symbol, {})
        trail_pct = cfg.get('trailing_stop', 1.0) / 100
        if current_price > h.get('peak_price', 0):
            h['peak_price'] = current_price
            h['trailing_stop_price'] = current_price * (1 - trail_pct)
        if current_price <= h.get('trailing_stop_price', 0) and h['trailing_stop_price'] > 0:
            return True
        return False

    def _check_stop_loss(self, symbol, current_price):
        h = self.holdings.get(symbol)
        if not h or not h.get('entries'):
            return False
        cfg = self.coins.get(symbol, {})
        sl_pct = cfg.get('stop_loss', 1.5) / 100
        entries = h['entries']
        total_usdt = sum(e['usdt'] for e in entries)
        total_amt = sum(e['amount'] for e in entries)
        avg_entry = total_usdt / total_amt if total_amt > 0 else current_price
        return current_price <= avg_entry * (1 - sl_pct)

    # ─── BOT LOOP ─────────────────────────────────────────────────────────────

    def _bot_loop(self):
        logger.info(f"Bot loop started — mode: {'LIVE' if self.live_mode else 'SIMULATION'}")
        while self.running:
            try:
                for symbol, cfg in list(self.coins.items()):
                    if not cfg.get('enabled', True):
                        continue
                    self._process_coin(symbol, cfg)
                time.sleep(30)
            except Exception as e:
                logger.error(f"Bot loop error: {e}")
                time.sleep(5)

    def _process_coin(self, symbol, cfg):
        try:
            market = self.get_market_data(symbol)
            price = market.get('price', 0)
            if not price:
                return

            signal = market.get('signal', 'neutral')
            has_holding = bool(self.holdings.get(symbol, {}).get('entries'))
            capital = cfg.get('capital', 10)

            if has_holding:
                if self._check_trailing_stop(symbol, price):
                    entries = self.holdings[symbol]['entries']
                    total_usdt = sum(e['usdt'] for e in entries)
                    total_amt = sum(e['amount'] for e in entries)
                    avg_entry = total_usdt / total_amt if total_amt > 0 else price
                    if price > avg_entry:
                        self._execute_sell(symbol, price, 'trailing_stop')
                        return

                if self._check_stop_loss(symbol, price):
                    self._execute_sell(symbol, price, 'stop_loss')
                    return

                if signal in ['sell', 'strong_sell']:
                    entries = self.holdings[symbol]['entries']
                    total_usdt = sum(e['usdt'] for e in entries)
                    total_amt = sum(e['amount'] for e in entries)
                    avg_entry = total_usdt / total_amt if total_amt > 0 else price
                    if price > avg_entry * 1.005:
                        self._execute_sell(symbol, price, f'signal_{signal}')
                        return

                if signal in ['buy', 'strong_buy']:
                    entries = self.holdings[symbol]['entries']
                    total_usdt = sum(e['usdt'] for e in entries)
                    total_amt = sum(e['amount'] for e in entries)
                    avg_entry = total_usdt / total_amt if total_amt > 0 else price
                    if price < avg_entry * 0.97 and len(entries) < 5:
                        self._execute_buy(symbol, capital, price, 'dca')
            else:
                if signal in ['buy', 'strong_buy']:
                    self._execute_buy(symbol, capital, price, f'signal_{signal}')

        except Exception as e:
            logger.error(f"Process coin error {symbol}: {e}")

    def start(self, wallet, private_key):
        if self.running:
            return {'success': False, 'error': 'Bot already running'}
        self.wallet = wallet
        self.private_key = private_key

        live = self._init_sdk(wallet, private_key)
        mode = 'LIVE' if live else 'SIMULATION'

        self.running = True
        self.stats['start_time'] = datetime.now().isoformat()
        self.thread = threading.Thread(target=self._bot_loop, daemon=True)
        self.thread.start()
        return {'success': True, 'message': f'Bot started in {mode} mode', 'mode': mode}

    def stop(self):
        self.running = False
        self.live_mode = False
        self.exchange = None
        return {'success': True, 'message': 'Bot stopped'}
