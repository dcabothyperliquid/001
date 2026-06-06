# ─────────────────────────────────────────────────────────────────────────────
# main.py  —  HyperliquidClient + BotEngine + Flask App  (PARALLEL PRO VERSION)
#
# Architecture:
#   • asyncio event loop  — single dedicated thread, non-blocking everywhere
#   • websockets          — single WS → allMids live price feed (reconnect backoff)
#   • aiohttp             — async HTTP for candle fetches (20 coins × 4 TF = 80 parallel)
#   • CandleCache         — background refresh every 5 min; bot reads from cache (near-zero latency)
#   • ThreadPoolExecutor  — SDK order calls (blocking) offloaded, never block event loop
#   • asyncio.gather()    — all coins processed simultaneously each cycle
#   • Supports 20+ coins with no degradation as more coins are added
# ─────────────────────────────────────────────────────────────────────────────

import os, json, time, threading, logging, asyncio, aiohttp, websockets, contextlib

# Force eth_hash to use pycryptodome backend — must be set before eth_account import
os.environ.setdefault('ETH_HASH_BACKEND', 'pycryptodome')
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import requests
from flask import Flask, jsonify, request, render_template

# ── Supabase ──────────────────────────────────────────────────────────────────
_supabase = None
SUPABASE_OK = False
_SB_URL = os.environ.get('SUPABASE_URL', '').strip()
_SB_KEY = os.environ.get('SUPABASE_KEY', '').strip()
if _SB_URL and _SB_KEY:
    try:
        from supabase import create_client
        _supabase = create_client(_SB_URL, _SB_KEY)
        SUPABASE_OK = True
    except Exception as _e:
        print(f"[SUPABASE ERROR] {type(_e).__name__}: {_e}")
else:
    print("[SUPABASE] URL or KEY missing in environment")

try:
    from flask_cors import CORS
    CORS_AVAILABLE = True
except ImportError:
    CORS_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from datetime import timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST).isoformat()

# ── SDK ───────────────────────────────────────────────────────────────────────
try:
    # Patch eth_hash backend before eth_account imports it — Python 3.14 fix
    try:
        import eth_hash.backends.pycryptodome as _ehb
        if not hasattr(_ehb, 'backend'):
            from Crypto.Hash import keccak as _kc
            _ehb.backend = _kc
    except Exception:
        pass
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    import eth_account
    SDK_AVAILABLE = True
    logger.info("✅ Hyperliquid SDK loaded — LIVE TRADING ENABLED")
except ImportError:
    SDK_AVAILABLE = False
    logger.warning("⚠️  hyperliquid-python-sdk not installed — SIMULATION mode")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
MAINNET_URL     = "https://api.hyperliquid.xyz"
WS_URL          = "wss://api.hyperliquid.xyz/ws"
DATA_FILE       = "bot_data.json"
SCAN_TIMEFRAMES = ['15m', '30m', '1h', '2h', '4h']
CANDLE_LOOKBACK = 60
CACHE_TTL       = 300        # 5 minutes candle cache
PRICE_CACHE_TTL = 3          # 3 seconds price cache (WS updates this faster anyway)
MAX_WORKERS     = 10         # ThreadPoolExecutor for SDK order calls
CANDLE_SEMAPHORE = 5         # max parallel candle HTTP requests (rate limit safe)

SIGNAL_SCORES = {
    'buy':     +1,
    'neutral':  0,
    'sell':    -1,
}

INTERVAL_MS = {
    '1m': 60000,   '3m': 180000,  '5m': 300000,
    '15m': 900000, '30m': 1800000,
    '1h': 3600000, '2h': 7200000,
    '4h': 14400000,'8h': 28800000,'12h': 43200000,
    '1d': 86400000
}

# ═════════════════════════════════════════════════════════════════════════════
# CandleCache  —  thread-safe, async-refreshed
# ═════════════════════════════════════════════════════════════════════════════
class CandleCache:
    """
    Stores candles per (symbol, interval).
    Background task refreshes all registered symbols every CACHE_TTL seconds.
    Bot decision loop reads from cache — O(1), no network wait.
    """
    def __init__(self):
        self._cache: dict = {}          # (symbol, tf) → {'candles': [...], 'ts': float}
        self._lock  = asyncio.Lock()    # asyncio lock (used inside async ctx)
        self._tlock = threading.Lock()  # threading lock (used from sync ctx)

    def _key(self, symbol, tf): return f"{symbol}:{tf}"

    def get(self, symbol, tf):
        """Sync read — safe from any thread."""
        with self._tlock:
            v = self._cache.get(self._key(symbol, tf))
            return v['candles'] if v else None

    async def set(self, symbol, tf, candles):
        """Async write."""
        key = self._key(symbol, tf)
        async with self._lock:
            self._cache[key] = {'candles': candles, 'ts': time.time()}

    def set_sync(self, symbol, tf, candles):
        """Sync write — used during initial warm-up."""
        key = self._key(symbol, tf)
        with self._tlock:
            self._cache[key] = {'candles': candles, 'ts': time.time()}

    def is_stale(self, symbol, tf):
        with self._tlock:
            v = self._cache.get(self._key(symbol, tf))
            if not v: return True
            return time.time() - v['ts'] > CACHE_TTL

candle_cache = CandleCache()

# ═════════════════════════════════════════════════════════════════════════════
# PriceCache  —  live prices from WebSocket allMids feed
# ═════════════════════════════════════════════════════════════════════════════
class PriceCache:
    def __init__(self):
        self._prices: dict = {}     # "@{idx}" → float
        self._ts:    float = 0.0
        self._lock = threading.Lock()

    def update(self, mids: dict):
        with self._lock:
            self._prices.update(mids)
            self._ts = time.time()

    def get(self, spot_key: str):
        with self._lock:
            v = self._prices.get(spot_key)
            return float(v) if v else None

    def age(self):
        return time.time() - self._ts

    def all_mids(self):
        with self._lock:
            return dict(self._prices)

price_cache = PriceCache()

# ═════════════════════════════════════════════════════════════════════════════
# HyperliquidClient  —  sync HTTP (meta/balance/orders) + async candle fetch
# ═════════════════════════════════════════════════════════════════════════════
class HyperliquidClient:
    def __init__(self):
        self.base_url = MAINNET_URL
        self.session  = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self._meta_cache = None
        self._meta_ts    = 0
        self._sym_index: dict = {}
        self._markpx_cache: dict = {}
        self._markpx_ts: float  = 0
        # Eagerly warm sym_index on startup (non-fatal if it fails)
        try:
            self.get_spot_meta(force=True)
            logger.info(f"✅ spotMeta pre-loaded — {len(self._sym_index)} symbols indexed")
        except Exception as e:
            logger.warning(f"spotMeta pre-load failed (will retry on demand): {e}")

    # ── sync HTTP ─────────────────────────────────────────────────────────────
    def _post(self, payload):
        for attempt in range(4):
            try:
                r = self.session.post(f"{self.base_url}/info", json=payload, timeout=15)
                if r.status_code == 429:
                    wait = 2 ** attempt   # 1s, 2s, 4s, 8s
                    logger.warning(f"API 429 type={payload.get('type')} — retry in {wait}s")
                    time.sleep(wait)
                    continue
                if r.status_code != 200:
                    logger.error(f"API {r.status_code} type={payload.get('type')} — {r.text[:200]}")
                    return None
                return r.json()
            except Exception as e:
                logger.error(f"API error: {e}"); return None
        logger.error(f"API 429 type={payload.get('type')} — gave up after 4 retries")
        return None

    def get_spot_meta(self, force=False):
        now = time.time()
        if not force and self._meta_cache and now - self._meta_ts < 300:
            return self._meta_cache
        data = self._post({"type": "spotMeta"})
        if data:
            self._meta_cache = data
            self._meta_ts    = now
            tokens_list = data.get('tokens', [])
            token_idx_to_name = {}
            for t in tokens_list:
                tidx = t.get('index')
                tname = t.get('name', '').strip()
                if tidx is not None and tname:
                    token_idx_to_name[tidx] = tname.upper()
            idx_map = {}
            for i, p in enumerate(data.get('universe', [])):
                tok_idxs = p.get('tokens', [])
                uni_name = p.get('name', '').strip().upper()
                base_name = ''
                if tok_idxs:
                    base_name = token_idx_to_name.get(tok_idxs[0], '')
                if not base_name and uni_name:
                    base_name = uni_name.split('/')[0].strip()
                if base_name:
                    idx_map[base_name] = i
                    idx_map[base_name + '/USDC'] = i
                    idx_map[base_name + 'USDC'] = i
            # ── Alias map: user types "SOL" → HL actual name "USOL" ──────────
            # IMPORTANT: These aliases MUST override any native token with same name
            # e.g. HL may have a native "ETH" token at a different index than "UETH"
            # We always want ETH→UETH, SOL→USOL, BTC→UBTC (the wrapped versions)
            ALIASES = {
                'BTC': 'UBTC', 'SOL': 'USOL', 'ETH': 'UETH',
                'BNB': 'BNB0', 'AVAX': 'AVAX0',
                'LINK': 'LINK0', 'AAVE': 'AAVE0', 'XRP': 'FXRP',
                'ZEC': 'UZEC', 'WLD': 'UWLD', 'MOG': 'UMOG',
                'PUMP': 'UPUMP', 'PENGU': 'HPENGU', 'PEPE': 'HPEPE',
                'PUMPFUN': 'HPUMP', 'XMR': 'XMR1', 'TAO': 'TAO1',
                'HYPE': 'HYPE',
            }
            for alias, real in ALIASES.items():
                if real in idx_map:
                    # Force-set alias even if a native token already claimed this key
                    idx_map[alias] = idx_map[real]
                    idx_map[alias + '/USDC'] = idx_map[real]
            self._sym_index = idx_map
            logger.info(f"spotMeta: {len(set(idx_map.values()))} pairs, aliases applied: {[a for a,r in ALIASES.items() if r in idx_map]}")
            # DEBUG — log key tokens to verify index mapping
            for dbg_sym in ['SOL', 'USOL', 'HYPE', 'BTC', 'UBTC', 'ETH', 'UETH']:
                logger.info(f"  [idx_map] {dbg_sym} → uni={idx_map.get(dbg_sym)}")
        return data

    def _resolve_candle_coin(self, symbol: str) -> str:
        """Return the coin string to use for WS candle subscription.
        Hyperliquid spot candles use '@{uni_index}' format e.g. '@152' for SOL.
        """
        idx = self.sym_to_index(symbol)
        if idx is not None:
            return f"@{idx}"
        return symbol.upper()

    def _resolve_coin_from_id(self, coin_id: str) -> str:
        """Reverse lookup: '@152' → 'SOL' (or whichever symbol the bot uses).
        Also handles plain name like 'HYPE'.
        """
        if coin_id.startswith('@'):
            try:
                target_idx = int(coin_id[1:])
                for sym in self.bot_coins_ref():
                    idx = self.sym_to_index(sym)
                    if idx == target_idx:
                        return sym
            except ValueError:
                pass
        # Plain name fallback
        for sym in self.bot_coins_ref():
            internal = self._sym_index.get(sym.upper())
            if coin_id.upper() in (sym.upper(), str(internal)):
                return sym
        return None

    def set_bot_coins_ref(self, coins_dict_ref):
        """Store reference to bot.coins so _resolve_coin_from_id can look up symbols."""
        self._bot_coins_ref = coins_dict_ref

    def bot_coins_ref(self):
        return list(getattr(self, '_bot_coins_ref', {}).keys())

    def get_spot_pairs(self):
        """
        Fetch live Hyperliquid spot token list.
        Returns clean display names (SOL, BTC, ETH) while keeping
        the internal HL name (USOL, UBTC, UETH) for order routing.
        Auto-updates: new listings appear, delisted tokens disappear.
        """
        import re as _re
        data = self._post({"type": "spotMetaAndAssetCtxs"})
        if not data or not isinstance(data, list) or len(data) < 1:
            data = [self.get_spot_meta(), []]
        meta = data[0] if isinstance(data, list) else data
        asset_ctxs = data[1] if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) else []

        # Build token index → raw name map
        token_idx_to_name = {}
        for t in meta.get('tokens', []):
            tidx  = t.get('index')
            tname = t.get('name', '').strip()
            if tidx is not None and tname:
                token_idx_to_name[tidx] = tname

        def _clean_display(raw: str) -> str:
            """Return display name. Only PRIORITY_DISPLAY handles remapping.
            Everything else shows as-is (ADHD, ANON, AAPL etc.)
            """
            return raw.upper().strip()

        # Build a lookup: coin identifier -> markPx context
        # asset_ctxs[j].coin can be "PURR/USDC" or "@{token_index}"
        ctx_by_coin = {}
        for ctx in asset_ctxs:
            coin = ctx.get('coin', '')
            if coin:
                ctx_by_coin[coin] = ctx

        pairs = []
        seen_display = set()
        seen_internal = set()
        # Priority list — these must always appear with their canonical display name
        PRIORITY_DISPLAY = {
            'UBTC':'BTC','USOL':'SOL','UETH':'ETH',
            'UZEC':'ZEC','UWLD':'WLD','UMOG':'MOG','UPUMP':'PUMP',
            'HPENGU':'PENGU','HPEPE':'PEPE','HPUMP':'PUMPFUN','FXRP':'XRP',
            'BNB0':'BNB','AVAX0':'AVAX','LINK0':'LINK',
            'AAVE0':'AAVE','XMR1':'XMR','TAO1':'TAO','HYPE':'HYPE',
        }
        for i, u in enumerate(meta.get('universe', [])):
            tok_indices = u.get('tokens', [])
            internal_name = ''
            if tok_indices:
                internal_name = token_idx_to_name.get(tok_indices[0], '')
            if not internal_name:
                internal_name = u.get('name', '').split('/')[0].strip().upper()
            if not internal_name or internal_name == 'USDC':
                continue
            if internal_name in seen_internal:
                continue
            seen_internal.add(internal_name)

            # Use priority map first, then _clean_display
            if internal_name in PRIORITY_DISPLAY:
                display_name = PRIORITY_DISPLAY[internal_name]
            else:
                display_name = _clean_display(internal_name)
            if display_name == 'USDC':
                continue
            # If display already claimed by a non-priority token, skip
            if display_name in seen_display and internal_name not in PRIORITY_DISPLAY:
                continue
            # Priority tokens always win — remove previous entry if it claimed this display
            if display_name in seen_display and internal_name in PRIORITY_DISPLAY:
                pairs = [p for p in pairs if p['display'] != display_name]
                seen_display.discard(display_name)

            # Properly match asset_ctxs by coin field (NOT by raw index i)
            # HL uses "NAME/USDC" for some tokens, "@{token_index}" for others
            ctx = None
            if tok_indices:
                # First try @{token_index} — most reliable
                ctx = ctx_by_coin.get(f"@{tok_indices[0]}")
            if ctx is None:
                # Try named pair format
                ctx = ctx_by_coin.get(f"{internal_name}/USDC")
            if ctx is None:
                # Last fallback: try universe index
                ctx = ctx_by_coin.get(f"@{i}")

            # Only include tokens with an active market (markPx > 0)
            if ctx is not None:
                mark_px = float(ctx.get('markPx', 0) or 0)
                if mark_px <= 0:
                    continue
            # If no ctx found, include anyway (API may be slow/missing data)

            seen_display.add(display_name)

            pairs.append({
                'index':    i,
                'name':     internal_name,   # used internally for orders
                'display':  display_name,    # shown to user (SOL, BTC etc.)
                'asset_id': 10000 + i
            })

        # Sort by display name so list is alphabetical in UI
        return sorted(pairs, key=lambda x: x['display'])

    def sym_to_index(self, symbol: str):
        """Returns universe index for spot symbol (cached in _sym_index)."""
        self.get_spot_meta()   # refresh if stale
        return self._sym_index.get(symbol.upper())

    # ── prices ────────────────────────────────────────────────────────────────
    def get_all_mids(self):
        """Sync fallback when WS price cache is empty/stale."""
        return self._post({"type": "allMids"}) or {}

    def _refresh_markpx(self):
        """Cache prices from allMids — returns plain token names as keys.
        e.g. {"HYPE": "35.2", "USOL": "178.5", "UBTC": "62000"}
        Refreshed every 3 seconds.
        """
        if time.time() - self._markpx_ts < 3:
            return
        mids = self._post({"type": "allMids"})
        if mids and isinstance(mids, dict):
            cache = {}
            # Build reverse index: uni_index → symbol name
            rev = {str(v): k for k, v in self._sym_index.items()}
            for name, px in mids.items():
                try:
                    fval = float(px)
                except:
                    continue
                cache[name] = fval
                # Also store under resolved symbol name if key is @idx format
                if name.startswith('@'):
                    sym = rev.get(name[1:])
                    if sym:
                        cache[sym] = fval
            if cache:
                self._markpx_cache = cache
                self._markpx_ts = time.time()
                # DEBUG — log key spot tokens once
                if not getattr(self, '_mids_logged', False):
                    self._mids_logged = True
                    for sym in ['USOL','HYPE','UBTC','UETH','BNB0','AAVE0','TRX1']:
                        logger.info(f"  [allMids] {sym} = {cache.get(sym, 'NOT FOUND')} | @152={cache.get('@152','?')}")

    def get_spot_price(self, symbol):
        idx = self.sym_to_index(symbol)
        if idx is None:
            self.get_spot_meta(force=True)
            idx = self.sym_to_index(symbol)

        def _sane(px):
            if not px or px <= 0: return False
            if px == 1.0: return False
            return True

        ALIASES = {
            'BTC':'UBTC','SOL':'USOL','ETH':'UETH','TRX':'TRX1','BNB':'BNB0',
            'AVAX':'AVAX0','LINK':'LINK0','AAVE':'AAVE0','XRP':'FXRP',
            'ZEC':'UZEC','WLD':'UWLD','MOG':'UMOG','PUMP':'UPUMP',
            'PENGU':'HPENGU','PEPE':'HPEPE','PUMPFUN':'HPUMP','XMR':'XMR1','TAO':'TAO1',
        }
        internal = ALIASES.get(symbol.upper(), symbol.upper())

        # 1. WS live price cache — always try first, zero HTTP cost
        p = price_cache.get(internal)
        if _sane(p): return float(p)

        # 1b. markPx cache — also has @idx resolved names from _refresh_markpx
        self._refresh_markpx()
        for key in [internal, symbol.upper(), f'@{idx}' if idx is not None else None]:
            if key:
                p = self._markpx_cache.get(key)
                if _sane(p): return float(p)

        # 2. REST fallback — ONLY when WS is stale/dead (>15s no update)
        if price_cache.age() >= 15:
            mids = self.get_all_mids()
            for key in [internal, f"{internal}/USDC", symbol.upper(), f"{symbol}/USDC", f'@{idx}' if idx is not None else None]:
                if key and key in mids:
                    try:
                        px = float(mids[key])
                        if _sane(px): return px
                    except: pass

        # 3. Last resort — candle close price from cache (no network)
        # Use internal spot name (USOL, UBTC) not display name (SOL, BTC)
        # to avoid accidentally reading perp candle prices
        for tf in ['1m', '5m', '15m', '1h']:
            for name in [internal, symbol.upper()]:
                candles = candle_cache.get(name, tf)
                if candles:
                    px = float(candles[-1][4])
                    if _sane(px): return px

        return None

    # ── async candle fetch ────────────────────────────────────────────────────
    async def async_get_candles(self, session: aiohttp.ClientSession,
                                 symbol: str, interval: str,
                                 lookback: int = CANDLE_LOOKBACK,
                                 semaphore: asyncio.Semaphore = None):
        """Fully async candle fetch — used by cache refresh task."""
        tf      = interval
        ms      = INTERVAL_MS.get(tf, 3600000)
        now_ms  = int(time.time() * 1000)
        start   = now_ms - lookback * ms

        idx     = self.sym_to_index(symbol)
        coin_id = f"@{idx}" if idx is not None else symbol

        payload = {"type": "candleSnapshot",
                   "req": {"coin": coin_id, "interval": tf,
                            "startTime": start, "endTime": now_ms}}
        try:
            ctx = semaphore if semaphore else contextlib.AsyncExitStack()
            async with ctx:
                for attempt in range(3):
                    async with session.post(f"{self.base_url}/info", json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status == 429:
                            wait = 2 ** attempt  # 1s, 2s, 4s
                            await asyncio.sleep(wait)
                            continue
                        if resp.status != 200:
                            logger.warning(f"Candle {symbol}/{tf} HTTP {resp.status}")
                            return []
                        raw = await resp.json()
                        break
                else:
                    logger.warning(f"Candle {symbol}/{tf} — 429 after 3 retries, skipping")
                    return []
        except Exception as e:
            logger.warning(f"Candle fetch error {symbol}/{tf}: {e}"); return []

        candles = []
        for c in (raw or []):
            if isinstance(c, dict):
                candles.append([c.get('t',0), float(c.get('o',0)), float(c.get('h',0)),
                                 float(c.get('l',0)), float(c.get('c',0)), float(c.get('v',0))])
            elif isinstance(c, list) and len(c) >= 6:
                candles.append(c)
        return candles

    # sync wrapper (used for initial warm-up + fallback)
    def get_candles(self, symbol, interval, lookback=CANDLE_LOOKBACK):
        # Try cache first
        cached = candle_cache.get(symbol, interval)
        if cached: return cached
        # Fallback sync REST
        tf      = interval
        ms      = INTERVAL_MS.get(tf, 3600000)
        now_ms  = int(time.time() * 1000)
        start   = now_ms - lookback * ms
        idx     = self.sym_to_index(symbol)
        coin_id = f"@{idx}" if idx is not None else symbol
        raw = self._post({"type": "candleSnapshot",
                          "req": {"coin": coin_id, "interval": tf,
                                  "startTime": start, "endTime": now_ms}})
        candles = []
        for c in (raw or []):
            if isinstance(c, dict):
                candles.append([c.get('t',0), float(c.get('o',0)), float(c.get('h',0)),
                                 float(c.get('l',0)), float(c.get('c',0)), float(c.get('v',0))])
            elif isinstance(c, list) and len(c) >= 6:
                candles.append(c)
        if candles:
            candle_cache.set_sync(symbol, interval, candles)
        return candles

    # ── balance ───────────────────────────────────────────────────────────────
    def get_spot_balance(self, address=None):
        address = address or os.environ.get('WALLET_ADDRESS', '')
        if not address: return {'error': 'No wallet address'}
        data = self._post({"type": "spotClearinghouseState", "user": address})
        if not data: return {}
        return {b['coin']: {'total': float(b.get('total',0)),
                             'hold':  float(b.get('hold',0)),
                             'available': float(b.get('total',0)) - float(b.get('hold',0))}
                for b in data.get('balances', [])}

    def get_order_book(self, symbol):
        return self._post({"type": "l2Book", "coin": symbol})


# ═════════════════════════════════════════════════════════════════════════════
# AsyncEngine  —  asyncio event loop running in a dedicated thread
#   • WebSocket allMids feed (reconnect with exponential backoff)
#   • CandleCache refresh task (every 5 min, all coins × all TFs in parallel)
#   • Bot decision loop (all coins in parallel via asyncio.gather)
# ═════════════════════════════════════════════════════════════════════════════
class AsyncEngine:
    def __init__(self, client: HyperliquidClient, bot_engine):
        self.client      = client
        self.bot         = bot_engine
        self.loop        = asyncio.new_event_loop()
        self._thread     = None
        self._executor   = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self._ws_running = False
        self._sem        = asyncio.Semaphore(CANDLE_SEMAPHORE)   # created in loop
        self._refreshing = True   # held until WS seed completes — blocks _cache_refresh_loop on startup

    def start(self):
        if self._thread and self._thread.is_alive():
            logger.warning("AsyncEngine already running — ignoring duplicate start")
            return
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="AsyncEngine")
        self._thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self._sem = asyncio.Semaphore(CANDLE_SEMAPHORE)
        self.loop.run_until_complete(self._main())

    async def _main(self):
        logger.info("⚡ AsyncEngine started")
        await asyncio.gather(
            self._ws_feed(),
            self._ws_candle_feed(),
            self._cache_refresh_loop(),
            self._decision_loop(),
        )

    # ── WebSocket allMids feed ────────────────────────────────────────────────
    async def _ws_feed(self):
        backoff = 1
        while True:
            try:
                logger.info(f"🔌 WS connecting → {WS_URL}")
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=30) as ws:
                    backoff = 1
                    sub = json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}})
                    await ws.send(sub)
                    logger.info("✅ WS subscribed to allMids")
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            if msg.get('channel') == 'allMids':
                                mids = msg.get('data', {}).get('mids', {})
                                if mids:
                                    price_cache.update(mids)
                        except Exception as e:
                            logger.warning(f"WS parse error: {e}")
            except Exception as e:
                logger.warning(f"WS disconnected: {e} — reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # ── WebSocket candle feed — real-time push, replaces REST polling ─────────
    async def _ws_candle_feed(self):
        """Subscribe to candle updates for all monitored coins × all TFs via WS.
        On each candle push → update cache → immediately trigger signal check.
        Falls back to REST snapshot on connect to seed the cache.
        """
        backoff = 1
        subscribed_coins = set()   # persists across reconnects — prevents duplicate seed+subscribe
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=30) as ws:
                    backoff = 1

                    async def _subscribe_coins():
                        """Subscribe to candles for any new coins."""
                        coins = list(self.bot.coins.keys())
                        new_coins = [c for c in coins if c not in subscribed_coins]
                        if not new_coins:
                            return
                        # Block _cache_refresh_loop while seeding
                        self._refreshing = True
                        try:
                            # One shared session for all coins — avoids connection burst
                            async with aiohttp.ClientSession() as sess:
                                for sym in new_coins:
                                    # Seed cache via REST — one TF at a time with delay
                                    for tf in SCAN_TIMEFRAMES:
                                        candles = await self.client.async_get_candles(sess, sym, tf, semaphore=self._sem)
                                        if candles:
                                            await candle_cache.set(sym, tf, candles)
                                        await asyncio.sleep(0.3)   # 300ms gap per TF to avoid burst
                                    # Subscribe via WS for live updates
                                    for tf in SCAN_TIMEFRAMES:
                                        coin_id = self.client._resolve_candle_coin(sym)
                                        sub = json.dumps({"method": "subscribe", "subscription": {
                                            "type": "candle", "coin": coin_id, "interval": tf}})
                                        await ws.send(sub)
                                    subscribed_coins.add(sym)
                                    logger.info(f"✅ WS candle subscribed: {sym} × {SCAN_TIMEFRAMES}")
                        finally:
                            self._refreshing = False

                    await _subscribe_coins()

                    # Periodic re-check for newly added coins
                    async def _coin_watcher():
                        while True:
                            await asyncio.sleep(10)
                            await _subscribe_coins()

                    asyncio.ensure_future(_coin_watcher())

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            if msg.get('channel') == 'candle':
                                data = msg.get('data', {})
                                if not data: continue
                                coin_id  = data.get('s', '')
                                interval = data.get('i', '')
                                sym = self.client._resolve_coin_from_id(coin_id)
                                if not sym or interval not in SCAN_TIMEFRAMES: continue

                                # Live candle update — replace last or append
                                existing = candle_cache.get(sym, interval) or []
                                candle = [int(data['t']), float(data['o']), float(data['h']),
                                          float(data['l']), float(data['c']), float(data['v'])]
                                if existing and existing[-1][0] == data['t']:
                                    existing[-1] = candle
                                else:
                                    existing.append(candle)
                                    if len(existing) > 500: existing = existing[-500:]
                                await candle_cache.set(sym, interval, existing)
                                if self.bot.running:
                                    cfg = self.bot.coins.get(sym, {})
                                    asyncio.ensure_future(self._process_coin(sym, cfg))
                        except Exception as e:
                            logger.warning(f"WS candle parse error: {e}")

            except Exception as e:
                logger.warning(f"WS candle disconnected: {e} — reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # ── CandleCache refresh (fallback, runs every 5 min as backup) ────────────
    async def _cache_refresh_loop(self):
        """Backup REST refresh every 60s — fills gaps if WS candle misses anything."""
        await asyncio.sleep(30)  # small buffer after WS seed releases lock
        while True:
            coins = list(self.bot.coins.keys())
            if coins:
                await self._refresh_candles(coins)
            await asyncio.sleep(60)

    async def _refresh_candles(self, coins: list):
        if not coins: return
        if getattr(self, '_refreshing', False):
            logger.debug("⏭ Candle refresh skipped — already in progress")
            return
        self._refreshing = True
        try:
            t0 = time.time()
            logger.info(f"🔄 Refreshing candles: {len(coins)} coins × {len(SCAN_TIMEFRAMES)} TFs = {len(coins)*len(SCAN_TIMEFRAMES)} requests")
            # Single shared session for all requests — avoids TCP burst
            async with aiohttp.ClientSession() as session:
                for sym in coins:
                    for tf in SCAN_TIMEFRAMES:
                        await self._fetch_and_cache(session, sym, tf)
                        await asyncio.sleep(0.4)   # 400ms gap per request — safe under HL rate limit
            logger.info(f"✅ Candle cache refreshed in {time.time()-t0:.2f}s for {len(coins)} coins")
        finally:
            self._refreshing = False

    async def _fetch_and_cache(self, session, symbol, tf):
        candles = await self.client.async_get_candles(session, symbol, tf,
                                                       semaphore=self._sem)
        if candles:
            await candle_cache.set(symbol, tf, candles)

    # ── Decision loop ─────────────────────────────────────────────────────────
    async def _decision_loop(self):
        """All coins processed in parallel every 15s."""
        await asyncio.sleep(15)   # let WS + cache warm up first
        while True:
            if not self.bot.running:
                await asyncio.sleep(5); continue
            coins = [(sym, cfg) for sym, cfg in list(self.bot.coins.items())
                     if cfg.get('enabled', True)]
            if coins:
                t0 = time.time()
                await asyncio.gather(*[self._process_coin(sym, cfg) for sym, cfg in coins],
                                      return_exceptions=True)
                logger.info(f"⚡ Processed {len(coins)} coins in {time.time()-t0:.2f}s")
            await asyncio.sleep(15)

    async def _process_coin(self, symbol: str, cfg: dict):
        """Async version of _process_coin — reads from cache, offloads orders to executor."""
        try:
            # MTF scan — purely CPU + cache reads, no network
            mtf   = self.bot._mtf_scan(symbol)
            price = self.client.get_spot_price(symbol) or 0
            if not price:
                candles = candle_cache.get(symbol, '1h')
                if candles: price = float(candles[-1][4])
            if not price: return

            mtf_score   = mtf.get('total_score', 0)
            direction   = mtf.get('signal', 'neutral')
            confidence  = mtf.get('confidence', 'low')
            best_tf     = mtf.get('best_timeframe', '?')
            # Always use full compound_capital for trade — capital is fully committed per coin
            effective_capital = cfg.get('compound_capital', cfg.get('capital', 10))
            trade_cap   = round(effective_capital, 4)
            # Hard guard — if already holding this coin, NEVER buy again until sold
            has_holding = bool(self.bot.holdings.get(symbol, {}).get('entries'))

            self.bot._push_event('monitor',
                f"{symbol} | score={mtf_score} | {direction} | {confidence} | {best_tf} | {price:.6f}",
                {'symbol': symbol, 'score': mtf_score, 'direction': direction,
                 'confidence': confidence, 'price': price,
                 'tf_breakdown': mtf.get('tf_breakdown', {})})

            # Push per-TF breakdown events so all timeframes show in Live Events
            tf_breakdown = mtf.get('tf_breakdown', {})
            for tf_key in SCAN_TIMEFRAMES:
                tf_sig = tf_breakdown.get(tf_key, 'neutral')
                tf_data = (mtf.get('tf_results') or {}).get(tf_key, {})
                rsi_val = tf_data.get('rsi', '—') if tf_data else '—'
                self.bot._push_event('monitor',
                    f"{symbol} | score={SIGNAL_SCORES.get(tf_sig,0)} | {tf_sig} | {'high' if tf_key == best_tf else 'low'} | {tf_key} | {price:.6f}",
                    {'symbol': symbol, 'score': SIGNAL_SCORES.get(tf_sig, 0),
                     'direction': tf_sig, 'confidence': 'high' if tf_key == best_tf else 'low',
                     'price': price, 'timeframe': tf_key})

            if has_holding:
                holding   = self.bot.holdings[symbol]
                entries   = holding['entries']
                avg_entry = sum(e['usdt'] for e in entries) / sum(e['amount'] for e in entries)
                trade_tf  = holding.get('trade_tf', best_tf)  # usi TF pe monitor karo jis pe buy hua
                atr       = (mtf.get('tf_results') or {}).get(trade_tf, {}).get('atr', 0.0)

                # ATR-based trailing stop — 2x ATR below peak price
                peak = holding.get('peak_price', avg_entry)
                if price > peak:
                    holding['peak_price'] = price
                    peak = price

                atr_sl_price = peak - (2 * atr) if atr > 0 else avg_entry * 0.97

                # 1. ATR trailing stop loss
                if price <= atr_sl_price:
                    await self._run_order(self.bot._execute_sell, symbol, price, f'atr_trailing_sl')
                    return

                # 2. Signal-based sell — same TF pe bearish reversal
                tf_sig = (mtf.get('tf_results') or {}).get(trade_tf, {}).get('signal', 'neutral')
                if tf_sig == 'sell':
                    await self._run_order(self.bot._execute_sell, symbol, price, f'signal_sell_{trade_tf}')
                    return

            else:
                if direction == 'buy' and trade_cap > 0:
                    await self._run_order(self.bot._execute_buy, symbol, trade_cap, price,
                                          f'signal_buy_{best_tf}')

        except Exception as e:
            logger.error(f"Async process error {symbol}: {e}")
            self.bot._push_event('error', f"Process error {symbol}: {e}", {'symbol': symbol})

    async def _run_order(self, fn, *args):
        """Run a blocking SDK order call in ThreadPoolExecutor — never blocks event loop."""
        try:
            return await self.loop.run_in_executor(self._executor, fn, *args)
        except Exception as e:
            logger.error(f"Order executor error: {e}")

    def trigger_cache_refresh(self, coins: list):
        """Called when new coin is added — seed cache via REST immediately."""
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self._refresh_candles(coins), self.loop)


# ═════════════════════════════════════════════════════════════════════════════
# BotEngine  —  state management, indicators, order execution
# ═════════════════════════════════════════════════════════════════════════════
class BotEngine:
    def __init__(self, client: HyperliquidClient):
        self.client      = client
        self.exchange    = None
        self.info        = None
        self.running     = False
        self.thread      = None
        self.wallet      = ''
        self.private_key = ''
        self.live_mode   = False

        self.coins    = {}
        self.holdings = {}
        self.trades   = []
        self.stats    = {'total_trades': 0, 'winning_trades': 0,
                         'total_profit': 0.0, 'daily_profit': 0.0, 'start_time': None}
        self._events          = deque(maxlen=500)
        self._critical_events = deque(maxlen=200)  # persisted across restarts
        self._user_stopped   = False
        self._persisted_running = None   # loaded from storage
        self._balance_cache  = {}        # cached USDC balance
        self._balance_ts     = 0         # timestamp of last balance fetch
        self._async_eng  = AsyncEngine(client, self)
        self.client.set_bot_coins_ref(self.coins)
        self._load_data()
        self._load_critical_events()

    # ── Events ────────────────────────────────────────────────────────────────
    def _push_event(self, etype, msg, data=None):
        event = {'type': etype, 'message': msg, 'data': data or {}, 'time': now_ist()}
        self._events.appendleft(event)
        # Persist critical/error/buy/sell events so they survive restarts
        if etype in ('error', 'critical', 'buy', 'sell', 'warn'):
            self._critical_events.appendleft(event)
            self._save_critical_events()

    def get_events(self):
        # Merge live events with persisted critical events (deduplicated by time+message)
        live = list(self._events)
        persisted = list(self._critical_events)
        seen = set()
        merged = []
        for e in live + persisted:
            key = f"{e.get('time','')}|{e.get('message','')[:60]}"
            if key not in seen:
                seen.add(key)
                merged.append(e)
        # Sort newest first
        merged.sort(key=lambda x: x.get('time',''), reverse=True)
        return merged[:500]

    def _save_critical_events(self):
        events_list = list(self._critical_events)
        if SUPABASE_OK:
            try:
                _supabase.table('bot_data').upsert(
                    {'key': 'critical_events', 'value': events_list,
                     'updated_at': datetime.now(IST).isoformat()}
                ).execute()
            except Exception:
                pass
        try:
            with open('critical_events.json', 'w') as f:
                json.dump(events_list, f)
        except Exception:
            pass

    def _load_critical_events(self):
        if SUPABASE_OK:
            try:
                res = _supabase.table('bot_data').select('value').eq('key','critical_events').execute()
                rows = res.data if isinstance(res.data, list) else []
                if rows and rows[0].get('value'):
                    d = rows[0]['value']
                    if isinstance(d, str):
                        import json as _j; d = _j.loads(d)
                    if isinstance(d, list):
                        for e in d: self._critical_events.appendleft(e)
                        logger.info(f"✅ Loaded {len(d)} persisted critical events")
                        return
            except Exception as e:
                logger.warning(f"Critical events Supabase load: {e}")
        if os.path.exists('critical_events.json'):
            try:
                with open('critical_events.json') as f:
                    d = json.load(f)
                for e in d: self._critical_events.appendleft(e)
                logger.info(f"✅ Loaded {len(d)} critical events from local JSON")
            except Exception:
                pass

    # ── Persistence (Supabase primary, local JSON fallback) ─────────────────
    def _save_data(self):
        payload = {
            'coins':        self.coins,
            'holdings':     self.holdings,
            'trades':       self.trades[-5000:],
            'stats':        self.stats,
            'user_stopped': self._user_stopped,
            'bot_running':  self.running,
        }
        if SUPABASE_OK:
            try:
                logger.info(f"💾 Attempting Supabase save — {len(self.coins)} coins")
                res = _supabase.table('bot_data').upsert(
                    {'key': 'state', 'value': payload, 'updated_at': datetime.now(IST).isoformat()}
                ).execute()
                logger.info(f"✅ Supabase saved — {len(self.coins)} coins | {list(self.coins.keys())}")
                return
            except Exception as e:
                logger.error(f"Supabase save error: {type(e).__name__}: {e}")
                print(f"[SUPABASE SAVE ERROR] {type(e).__name__}: {e}")
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump(payload, f, indent=2)
            logger.info(f"💾 Local JSON saved — {len(self.coins)} coins")
        except Exception as e:
            logger.error(f"Local save error: {e}")

    def _load_data(self):
        if SUPABASE_OK:
            try:
                res = _supabase.table('bot_data').select('value').eq('key', 'state').execute()
                rows = res.data if isinstance(res.data, list) else []
                logger.info(f"Supabase load response: rows={len(rows)}")
                if len(rows) > 0 and rows[0].get('value'):
                    d = rows[0]['value']
                    if isinstance(d, str):
                        import json as _json
                        d = _json.loads(d)
                    self.coins         = d.get('coins', {})
                    self.holdings      = d.get('holdings', {})
                    self.trades        = d.get('trades', [])
                    self.stats         = d.get('stats', self.stats)
                    self._user_stopped = d.get('user_stopped', False)
                    self._persisted_running = d.get('bot_running', None)
                    logger.info(f"✅ Supabase loaded — {len(self.coins)} coins | {list(self.coins.keys())}")
                    self._backfill_display_names()
                    return
                else:
                    logger.warning(f"⚠️ Supabase no data found — fresh start (rows={len(rows)})")
            except Exception as e:
                logger.error(f"Supabase load error: {e} — falling back to local JSON")
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE) as f: d = json.load(f)
                self.coins         = d.get('coins', {})
                self.holdings      = d.get('holdings', {})
                self.trades        = d.get('trades', [])
                self.stats         = d.get('stats', self.stats)
                self._user_stopped = d.get('user_stopped', False)
                self._persisted_running = d.get('bot_running', None)
                logger.info(f"Local JSON loaded — {len(self.coins)} coins | user_stopped={self._user_stopped}")
                self._backfill_display_names()
            except Exception as e:
                logger.error(f"Local load error: {e}")

    def _backfill_display_names(self):
        """Add display_name to coins and deduplicate by display_name.
        Also backfills compound_capital for coins that don't have it yet.
        """
        DISPLAY_MAP = {
            'UBTC':'BTC','USOL':'SOL','UETH':'ETH','UZEC':'ZEC','UWLD':'WLD',
            'UMOG':'MOG','UPUMP':'PUMP','HPENGU':'PENGU','HPEPE':'PEPE',
            'HPUMP':'PUMPFUN','FXRP':'XRP','TRX1':'TRX','BNB0':'BNB',
            'AVAX0':'AVAX','LINK0':'LINK','AAVE0':'AAVE','XMR1':'XMR','TAO1':'TAO',
        }
        # Reverse map — display_name → canonical symbol key (prefer shorter/cleaner key)
        CANONICAL = {v: k for k, v in DISPLAY_MAP.items()}
        # First pass — assign display names
        for sym, cfg in self.coins.items():
            if not cfg.get('display_name'):
                if sym in DISPLAY_MAP:
                    cfg['display_name'] = DISPLAY_MAP[sym]
                else:
                    cfg['display_name'] = sym
        # Second pass — deduplicate: if same display_name appears twice, remove the internal one
        seen_display = {}
        to_remove = []
        for sym, cfg in self.coins.items():
            dn = cfg.get('display_name', sym)
            if dn in seen_display:
                prev_sym = seen_display[dn]
                # Keep the one whose key matches display_name (e.g. SOL over USOL)
                if sym == dn:
                    to_remove.append(prev_sym)
                    seen_display[dn] = sym
                else:
                    to_remove.append(sym)
            else:
                seen_display[dn] = sym
        for sym in to_remove:
            logger.info(f"Dedup: removing duplicate coin key '{sym}' (display already claimed)")
            self.coins.pop(sym, None)
        # Third pass — backfill compound_capital for old coins that don't have it
        for sym, cfg in self.coins.items():
            if 'compound_capital' not in cfg:
                cfg['compound_capital'] = cfg.get('capital', 10)
                logger.info(f"💰 Backfilled compound_capital for {sym}: {cfg['compound_capital']}")

    # ── SDK Init ──────────────────────────────────────────────────────────────
    def _init_sdk(self, wallet, private_key):
        if not SDK_AVAILABLE: return False
        if not private_key or len(private_key) < 10: return False
        # Fix eth_hash backend for Python 3.14 — force pycryptodome backend explicitly
        try:
            import eth_hash.backends.pycryptodome  # noqa
            from eth_hash.auto import keccak as _k; _k(b'test')  # warm up
        except Exception:
            try:
                import eth_hash.backends.pysha3  # noqa — fallback backend
            except Exception:
                pass
        try:
            account = eth_account.Account.from_key(private_key)
        except Exception as e:
            logger.error(f"SDK init failed (key error): {e}"); self.live_mode = False; return False
        # Retry Info() init — can 429 on startup rush
        last_err = None
        for attempt in range(4):
            try:
                self.info     = Info(MAINNET_URL, skip_ws=True)
                self.exchange = Exchange(account, MAINNET_URL,
                                         account_address=wallet or account.address)
                self.wallet    = wallet or account.address
                self.live_mode = True
                logger.info(f"✅ SDK init — {account.address[:10]}...")
                return True
            except Exception as e:
                last_err = e
                wait = 2 ** attempt  # 1s, 2s, 4s, 8s
                logger.warning(f"SDK init attempt {attempt+1} failed: {e} — retry in {wait}s")
                time.sleep(wait)
        logger.error(f"SDK init failed after 4 attempts: {last_err}")
        self.live_mode = False
        return False

    # ── Spot asset index ──────────────────────────────────────────────────────
    def _get_spot_asset_index(self, symbol):
        return self.client.sym_to_index(symbol)

    # ── Live order execution (BLOCKING — always called via executor) ──────────
    def _live_buy(self, symbol, usdt_amount, price):
        try:
            idx = self._get_spot_asset_index(symbol)
            if idx is None: logger.error(f"{symbol} not found"); return None
            coin_str = f"@{10000 + idx}"
            size     = round(usdt_amount / price, 6)
            limit_px = round(price * 1.02, 6)
            result   = self.exchange.order(coin_str, is_buy=True, sz=size, limit_px=limit_px,
                                           order_type={"limit": {"tif": "Ioc"}}, reduce_only=False)
            logger.info(f"LIVE BUY {symbol}: {result}")
            if result.get('status') == 'ok':
                fills     = result.get('response',{}).get('data',{}).get('statuses',[{}])
                fill      = fills[0] if fills else {}
                # IOC unfilled: HL returns {'error': 'Order has no fills'} or empty filled
                if fill.get('error') or not fill.get('filled'):
                    logger.warning(f"BUY {symbol} IOC not filled: {fill}")
                    self._push_event('warn', f"BUY {symbol} IOC not filled — no execution", {'symbol': symbol})
                    return None
                filled_px = float(fill['filled'].get('avgPx', price))
                return {'success': True, 'price': filled_px, 'size': size, 'raw': result}
            return None
        except Exception as e: logger.error(f"Live buy error {symbol}: {e}"); return None

    def _live_sell(self, symbol, amount, price):
        try:
            idx = self._get_spot_asset_index(symbol)
            if idx is None: return None
            coin_str = f"@{10000 + idx}"
            limit_px = round(price * 0.98, 6)
            result   = self.exchange.order(coin_str, is_buy=False, sz=round(amount,6),
                                           limit_px=limit_px,
                                           order_type={"limit": {"tif": "Ioc"}}, reduce_only=False)
            logger.info(f"LIVE SELL {symbol}: {result}")
            if result.get('status') == 'ok':
                fills     = result.get('response',{}).get('data',{}).get('statuses',[{}])
                fill      = fills[0] if fills else {}
                # IOC unfilled: HL returns {'error': 'Order has no fills'} or empty filled
                if fill.get('error') or not fill.get('filled'):
                    logger.warning(f"SELL {symbol} IOC not filled: {fill}")
                    self._push_event('warn', f"SELL {symbol} IOC not filled — no execution", {'symbol': symbol})
                    return None
                filled_px = float(fill['filled'].get('avgPx', price))
                return {'success': True, 'price': filled_px, 'raw': result}
            return None
        except Exception as e: logger.error(f"Live sell error {symbol}: {e}"); return None

    # ── Coin management ───────────────────────────────────────────────────────
    def add_coin(self, symbol, capital, timeframe='auto', stop_loss=1.5, trailing_stop=1.0, take_profit=2.0):
        symbol = symbol.upper().strip()
        # Validate coin exists on Hyperliquid spot — also handles aliases (ETH→UETH etc)
        idx = self.client.sym_to_index(symbol)
        if idx is None:
            return {'success': False, 'error': f'{symbol} not found on Hyperliquid spot market'}
        # Derive the clean display name for the card (ETH not UETH, SOL not USOL)
        import re as _re
        def _display(s):
            _special = {'HPUMP':'PUMPFUN','HPENGU':'PENGU','HPEPE':'PEPE','FXRP':'XRP'}
            if s in _special: return _special[s]
            # Resolve alias → internal → display
            internal = self.client._sym_index.get(s)  # index value
            # Find internal name for this index
            for k, v in self.client._sym_index.items():
                if v == internal and not k.endswith('/USDC') and 'USDC' not in k:
                    s2 = k
                    if s2.startswith('U') and len(s2) > 1 and s2[1:].isalpha(): return s2[1:]
                    if _re.match(r'^[A-Z]+\d$', s2): return s2[:-1]
                    if s2 in _special: return _special[s2]
            return symbol  # fallback: keep as-is
        display_name = _display(symbol)
        self.coins[symbol] = {
            'symbol': symbol, 'display_name': display_name,
            'capital': capital,           # initial capital set by user
            'compound_capital': capital,  # grows with profit — this is what bot uses for trades
            'timeframe': timeframe,
            'stop_loss': stop_loss, 'trailing_stop': trailing_stop,
            'take_profit': take_profit,
            'enabled': True, 'added_at': now_ist()
        }
        self._save_data()
        self._async_eng.trigger_cache_refresh([symbol])
        return {'success': True, 'coin': self.coins[symbol]}

    def remove_coin(self, symbol):
        if symbol in self.coins:
            del self.coins[symbol]; self._save_data(); return {'success': True}
        return {'success': False, 'error': 'Coin not found'}

    def update_coin(self, symbol, data):
        if symbol not in self.coins: return {'success': False, 'error': 'Not found'}
        for k in ['capital', 'timeframe', 'stop_loss', 'trailing_stop', 'take_profit', 'enabled']:
            if k in data: self.coins[symbol][k] = data[k]
        self._save_data()
        return {'success': True, 'coin': self.coins[symbol]}

    def get_coins(self):
        """Parallel market data fetch using ThreadPoolExecutor."""
        syms = list(self.coins.keys())
        if not syms: return []

        # Pre-warm markPx cache ONCE before parallel fetch — prevents race condition
        # where multiple threads trigger _refresh_markpx simultaneously and get
        # different price snapshots (causing paired tokens to "fluctuate" together)
        self.client._refresh_markpx()

        def _fetch(sym):
            try:
                market  = self.get_market_data(sym)
                cfg     = self.coins.get(sym, {})
                holding = self.holdings.get(sym)
                pnl = avg_entry = total_held = 0.0
                if holding and holding.get('entries'):
                    entries    = holding['entries']
                    total_usdt = sum(e['usdt'] for e in entries)
                    total_amt  = sum(e['amount'] for e in entries)
                    avg_entry  = total_usdt / total_amt if total_amt else 0
                    total_held = total_amt
                    cp         = market.get('price', 0)
                    if cp and avg_entry:
                        pnl = ((cp - avg_entry) / avg_entry) * 100
                return {**cfg,
                        'price': market.get('price', 0), 'rsi': market.get('rsi', 50),
                        'macd_signal': market.get('macd_signal','neutral'),
                        'volume_signal': bool(market.get('volume_signal', False)),
                        'signal': market.get('signal','neutral'),
                        'mtf_score': market.get('mtf_score', 0),
                        'best_timeframe': market.get('best_timeframe','N/A'),
                        'confidence': market.get('confidence','low'),
                        'holding': total_held, 'avg_entry': avg_entry,
                        'pnl_pct': round(pnl, 2),
                        'peak_price': holding.get('peak_price',0) if holding else 0}
            except Exception as e:
                logger.error(f"get_coins {sym}: {e}")
                return {**self.coins.get(sym,{}),
                        'price':0,'rsi':50,'macd_signal':'neutral','volume_signal':False,
                        'signal':'neutral','mtf_score':0,'best_timeframe':'N/A',
                        'confidence':'low','holding':0,'avg_entry':0,'pnl_pct':0,'peak_price':0}

        with ThreadPoolExecutor(max_workers=min(len(syms), 20)) as ex:
            results = list(ex.map(_fetch, syms))
        # Filter out None results to ensure all coins always show in monitor
        return [r for r in results if r is not None]

    def get_holdings(self):
        result = []
        for sym, h in self.holdings.items():
            if not h.get('entries'): continue
            entries    = h['entries']
            total_usdt = sum(e['usdt'] for e in entries)
            total_amt  = sum(e['amount'] for e in entries)
            avg_entry  = total_usdt / total_amt if total_amt else 0
            cur_price  = self.client.get_spot_price(sym) or 0
            pnl        = ((cur_price - avg_entry) / avg_entry * 100) if avg_entry else 0
            result.append({'symbol': sym, 'amount': total_amt, 'avg_entry': avg_entry,
                           'current_price': cur_price, 'pnl_pct': round(pnl,2),
                           'pnl_usdt': round((cur_price-avg_entry)*total_amt, 4),
                           'invested_usdt': total_usdt, 'current_value': cur_price*total_amt,
                           'dca_count': len(entries),
                           'peak_price': h.get('peak_price',0),
                           'trailing_stop_price': h.get('trailing_stop_price',0)})
        return result

    def get_trade_history(self): return list(reversed(self.trades[-5000:]))

    def get_stats(self):
        wr = round(self.stats['winning_trades']/self.stats['total_trades']*100,1) \
             if self.stats['total_trades'] else 0
        return {**self.stats, 'win_rate': wr}

    def get_status(self):
        wallet = self.wallet or os.environ.get('WALLET_ADDRESS','')
        return {'running': self.running, 'live_mode': self.live_mode,
                'sdk_available': SDK_AVAILABLE,
                'user_stopped':      self._user_stopped,
                'persisted_running': self._persisted_running,
                'coins_monitored': len(self.coins),
                'active_holdings': len([h for h in self.holdings.values() if h.get('entries')]),
                'wallet_configured': bool(wallet),
                'wallet_masked': (wallet[:6]+'...'+wallet[-4:]) if len(wallet)>10 else wallet,
                'key_configured': bool(self.private_key or os.environ.get('PRIVATE_KEY','')),
                'ws_price_age_s': round(price_cache.age(), 1),
                'cache_keys': sum(1 for sym in self.coins for tf in SCAN_TIMEFRAMES
                                   if candle_cache.get(sym, tf) is not None)}

    # ── Indicators ────────────────────────────────────────────────────────────
    def _calc_rsi(self, closes, period=14):
        """Wilder's smoothed RSI — correct implementation."""
        if len(closes) < period + 1: return 50.0
        deltas = np.diff(closes)
        gains  = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        # Seed with simple average of first `period` bars
        avg_gain = float(np.mean(gains[:period]))
        avg_loss = float(np.mean(losses[:period]))
        # Wilder's smoothing for remaining bars
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0: return 100.0
        return round(100.0 - (100.0 / (1.0 + avg_gain / avg_loss)), 2)

    def _calc_macd(self, closes, fast=12, slow=26, signal=9):
        if len(closes) < slow+signal: return 0, 0, 'neutral'
        def ema(data, n):
            k=2/(n+1); r=[data[0]]
            for p in data[1:]: r.append(p*k+r[-1]*(1-k))
            return r
        ema_f   = ema(closes, fast)
        ema_s   = ema(closes, slow)
        macd_ln = [f-s for f,s in zip(ema_f, ema_s)]
        sig_ln  = ema(macd_ln, signal)
        hist    = [m-s for m,s in zip(macd_ln, sig_ln)]
        if len(hist) >= 2:
            if hist[-1]>0 and hist[-1]>hist[-2]: return macd_ln[-1], sig_ln[-1], 'bullish'
            if hist[-1]<0 and hist[-1]<hist[-2]: return macd_ln[-1], sig_ln[-1], 'bearish'
        return macd_ln[-1], sig_ln[-1], 'neutral'

    def _calc_volume_signal(self, volumes, multiplier=1.5):
        if len(volumes) < 20: return False
        return bool(volumes[-1] > np.mean(volumes[-20:-1]) * multiplier)

    def _calc_atr(self, candles, period=14):
        """Average True Range for dynamic stop loss."""
        if len(candles) < period + 1: return 0.0
        trs = []
        for i in range(1, len(candles)):
            high  = float(candles[i][2])
            low   = float(candles[i][3])
            prev_close = float(candles[i-1][4])
            trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        if not trs: return 0.0
        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        return round(atr, 8)

    def _signal_for_candles(self, candles):
        """
        Simple strategy: MACD signal line cross + RSI filter + volume confirmation
        BUY:  MACD line crosses above signal line + RSI 30-65 + volume above average
        SELL: MACD line crosses below signal line + RSI above 55
        """
        if not candles or len(candles) < 35:
            return 'neutral', 50.0, 'neutral', False, 0.0

        closes  = [float(c[4]) for c in candles]
        volumes = [float(c[5]) for c in candles]

        rsi_now = self._calc_rsi(closes)
        atr     = self._calc_atr(candles)
        vol_sig = self._calc_volume_signal(volumes)

        def ema(data, n):
            k = 2/(n+1); r = [data[0]]
            for p in data[1:]: r.append(p*k + r[-1]*(1-k))
            return r

        ema_f   = ema(closes, 12)
        ema_s   = ema(closes, 26)
        macd_ln = [f - s for f, s in zip(ema_f, ema_s)]
        sig_ln  = ema(macd_ln, 9)

        if len(macd_ln) < 2 or len(sig_ln) < 2:
            return 'neutral', rsi_now, 'neutral', vol_sig, atr

        macd_bull_cross = macd_ln[-2] <= sig_ln[-2] and macd_ln[-1] > sig_ln[-1]
        macd_bear_cross = macd_ln[-2] >= sig_ln[-2] and macd_ln[-1] < sig_ln[-1]
        hist_now        = macd_ln[-1] - sig_ln[-1]
        macd_sig_str    = 'bullish' if hist_now > 0 else ('bearish' if hist_now < 0 else 'neutral')

        # BUY: MACD cross + RSI in range + volume confirms momentum
        if macd_bull_cross and 30 <= rsi_now <= 65 and vol_sig:
            return 'buy', rsi_now, macd_sig_str, vol_sig, atr

        # SELL: MACD cross + RSI elevated (no volume requirement for exits)
        if macd_bear_cross and rsi_now > 55:
            return 'sell', rsi_now, macd_sig_str, vol_sig, atr

        return 'neutral', rsi_now, macd_sig_str, vol_sig, atr

    def _mtf_scan(self, symbol: str) -> dict:
        tf_results  = {}
        best_tf     = None
        best_atr    = 0.0

        for tf in SCAN_TIMEFRAMES:
            candles = candle_cache.get(symbol, tf)
            if not candles:
                tf_results[tf] = {'signal': 'neutral', 'score': 0, 'rsi': 50.0, 'macd': 'neutral', 'vol': False, 'atr': 0.0}
                continue
            signal, rsi, macd, vol, atr = self._signal_for_candles(candles)
            score = SIGNAL_SCORES.get(signal, 0)
            tf_results[tf] = {'signal': signal, 'score': score, 'rsi': rsi, 'macd': macd, 'vol': vol, 'atr': atr}

        # First TF with a non-neutral signal wins — no conflict blocking
        direction  = 'neutral'
        confidence = 'low'
        for tf in SCAN_TIMEFRAMES:
            sig = tf_results[tf]['signal']
            if sig in ('buy', 'sell'):
                direction  = sig
                confidence = 'high'
                best_tf    = tf
                best_atr   = tf_results[tf]['atr']
                break

        if best_tf is None:
            best_tf  = '1h'
            best_atr = tf_results.get('1h', {}).get('atr', 0.0)

        best        = tf_results[best_tf]
        total_score = best['score']
        capital_pct = 1.0 if direction == 'buy' else 0.0

        return {'total_score': total_score, 'confidence': confidence,
                'direction': direction, 'best_timeframe': best_tf,
                'capital_pct': capital_pct, 'atr': best_atr,
                'tf_breakdown': {tf: v['signal'] for tf, v in tf_results.items()},
                'tf_results': tf_results,
                'rsi': best['rsi'], 'macd_signal': best['macd'], 'volume_signal': best['vol'],
                'signal': direction if direction != 'neutral' else 'neutral'}

    # ── Market data (for REST API) ────────────────────────────────────────────
    def get_market_data(self, symbol):
        try:
            mtf   = self._mtf_scan(symbol)
            price = self.client.get_spot_price(symbol) or 0
            if not price:
                candles = candle_cache.get(symbol,'1h') or self.client.get_candles(symbol,'1h',5)
                if candles: price = float(candles[-1][4])
            return {'price': price, 'rsi': mtf['rsi'], 'macd_signal': mtf['macd_signal'],
                    'volume_signal': mtf['volume_signal'], 'signal': mtf['signal'],
                    'mtf_score': mtf['total_score'], 'best_timeframe': mtf['best_timeframe'],
                    'confidence': mtf['confidence'], 'capital_pct': mtf['capital_pct'],
                    'tf_breakdown': mtf['tf_breakdown']}
        except Exception as e:
            logger.error(f"Market data error {symbol}: {e}")
            return {'price':0,'rsi':50,'macd_signal':'neutral','volume_signal':False,
                    'signal':'neutral','mtf_score':0,'best_timeframe':'N/A',
                    'confidence':'low','capital_pct':0.0,'tf_breakdown':{}}

    # ── Execute buy ───────────────────────────────────────────────────────────
    def _execute_buy(self, symbol, capital, price, reason):
        if self.live_mode:
            # Cache balance for 30s to avoid REST call on every buy attempt
            if time.time() - self._balance_ts > 30:
                self._balance_cache = self.client.get_spot_balance()
                self._balance_ts = time.time()
            usdc_bal = self._balance_cache.get('USDC', {}).get('available', 0)
            if usdc_bal < capital:
                msg = f"Insufficient balance — need {capital:.2f} USDC, have {usdc_bal:.2f}"
                logger.warning(f"[{symbol}] {msg}")
                self._push_event('warn', msg, {'symbol': symbol})
                return None

        actual_price = price; order_id = None; mode_tag = 'SIM'
        if self.live_mode:
            result = self._live_buy(symbol, capital, price)
            if result:
                actual_price = result['price']
                order_id     = str(result.get('raw',{}).get('response',{}).get('data',{}).get('statuses',[{}])[0].get('resting',{}).get('oid',''))
                mode_tag     = 'LIVE'
            else:
                self._push_event('error', f"Live buy failed {symbol}", {'symbol':symbol,'price':price})
                return None

        amount = capital / actual_price
        # Extract TF from reason string e.g. 'signal_buy_4h' → '4h'
        trade_tf = reason.split('_')[-1] if '_' in reason else '1h'
        trade  = {'type':'BUY','symbol':symbol,'price':actual_price,'amount':amount,
                  'usdt':capital,'reason':reason,'mode':mode_tag,'order_id':order_id,
                  'signal_price': price,       # price when signal fired
                  'buy_price':    actual_price, # actual fill price
                  'time':now_ist(),'pnl':None}
        if symbol not in self.holdings:
            self.holdings[symbol] = {'entries':[],'peak_price':actual_price,'trailing_stop_price':0,'trade_tf':trade_tf}
        self.holdings[symbol]['entries'].append(
            {'price':actual_price,'amount':amount,'usdt':capital,'time':trade['time']})
        self.holdings[symbol]['peak_price'] = max(self.holdings[symbol].get('peak_price',actual_price), actual_price)
        self.holdings[symbol]['trade_tf']   = trade_tf  # always update to latest buy TF
        trail_pct = self.coins.get(symbol,{}).get('trailing_stop',1.0)/100
        self.holdings[symbol]['trailing_stop_price'] = actual_price*(1-trail_pct)
        self.trades.append(trade)
        self._push_event('buy', f"[{mode_tag}] BUY {symbol} @ {actual_price:.6f} — {reason}",
                         {'symbol':symbol,'price':actual_price,'usdt':capital,'reason':reason})
        logger.info(f"[{mode_tag}] BUY {symbol} @ {actual_price:.6f} | {capital:.2f} USDC | {reason}")
        self._balance_ts = 0  # invalidate balance cache after buy
        self._save_data()
        return trade

    # ── Execute sell ──────────────────────────────────────────────────────────
    def _execute_sell(self, symbol, price, reason):
        if symbol not in self.holdings or not self.holdings[symbol].get('entries'):
            return None
        entries    = self.holdings[symbol]['entries']
        total_usdt = sum(e['usdt'] for e in entries)
        total_amt  = sum(e['amount'] for e in entries)
        avg_entry  = total_usdt/total_amt if total_amt else price
        actual_price = price; order_id = None; mode_tag = 'SIM'

        if self.live_mode:
            result = self._live_sell(symbol, total_amt, price)
            if result:
                actual_price = result['price']
                order_id     = str(result.get('raw',{}).get('response',{}).get('data',{}).get('statuses',[{}])[0].get('resting',{}).get('oid',''))
                mode_tag     = 'LIVE'
            else:
                self._push_event('error', f"Live sell failed {symbol}", {'symbol':symbol})
                return None

        pnl_usdt = (actual_price-avg_entry)*total_amt
        pnl_pct  = (actual_price-avg_entry)/avg_entry*100
        # Find the matching buy trade to pull signal_price and buy_price
        buy_signal_price = None; buy_fill_price = None
        for tr in reversed(self.trades):
            if tr.get('type') == 'BUY' and tr.get('symbol') == symbol:
                buy_signal_price = tr.get('signal_price')
                buy_fill_price   = tr.get('buy_price', tr.get('price'))
                break
        trade    = {'type':'SELL','symbol':symbol,'price':actual_price,'amount':total_amt,
                    'usdt':actual_price*total_amt,'avg_entry':avg_entry,
                    'pnl_usdt':round(pnl_usdt,4),'pnl_pct':round(pnl_pct,2),
                    'reason':reason,'mode':mode_tag,'order_id':order_id,
                    'buy_signal_price':  buy_signal_price,
                    'buy_price':         buy_fill_price,
                    'sell_signal_price': price,        # price when sell signal fired
                    'sell_price':        actual_price, # actual fill price
                    'dca_count':len(entries),'time':now_ist()}
        self.stats['total_trades'] += 1
        self.stats['total_profit']  += pnl_usdt
        if pnl_usdt > 0: self.stats['winning_trades'] += 1
        # ── Compound profit into this coin's capital pool ──────────────────────
        if symbol in self.coins:
            old_cap = self.coins[symbol].get('compound_capital', self.coins[symbol].get('capital', 0))
            new_cap = round(old_cap + pnl_usdt, 4)
            # Never let compound_capital go below 10% of initial capital (safety floor)
            initial = self.coins[symbol].get('capital', old_cap)
            floor   = round(initial * 0.1, 4)
            self.coins[symbol]['compound_capital'] = max(new_cap, floor)
            logger.info(f"💰 Compound [{symbol}]: {old_cap:.4f} → {self.coins[symbol]['compound_capital']:.4f} USDC (pnl: {pnl_usdt:+.4f})")
        self.holdings[symbol] = {'entries':[],'peak_price':0,'trailing_stop_price':0}
        self.trades.append(trade)
        self._push_event('sell',
            f"[{mode_tag}] SELL {symbol} @ {actual_price:.6f} | PnL: {pnl_pct:.2f}% ({pnl_usdt:.4f} USDC)",
            {'symbol':symbol,'price':actual_price,'pnl_usdt':round(pnl_usdt,4),'pnl_pct':round(pnl_pct,2),'reason':reason})
        logger.info(f"[{mode_tag}] SELL {symbol} @ {actual_price:.6f} | PnL: {pnl_pct:.2f}% | {reason}")
        self._save_data()
        return trade

    def manual_sell(self, symbol, amount=None):
        price = self.client.get_spot_price(symbol) or 0
        if not price: return {'success':False,'error':'Could not fetch price'}
        trade = self._execute_sell(symbol, price, 'manual')
        return {'success':True,'trade':trade} if trade else {'success':False,'error':'No holdings or sell failed'}

    # ── Stop/trailing checks ──────────────────────────────────────────────────
    def _check_trailing_stop(self, symbol, current_price):
        h = self.holdings.get(symbol)
        if not h or not h.get('entries'): return False
        trail_pct = self.coins.get(symbol,{}).get('trailing_stop',1.0)/100
        if current_price > h.get('peak_price',0):
            h['peak_price']          = current_price
            h['trailing_stop_price'] = current_price*(1-trail_pct)
        return current_price <= h.get('trailing_stop_price',0) and h['trailing_stop_price'] > 0

    def _check_stop_loss(self, symbol, current_price):
        h = self.holdings.get(symbol)
        if not h or not h.get('entries'): return False
        sl_pct    = self.coins.get(symbol,{}).get('stop_loss',1.5)/100
        entries   = h['entries']
        avg_entry = sum(e['usdt'] for e in entries)/sum(e['amount'] for e in entries)
        return current_price <= avg_entry*(1-sl_pct)

    # ── Start / Stop ──────────────────────────────────────────────────────────
    def start(self, wallet=None, private_key=None):
        if self.running: return {'success':False,'error':'Already running'}
        wallet      = wallet      or os.environ.get('WALLET_ADDRESS','')
        private_key = private_key or os.environ.get('PRIVATE_KEY','')
        self.wallet = wallet; self.private_key = private_key
        live = self._init_sdk(wallet, private_key)
        if not live:
            return {'success': False, 'error': 'Live trading init failed — check WALLET_ADDRESS and PRIVATE_KEY'}
        mode = 'LIVE'
        self.running = True
        self._user_stopped = False
        self.stats['start_time'] = now_ist()
        self._save_data()
        self._async_eng.start()
        coins = list(self.coins.keys())
        if coins:
            self._async_eng.trigger_cache_refresh(coins)
        self._push_event('monitor', f"Bot started in LIVE mode (PARALLEL ENGINE)", {'mode': mode})
        return {'success': True, 'message': 'Bot started (LIVE + parallel engine)', 'mode': mode}

    def stop(self):
        self.running = False; self.live_mode = False; self.exchange = None
        self._user_stopped = True
        self._balance_ts = 0  # invalidate balance cache
        self._async_eng._thread = None   # allow fresh start on next start()
        self._save_data()
        self._push_event('monitor','Bot stopped',{})
        return {'success':True,'message':'Bot stopped'}


# ═════════════════════════════════════════════════════════════════════════════
# Flask App
# ═════════════════════════════════════════════════════════════════════════════
app = Flask(__name__)
if CORS_AVAILABLE: CORS(app)

client     = HyperliquidClient()
bot_engine = BotEngine(client)

# ── Auto-restart bot if it was running before deploy/restart ─────────────────
def _auto_start():
    """If bot was running before this deploy, restart it automatically."""
    if bot_engine._persisted_running and not bot_engine._user_stopped:
        logger.info("🔄 Auto-restarting bot (was running before deploy)...")
        result = bot_engine.start()
        logger.info(f"🔄 Auto-start result: {result}")
    else:
        logger.info(f"ℹ️ Auto-start skipped — persisted_running={bot_engine._persisted_running}, user_stopped={bot_engine._user_stopped}")

_auto_start_thread = threading.Thread(target=_auto_start, daemon=True, name="AutoStart")
_auto_start_thread.start()

@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/coins', methods=['GET'])
def get_coins(): return jsonify(bot_engine.get_coins())

@app.route('/api/coins', methods=['POST'])
def add_coin():
    d             = request.json or {}
    symbol        = d.get('symbol','').upper().strip()
    capital       = float(d.get('capital', 10))
    timeframe     = d.get('timeframe','auto')
    stop_loss     = float(d.get('stop_loss', 1.5))
    trailing_stop = float(d.get('trailing_stop', 1.0))
    take_profit   = float(d.get('take_profit', 2.0))
    return jsonify(bot_engine.add_coin(symbol, capital, timeframe, stop_loss, trailing_stop, take_profit))

@app.route('/api/coins/<symbol>', methods=['DELETE'])
def remove_coin(symbol): return jsonify(bot_engine.remove_coin(symbol.upper()))

@app.route('/api/coins/<symbol>', methods=['PUT'])
def update_coin(symbol): return jsonify(bot_engine.update_coin(symbol.upper(), request.json))

@app.route('/api/market/<symbol>', methods=['GET'])
def get_market_data(symbol): return jsonify(bot_engine.get_market_data(symbol.upper()))

@app.route('/api/balance', methods=['GET'])
def get_balance():
    wallet = request.args.get('wallet','') or None
    return jsonify(client.get_spot_balance(wallet))

@app.route('/api/holdings', methods=['GET'])
def get_holdings(): return jsonify(bot_engine.get_holdings())

@app.route('/api/trades', methods=['GET'])
def get_trades(): return jsonify(bot_engine.get_trade_history())

@app.route('/api/stats', methods=['GET'])
def get_stats(): return jsonify(bot_engine.get_stats())

@app.route('/api/bot/start', methods=['POST'])
def start_bot():
    d = request.json or {}
    return jsonify(bot_engine.start(d.get('wallet') or None, d.get('private_key') or None))

@app.route('/api/bot/stop', methods=['POST'])
def stop_bot(): return jsonify(bot_engine.stop())

@app.route('/api/bot/status', methods=['GET'])
def bot_status(): return jsonify(bot_engine.get_status())

@app.route('/api/events', methods=['GET'])
def get_events():
    events      = bot_engine.get_events()
    filter_type = request.args.get('type','').lower()
    if filter_type: events = [e for e in events if e.get('type')==filter_type]
    return jsonify(events)

@app.route('/api/sell/<symbol>', methods=['POST'])
def manual_sell(symbol):
    d = request.json or {}
    return jsonify(bot_engine.manual_sell(symbol.upper(), d.get('amount')))

@app.route('/api/spot/pairs', methods=['GET'])
def get_spot_pairs(): return jsonify(client.get_spot_pairs())

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
