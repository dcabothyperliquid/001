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

import os
import psutil, json, time, threading, logging, asyncio, aiohttp, websockets, contextlib

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
SCAN_TIMEFRAMES = ['5m', '15m', '30m', '1h', '2h', '4h']
ALL_TIMEFRAMES  = ['5m','15m','30m','1h','2h','4h']
_enabled_tfs      = set(SCAN_TIMEFRAMES)   # mutable — UI se toggle hoga
_enabled_tfs_lock = threading.Lock()

def get_active_tfs():
    with _enabled_tfs_lock:
        return [tf for tf in ALL_TIMEFRAMES if tf in _enabled_tfs]

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

# ── Shared math helpers (EMA / RSI / MACD-cross / RSI-series) ─────────────────
def _ema_fn(data, n):
    k = 2/(n+1); r = [data[0]]
    for p in data[1:]: r.append(p*k + r[-1]*(1-k))
    return r

def _rsi_fn(closes, period=14):
    if len(closes) < period+1: return 50.0
    arr = np.array(closes, dtype=float)
    d = np.diff(arr)
    g = np.where(d>0, d, 0.0); l = np.where(d<0, -d, 0.0)
    ag = float(np.mean(g[:period])); al = float(np.mean(l[:period]))
    for i in range(period, len(g)):
        ag = float((ag*(period-1)+g[i])/period); al = float((al*(period-1)+l[i])/period)
    return float(round(100.0-(100.0/(1.0+ag/al)), 1)) if al > 0 else 100.0

def _rsi_series_fn(closes, period=14):
    """Rolling RSI value at every point (same length as closes, padded with 50.0)."""
    n = len(closes)
    out = [50.0] * n
    if n < period+1: return out
    arr = np.array(closes, dtype=float)
    d = np.diff(arr)
    g = np.where(d>0, d, 0.0); l = np.where(d<0, -d, 0.0)
    ag = float(np.mean(g[:period])); al = float(np.mean(l[:period]))
    rsi = 100.0 - (100.0/(1.0+ag/al)) if al > 0 else 100.0
    out[period] = round(rsi, 1)
    for i in range(period, len(g)):
        ag = float((ag*(period-1)+g[i])/period); al = float((al*(period-1)+l[i])/period)
        rsi = 100.0 - (100.0/(1.0+ag/al)) if al > 0 else 100.0
        out[i+1] = round(rsi, 1)
    return out

def _macd_cross_markers(closes, timestamps):
    """Return list of {time, type:'bull'|'bear'} where MACD crosses signal line."""
    if len(closes) < 35: return []
    ema_f   = _ema_fn(closes, 12)
    ema_s   = _ema_fn(closes, 26)
    macd_ln = [f - s for f, s in zip(ema_f, ema_s)]
    sig_ln  = _ema_fn(macd_ln, 9)
    out = []
    for i in range(1, len(macd_ln)):
        if macd_ln[i-1] <= sig_ln[i-1] and macd_ln[i] > sig_ln[i]:
            out.append({'time': int(timestamps[i] / 1000), 'type': 'bull'})
        elif macd_ln[i-1] >= sig_ln[i-1] and macd_ln[i] < sig_ln[i]:
            out.append({'time': int(timestamps[i] / 1000), 'type': 'bear'})
    return out

def _build_chart_payload(raw, tail_n=80, support_lookback=20):
    """Build full chart payload (candles+volume, EMA9/21, RSI series, support level, MACD cross markers)
    from raw candle data [[ts,o,h,l,c,v], ...]. Returns None if not enough data."""
    if len(raw) < 10:
        return None
    closes    = [float(c[4]) for c in raw]
    ema9_all  = _ema_fn(closes, 9)
    ema21_all = _ema_fn(closes, 21)
    rsi_series_all = _rsi_series_fn(closes)
    rsi_val   = rsi_series_all[-1]
    tail      = raw[-tail_n:]
    n         = len(tail)
    ema9_t    = ema9_all[-n:]
    ema21_t   = ema21_all[-n:]
    rsi_t     = rsi_series_all[-n:]
    timestamps = [int(c[0]) for c in tail]
    tail_closes = closes[-n:]
    support = min(float(c[3]) for c in raw[-support_lookback:]) if len(raw) >= 5 else None
    return {
        'candles':    [[int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])] for c in tail],
        'volumes':    [[int(c[0]), float(c[5]) if len(c) > 5 else 0.0, float(c[4]) >= float(c[1])] for c in tail],
        'ema9':       [round(v, 6) for v in ema9_t],
        'ema21':      [round(v, 6) for v in ema21_t],
        'rsi':        rsi_val,
        'rsi_series': rsi_t,
        'ema_bull':   bool(ema9_all[-1] > ema21_all[-1]),
        'rsi_ok':     bool(35 <= rsi_val <= 68),
        'support':    round(support, 6) if support is not None else None,
        'macd_cross': _macd_cross_markers(tail_closes, timestamps),
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
        self._markpx_lock = __import__('threading').Lock()
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
            for p in data.get('universe', []):
                uni_index = p.get('index')  # .index field = actual HL universe ID, NOT array position
                if uni_index is None:
                    continue
                tok_idxs = p.get('tokens', [])
                uni_name = p.get('name', '').strip().upper()
                base_name = ''
                if tok_idxs:
                    base_name = token_idx_to_name.get(tok_idxs[0], '')
                if not base_name and uni_name:
                    base_name = uni_name.split('/')[0].strip()
                if base_name:
                    idx_map[base_name] = uni_index
                    idx_map[base_name + '/USDC'] = uni_index
                    idx_map[base_name + 'USDC'] = uni_index
            # ── Alias map: user types "SOL" → HL actual name "USOL" ──────────
            # IMPORTANT: These aliases MUST override any native token with same name
            # e.g. HL may have a native "ETH" token at a different index than "UETH"
            # We always want ETH→UETH, SOL→USOL, BTC→UBTC (the wrapped versions)
            ALIASES = {
                'BTC': 'UBTC', 'SOL': 'USOL', 'ETH': 'UETH',
                'AVAX': 'AVAX0',
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
            'AVAX0':'AVAX','LINK0':'LINK',
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
        """Spot-only price cache using spotMetaAndAssetCtxs endpoint.

        Official HL docs (asset-ids page) confirm:
          - HYPE token ID = 150, HYPE spot/pair ID = 107
          - asset_ctxs[].coin uses UNIVERSE PAIR index (@107 for HYPE), NOT token index (150)
          - Spot asset ID = 10000 + universe[].index  (the @N number)

        Correct map: universe_pair_index -> token_name
        Built from universe[].index (pair_idx) + universe[].tokens[0] -> tokens[].name
        """
        if time.time() - self._markpx_ts < 15:
            return
        if not self._markpx_lock.acquire(blocking=False):
            return  # another thread is already fetching
        try:
            data = self._post({"type": "spotMetaAndAssetCtxs"})
            if not data or not isinstance(data, list) or len(data) < 2:
                return
            meta       = data[0] if isinstance(data[0], dict) else {}
            asset_ctxs = data[1] if isinstance(data[1], list) else []

            # Step 1: token_index -> token_name  (from tokens[])
            tok_idx_to_name: dict = {}
            for t in meta.get('tokens', []):
                tidx  = t.get('index')
                tname = t.get('name', '').strip().upper()
                if tidx is not None and tname and tname != 'USDC':
                    tok_idx_to_name[tidx] = tname

            # Step 2: universe_pair_index -> token_name  (from universe[])
            # asset_ctxs[].coin = "@{universe[].index}" i.e. the PAIR index
            # universe[].tokens[0] = base token token_index -> resolve name
            uni_pair_idx_to_name: dict = {}
            for u in meta.get('universe', []):
                pair_idx = u.get('index')       # e.g. 107 for HYPE pair
                tok_idxs = u.get('tokens', [])  # e.g. [150, 0] for HYPE/USDC
                uni_name = u.get('name', '').strip().upper()
                if pair_idx is None:
                    continue
                base = ''
                if tok_idxs:
                    base = tok_idx_to_name.get(tok_idxs[0], '')
                if not base and '/' in uni_name:
                    base = uni_name.split('/')[0].strip()
                if base and base != 'USDC':
                    uni_pair_idx_to_name[pair_idx] = base  # 107 -> "HYPE"

            # ONE-TIME raw debug — print first 5 assetCtxs entries + idx 234/235
            if not getattr(self, '_raw_debug_done', False):
                self._raw_debug_done = True
                logger.info(f"[RAW_DEBUG] uni_pair_idx_to_name sample: { {k:v for k,v in list(uni_pair_idx_to_name.items())[:10]} }")
                logger.info(f"[RAW_DEBUG] BTC/ETH in map: BTC={uni_pair_idx_to_name.get(234)}, ETH={uni_pair_idx_to_name.get(235)}")
                logger.info(f"[RAW_DEBUG] first 5 asset_ctxs: {asset_ctxs[:5]}")
                # Also find entries with coin @234 or @235
                for ctx2 in asset_ctxs:
                    c2 = ctx2.get('coin','')
                    if c2 in ['@234','@235','BTC/USDC','ETH/USDC','UBTC/USDC','UETH/USDC']:
                        logger.info(f"[RAW_DEBUG] FOUND coin={c2} entry={ctx2}")

            cache = {}
            for ctx in asset_ctxs:
                coin   = ctx.get('coin', '')
                _mpx = ctx.get('markPx')
                _mipx = ctx.get('midPx')
                # markPx is "0" (string zero) for BTC/ETH spot — must check float value
                try:
                    px_str = _mpx if _mpx and float(_mpx) > 0 else _mipx
                except (TypeError, ValueError):
                    px_str = _mipx
                if not px_str:
                    continue
                try:
                    fval = float(px_str)
                except:
                    continue
                if fval <= 0:
                    continue

                if coin.startswith('@'):
                    # "@{universe_pair_index}" e.g. "@107" for HYPE/BTC/ETH
                    try:
                        pair_idx = int(coin[1:])
                    except:
                        continue
                    # Always store by @idx key so get_spot_price fallback works
                    cache[coin] = fval  # "@107" -> price (always)
                    token_name = uni_pair_idx_to_name.get(pair_idx)
                    if token_name:
                        cache[token_name] = fval  # "UBTC" -> price
                        # Bidirectional alias
                        ALIASES_FWD = {
                            'UBTC': 'BTC', 'UETH': 'ETH', 'USOL': 'SOL',
                            'UZEC': 'ZEC', 'UWLD': 'WLD', 'UMOG': 'MOG',
                            'UPUMP': 'PUMP', 'AAVE0': 'AAVE', 'AVAX0': 'AVAX',
                            'LINK0': 'LINK', 'FXRP': 'XRP', 'HPENGU': 'PENGU',
                            'HPEPE': 'PEPE', 'HPUMP': 'PUMPFUN', 'XMR1': 'XMR',
                            'TAO1': 'TAO',
                        }
                        plain = ALIASES_FWD.get(token_name)
                        if plain:
                            cache[plain] = fval  # "BTC" -> price
                elif '/' in coin:
                    # "BTC/USDC", "ETH/USDC", "UBTC/USDC", "PURR/USDC" named pair format
                    base = coin.split('/')[0].strip().upper()
                    if base and base != 'USDC':
                        cache[base] = fval
                        # Store both alias directions so all lookups hit
                        SPOT_ALIASES_BIDIR = {
                            'UBTC': 'BTC', 'UETH': 'ETH', 'USOL': 'SOL',
                            'UZEC': 'ZEC', 'UWLD': 'WLD', 'UMOG': 'MOG',
                            'UPUMP': 'PUMP', 'AAVE0': 'AAVE', 'AVAX0': 'AVAX',
                            'LINK0': 'LINK', 'FXRP': 'XRP', 'HPENGU': 'PENGU',
                            'HPEPE': 'PEPE', 'HPUMP': 'PUMPFUN', 'XMR1': 'XMR',
                            'TAO1': 'TAO',
                            # plain -> wrapped (when API sends "BTC/USDC" not "UBTC/USDC")
                            'BTC': 'UBTC', 'ETH': 'UETH', 'SOL': 'USOL',
                            'ZEC': 'UZEC', 'WLD': 'UWLD', 'MOG': 'UMOG',
                            'PUMP': 'UPUMP', 'AAVE': 'AAVE0', 'AVAX': 'AVAX0',
                            'LINK': 'LINK0', 'XRP': 'FXRP',
                        }
                        alias = SPOT_ALIASES_BIDIR.get(base)
                        if alias:
                            cache[alias] = fval

            if cache:
                self._markpx_cache = cache
                self._markpx_ts = time.time()
                if not getattr(self, '_mids_logged', False):
                    self._mids_logged = True
                    for sym in ['USOL','UBTC','UETH','BTC','ETH','AAVE0','HYPE','UZEC']:
                        logger.info(f"  [markpx] {sym} = {cache.get(sym, 'NOT FOUND')}")
            else:
                logger.warning("[markpx] cache empty — spotMetaAndAssetCtxs returned no prices")
        except Exception as e:
            logger.warning(f"_refresh_markpx error: {e}")
        finally:
            try: self._markpx_lock.release()
            except RuntimeError: pass

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
            'BTC':'UBTC','SOL':'USOL','ETH':'UETH','TRX':'TRX1',
            'AVAX':'AVAX0','LINK':'LINK0','AAVE':'AAVE0','XRP':'FXRP',
            'ZEC':'UZEC','WLD':'UWLD','MOG':'UMOG','PUMP':'UPUMP',
            'PENGU':'HPENGU','PEPE':'HPEPE','PUMPFUN':'HPUMP','XMR':'XMR1','TAO':'TAO1',
        }
        internal = ALIASES.get(symbol.upper(), symbol.upper())

        # Inactive spot pairs (BTC/ETH): @idx has stale markPx, plain key has live perp price
        # Detect stale: @idx price == markPx cache price AND plain key differs significantly
        INACTIVE_SPOT = {'BTC', 'ETH', 'UBTC', 'UETH'}

        if symbol.upper() in INACTIVE_SPOT or internal in INACTIVE_SPOT:
            # Use plain perp price from allMids — most accurate for inactive HL spot pairs
            for key in [symbol.upper(), internal.replace('U','',1) if internal.startswith('U') else internal]:
                p = price_cache.get(key)
                if _sane(p): return float(p)

        # 1. WS allMids cache — @idx spot price
        if idx is not None:
            p = price_cache.get(f'@{idx}')
            if _sane(p): return float(p)

        # 1b. WS allMids plain key fallback — try internal alias first, then symbol
        # IMPORTANT: for tokens with active spot markets (ZEC→UZEC, WLD→UWLD etc),
        # plain symbol key may hold a stale perp price — prefer internal alias
        for key in [internal, symbol.upper()]:
            p = price_cache.get(key)
            if _sane(p): return float(p)

        # 2. markPx cache from spotMetaAndAssetCtxs (refreshed every 15s, rate-limited)
        self._refresh_markpx()
        for key in [internal, f'@{idx}' if idx is not None else None]:
            if key:
                p = self._markpx_cache.get(key)
                if _sane(p): return float(p)

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

        # BTC/ETH spot pairs are inactive on HL — use perp coin name for candles
        PERP_CANDLE_COINS = {'BTC', 'ETH', 'UBTC', 'UETH'}
        if symbol.upper() in PERP_CANDLE_COINS:
            coin_id = 'BTC' if symbol.upper() in ('BTC', 'UBTC') else 'ETH'
        else:
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
        # BTC/ETH spot pairs are inactive on HL — use perp coin name for candles
        PERP_CANDLE_COINS = {'BTC', 'ETH', 'UBTC', 'UETH'}
        if symbol.upper() in PERP_CANDLE_COINS:
            coin_id = 'BTC' if symbol.upper() in ('BTC', 'UBTC') else 'ETH'
        else:
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


# ── Per-coin BUY/SELL cycle state ─────────────────────────────────────────────
# Prevents duplicate BUY signals being counted until a SELL resets the cycle.
_coin_signal_state      = {}   # symbol → 'buy' | 'sell' | None
_coin_signal_state_lock = __import__('threading').Lock()

def _signal_state_get(symbol):
    with _coin_signal_state_lock:
        return _coin_signal_state.get(symbol)

def _signal_state_set(symbol, state):
    with _coin_signal_state_lock:
        _coin_signal_state[symbol] = state

# ── Virtual P&L Tracker ───────────────────────────────────────────────────────
# Tracks hypothetical trades based purely on BUY/SELL signals (no real orders).
# BUY signal -> assume bought at that price with current fund allocation.
# SELL signal -> assume sold, calculate P&L, compound fund.
# Fully independent from live/sim trading logic.
_vt_lock    = __import__('threading').Lock()
_vt_fund    = {}   # symbol -> {fund, buy_price, buy_time, amount, timeframe}
_vt_trades  = []   # completed virtual trade pairs
_vt_stats   = {}   # symbol -> {total_trades, wins, total_pnl, fund}
_VT_INITIAL = 10.0 # default starting fund per coin

def _vt_persist():
    """Save virtual tracker state to Supabase so restarts don't lose data."""
    if not SUPABASE_OK: return
    import json as _json
    try:
        state = {
            'vt_stats': _vt_stats,
            'vt_trades': _vt_trades[-200:],  # keep last 200 trades
            'vt_fund': _vt_fund,
            'coin_signal_state': _coin_signal_state,  # persist cycle state too!
        }
        _supabase.table('bot_data').upsert({
            'key': 'virtual_tracker', 'value': _json.dumps(state)
        }).execute()
    except Exception as e:
        print(f"[VT PERSIST ERROR] {e}")

def _vt_load():
    """Load virtual tracker state from Supabase on startup."""
    global _vt_stats, _vt_trades, _vt_fund, _coin_signal_state
    if not SUPABASE_OK: return
    import json as _json
    try:
        res = _supabase.table('bot_data').select('value').eq('key', 'virtual_tracker').execute()
        if not res.data: return
        state = _json.loads(res.data[0]['value'])
        with _vt_lock:
            _vt_stats.update(state.get('vt_stats', {}))
            _vt_trades.extend(state.get('vt_trades', []))
            _vt_fund.update(state.get('vt_fund', {}))
            # Recalculate total_fees from actual trade records to fix any persisted double-count
            fees_by_coin = {}
            for t in _vt_trades:
                sym = t.get('symbol')
                if not sym: continue
                fees_by_coin[sym] = round(
                    fees_by_coin.get(sym, 0.0) + t.get('buy_fee', 0.0) + t.get('sell_fee', 0.0), 6
                )
            for sym, recalc_fees in fees_by_coin.items():
                if sym in _vt_stats:
                    _vt_stats[sym]['total_fees'] = recalc_fees
        with _coin_signal_state_lock:
            _coin_signal_state.update(state.get('coin_signal_state', {}))
        print(f"[VT] Loaded state: {list(_vt_stats.keys())} — cycle states: {dict(_coin_signal_state)}")
    except Exception as e:
        print(f"[VT LOAD ERROR] {e}")

# Hyperliquid spot base tier fees (taker = market order, which bot uses)
_VT_TAKER_FEE = 0.00070   # 0.07% per side (buy = market IOC)
_VT_MAKER_FEE = 0.00070   # 0.07% (sell = SL/TP/signal all fire as market/IOC = taker)

# Virtual Tracker — configurable risk params (same defaults as real bot)
VT_SL_PCT    = 1.5   # Stop Loss: % below entry price
VT_TRAIL_PCT = 1.0   # Trailing Stop: % below peak price
VT_TP_PCT    = 0.5   # Take Profit: % above entry price

def _vt_on_buy(symbol, price, timeframe, initial_fund=None):
    """Record virtual BUY at signal price."""
    with _vt_lock:
        existing = _vt_fund.get(symbol, {})
        if existing.get('buy_price'):
            return  # already in virtual position — wait for sell
        existing = _vt_stats.get(symbol) or {}
        fund = existing.get('fund', initial_fund or _VT_INITIAL)
        if symbol not in _vt_stats:
            _vt_stats[symbol] = {
                'total_trades': 0, 'wins': 0, 'total_pnl': 0.0,
                'total_fees': 0.0, 'fund': fund, 'initial_fund': fund
            }
        import time as _vt_t
        buy_fee  = round(fund * _VT_TAKER_FEE, 6)   # fee on buy notional
        fund_after_fee = round(fund - buy_fee, 6)     # effective capital after buy fee
        amount   = round(fund_after_fee / price, 8)
        _vt_fund[symbol] = {
            'fund': fund_after_fee, 'buy_price': price, 'amount': amount,
            'buy_fee': buy_fee,
            'buy_time': (__import__('datetime').datetime.utcnow() + __import__('datetime').timedelta(hours=5, minutes=30)).strftime('%H:%M:%S'),
            'timeframe': timeframe, 'entry_ts': _vt_t.time(),
            'entry_price': price,   # fixed for SL/TP calc
            'peak_price': price,    # updated as price rises (trailing stop)
        }
        # Track buy fee immediately in stats
        _vt_stats[symbol]['total_fees'] = round(_vt_stats[symbol].get('total_fees', 0.0) + buy_fee, 6)
    __import__('threading').Thread(target=_vt_persist, daemon=True).start()

    # ── Push ORDER PLACED + BUY events — Steps 3 & 4 of trade flow (VT) ──────
    try:
        bot_engine._push_event('order_placed',
            f"ORDER PLACED — VT BUY {symbol} @ ${price:.6f} | ${fund_after_fee:.4f} USDC | TF={timeframe}",
            {'symbol': symbol, 'price': price, 'usdt': fund_after_fee,
             'mode': 'VT', 'tf': timeframe, 'step': 'order_placed'})
        bot_engine._push_event('buy', f"[VT] BUY {symbol} @ {price:.6f} — signal_buy_{timeframe}",
            {'symbol': symbol, 'price': price, 'usdt': fund_after_fee,
             'reason': f'signal_buy_{timeframe}'})
    except Exception:
        pass

def _vt_on_sell(symbol, price, exit_reason='signal'):
    """Record virtual SELL at signal price, compound fund."""
    with _vt_lock:
        entry = _vt_fund.get(symbol)
        if not entry or not entry.get('buy_price'):
            return
        buy_price  = entry['buy_price']
        amount     = entry['amount']
        fund_in    = entry['fund']
        buy_fee    = entry.get('buy_fee', 0.0)
        peak_price = entry.get('peak_price', buy_price)   # ATH since buy

        gross_out  = round(amount * price, 6)
        sell_fee   = round(gross_out * _VT_MAKER_FEE, 6)    # sell = taker (SL/TP/signal = market IOC)
        fund_out   = round(gross_out - sell_fee, 4)          # net after sell fee
        total_fee  = round(buy_fee + sell_fee, 6)
        # fund_in is post-buy-fee capital; recover original to get true net PnL vs what we started with
        original_fund = round(fund_in + buy_fee, 6)
        pnl_gross  = round(gross_out - original_fund, 4)    # price move only, before any fees
        pnl_usdt   = round(fund_out  - original_fund, 4)    # true net P&L after BOTH buy+sell fees
        pnl_pct    = round((price - buy_price) / buy_price * 100, 2)
        peak_pct   = round((peak_price - buy_price) / buy_price * 100, 2)  # max pump %

        trade = {
            'symbol': symbol, 'buy_price': buy_price, 'sell_price': price,
            'peak_price': round(peak_price, 6), 'peak_pct': peak_pct,
            'buy_time': entry.get('buy_time', ''),
            'sell_time': (__import__('datetime').datetime.utcnow() + __import__('datetime').timedelta(hours=5, minutes=30)).strftime('%H:%M:%S'),
            'timeframe': entry.get('timeframe', ''),
            'fund_in': fund_in, 'fund_out': fund_out,
            'buy_fee': buy_fee, 'sell_fee': sell_fee, 'total_fee': total_fee,
            'pnl_gross': pnl_gross,
            'pnl_usdt': pnl_usdt, 'pnl_pct': pnl_pct, 'win': pnl_usdt > 0,
            'exit_reason': exit_reason,
        }
        _vt_trades.append(trade)
        if symbol not in _vt_stats:
            _vt_stats[symbol] = {'total_trades': 0, 'wins': 0, 'total_pnl': 0.0, 'total_fees': 0.0, 'fund': fund_in, 'initial_fund': fund_in}
        _vt_stats[symbol]['total_trades'] += 1
        _vt_stats[symbol]['total_pnl']    = round(_vt_stats[symbol]['total_pnl'] + pnl_usdt, 4)
        # buy_fee already added to total_fees in _vt_on_buy — only add sell_fee here
        _vt_stats[symbol]['total_fees']   = round(_vt_stats[symbol].get('total_fees', 0.0) + sell_fee, 6)
        if pnl_usdt > 0:
            _vt_stats[symbol]['wins'] += 1
        _initial = _vt_stats[symbol].get('initial_fund', fund_in)
        new_fund = max(round(fund_out, 4), round(_initial * 0.1, 4))
        _vt_stats[symbol]['fund'] = new_fund
        _vt_fund[symbol] = {'fund': new_fund, 'buy_price': None, 'amount': 0, 'timeframe': '', 'last_sell_ts': __import__('time').time()}
    __import__('threading').Thread(target=_vt_persist, daemon=True).start()

def _vt_get_summary():
    """Returns virtual P&L summary for /api/virtual/summary endpoint."""
    with _vt_lock:
        total_pnl    = round(sum(s['total_pnl'] for s in _vt_stats.values()), 4)
        total_fees   = round(sum(s.get('total_fees', 0.0) for s in _vt_stats.values()), 6)
        total_trades = sum(s['total_trades'] for s in _vt_stats.values())
        total_wins   = sum(s['wins']         for s in _vt_stats.values())
        win_rate     = round(total_wins / total_trades * 100, 1) if total_trades else 0
        by_coin = {}
        for sym, s in _vt_stats.items():
            op = _vt_fund.get(sym) or {}
            initial = s.get('initial_fund', _VT_INITIAL)
            cur_fund = s['fund']
            in_pos   = bool(op.get('buy_price'))

            # Live unrealized P&L for open positions
            live_price       = None
            unrealized_pnl   = None
            unrealized_pct   = None
            live_fund        = cur_fund

            price_stale = False
            if in_pos:
                try:
                    live_price = bot_engine.client.get_spot_price(sym)
                except Exception:
                    live_price = None
                # NOTE: no stale-price fallback — wrong price → wrong unrealized
                # If price unavailable, leave unrealized_pnl as None (UI shows "...")
                if live_price and op.get('amount') and op.get('buy_price'):
                    gross_live     = round(op['amount'] * live_price, 6)
                    # original_fund = capital deployed before buy fee deducted
                    _buy_fee_op    = op.get('buy_fee', round(op['fund'] * _VT_TAKER_FEE / (1 - _VT_TAKER_FEE), 6))
                    original_fund  = round(op['fund'] + _buy_fee_op, 6)
                    # Unrealized P&L = price move only (buy fee already paid, sell fee not yet incurred)
                    unrealized_pnl = round(gross_live - original_fund, 4)
                    unrealized_pct = round((live_price - op['buy_price']) / op['buy_price'] * 100, 3)
                    # display_fund = what the position is worth right now (gross, no sell fee yet)
                    live_fund      = gross_live

            display_fund = live_fund if (in_pos and live_price) else cur_fund
            growth_pct   = round((display_fund - initial) / initial * 100, 2) if initial else 0

            # Pending buy fee already deducted in open position
            coin_fees = s.get('total_fees', 0.0)

            # Coin-level SL/TP/Trail (same values used by real bot and VT logic)
            _ccfg      = bot_engine.coins.get(sym, {})
            _sl_pct_c  = _ccfg.get('stop_loss',     VT_SL_PCT)
            _tp_pct_c  = _ccfg.get('take_profit',   VT_TP_PCT)
            _tr_pct_c  = _ccfg.get('trailing_stop', VT_TRAIL_PCT)
            _ep        = op.get('entry_price', op.get('buy_price', 0))

            by_coin[sym] = {
                'total_trades':   s['total_trades'],
                'wins':           s['wins'],
                'losses':         s['total_trades'] - s['wins'],
                'win_rate':       round(s['wins'] / s['total_trades'] * 100, 1) if s['total_trades'] else 0,
                'total_pnl':      s['total_pnl'],
                'total_fees':     round(coin_fees, 4),
                'initial_fund':   initial,
                'current_fund':   display_fund,
                'growth_pct':     growth_pct,
                'in_position':    in_pos,
                'entry_price':    op.get('buy_price'),
                'entry_time':     op.get('buy_time'),
                'entry_tf':       op.get('timeframe'),
                'pending_buy_fee': round(op.get('buy_fee', 0.0), 4) if in_pos else None,
                'live_price':     live_price,
                'unrealized_pnl': unrealized_pnl,
                'unrealized_pct': unrealized_pct,
                # Risk levels for open positions — use coin-level settings (same as real bot)
                'vt_sl_price':    round(_ep * (1 - _sl_pct_c / 100), 6) if in_pos else None,
                'vt_tp_price':    round(_ep * (1 + _tp_pct_c / 100), 6) if in_pos else None,
                'sl_pct':         _sl_pct_c,
                'tp_pct':         _tp_pct_c,
                'vt_peak_price':  op.get('peak_price') if in_pos else None,
                'buy_usd':        round(op.get('fund', 0.0), 4) if in_pos else None,
                'buy_qty':        round(op.get('amount', 0.0), 6) if in_pos else None,
            }
        return {
            'total_pnl': total_pnl, 'total_fees': total_fees,
            'total_trades': total_trades,
            'win_rate':  win_rate,  'by_coin': by_coin,
            'recent_trades': list(reversed(_vt_trades[-1000:])),
        }


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
        self._refreshing_since = time.time()   # watchdog: auto-clear if stuck > 3 min

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
                                    # Store @idx spot prices
                                    spot_mids = {k: v for k, v in mids.items() if k.startswith('@')}
                                    if spot_mids:
                                        price_cache.update(spot_mids)
                                    # Also store plain names as perp fallback for inactive spot pairs
                                    # NOTE: ZEC/WLD/MOG/PUMP/HYPE have active spot markets (UZEC/UWLD/etc)
                                    # — do NOT store their perp price under plain key or get_spot_price
                                    #   will return the wrong (perp) price when @idx is missing from cache
                                    PERP_FALLBACK = {'BTC','ETH','SOL','AVAX','LINK','AAVE','XRP'}
                                    perp_mids = {k: v for k, v in mids.items() if k in PERP_FALLBACK}
                                    if perp_mids:
                                        price_cache.update(perp_mids)
                                    # ── VT SL/TP/Trail check on every tick (WSS-driven) ──
                                    self._vt_check_exits(mids)
                                    self._real_check_exits(mids)
                        except Exception as e:
                            logger.warning(f"WS parse error: {e}")
            except Exception as e:
                logger.warning(f"WS disconnected: {e} — reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def _vt_check_exits(self, mids: dict):
        """Called on every allMids WSS tick — checks SL/TP/Trail for all open VT positions.
        Much faster than 15s decision loop — catches price spikes that would otherwise be missed."""
        with _vt_lock:
            open_positions = {sym: pos.copy() for sym, pos in _vt_fund.items()
                              if pos.get('buy_price')}
        if not open_positions:
            return

        # Build a fast symbol→price lookup from this tick's mids
        # Covers both @idx spot keys and plain perp keys
        SPOT_ALIAS_TO_PLAIN = {
            'UZEC':'ZEC','UWLD':'WLD','UMOG':'MOG','UPUMP':'PUMP',
            'USOL':'SOL','UBTC':'BTC','UETH':'ETH',
        }
        def _price_from_mids(symbol):
            # price_cache already updated with this tick before we were called
            p = self.client.get_spot_price(symbol)
            if p:
                return p
            # Fallback: try plain key and alias keys directly in mids
            for key in [symbol.upper(),
                        f'U{symbol.upper()}',
                        *[k for k,v in SPOT_ALIAS_TO_PLAIN.items() if v == symbol.upper()]]:
                raw = mids.get(key)
                if raw:
                    try:
                        val = float(raw)
                        if 1e-9 < val < 1e9:
                            return val
                    except Exception:
                        pass
            return None

        for symbol, pos in open_positions.items():
            price = _price_from_mids(symbol)
            if not price:
                continue
            # Update peak price
            with _vt_lock:
                if symbol in _vt_fund and _vt_fund[symbol].get('buy_price'):
                    if price > _vt_fund[symbol].get('peak_price', price):
                        _vt_fund[symbol]['peak_price'] = price
                    peak_price = _vt_fund[symbol].get('peak_price', pos['buy_price'])
                else:
                    continue  # position already closed
            entry_price = pos.get('entry_price', pos['buy_price'])
            _ccfg2      = self.bot.coins.get(symbol, {})
            _sl_p2      = _ccfg2.get('stop_loss',   VT_SL_PCT)
            _tp_p2      = _ccfg2.get('take_profit', VT_TP_PCT)
            sl_price    = entry_price * (1 - _sl_p2 / 100)
            tp_price    = entry_price * (1 + _tp_p2 / 100)
            exit_reason = None
            if price <= sl_price:
                exit_reason = f'SL {_sl_p2}%'
            elif price >= tp_price:
                exit_reason = f'TP {_tp_p2}%'
            if exit_reason:
                logger.info(f"[VT-WSS] EXIT {symbol} @ {price:.4f} reason={exit_reason}")
                _record_sell_signal(symbol, price, exit_reason=exit_reason)

    def _real_check_exits(self, mids: dict):
        """WSS-driven exit for REAL holdings — same logic as VT, fires on every allMids tick."""
        holdings = dict(self.bot.holdings)  # snapshot
        if not holdings:
            return
        for symbol, holding in holdings.items():
            if not holding.get('entries'):
                continue
            # Get price from cache (already updated before this call)
            price = self.client.get_spot_price(symbol)
            if not price:
                continue
            entries   = holding['entries']
            avg_entry = sum(e['usdt'] for e in entries) / sum(e['amount'] for e in entries) if entries else 0
            if not avg_entry:
                continue
            sl_price    = avg_entry * (1 - VT_SL_PCT   / 100)
            tp_price    = avg_entry * (1 + VT_TP_PCT   / 100)
            exit_reason = None
            if price <= sl_price:
                exit_reason = f'sl_{VT_SL_PCT}pct'
            elif price >= tp_price:
                exit_reason = f'tp_{VT_TP_PCT}pct'
            if exit_reason:
                logger.info(f"[REAL-WSS] EXIT {symbol} @ {price:.4f} reason={exit_reason}")
                self.bot.holdings[symbol]['last_sell_ts'] = __import__('time').time()
                # Schedule sell as async task — don't block WSS loop
                asyncio.ensure_future(
                    self._run_order(self.bot._execute_sell, symbol, price, exit_reason),
                    loop=self.loop
                )

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
                        self._refreshing_since = time.time()
                        try:
                            # One shared session for all coins — avoids connection burst
                            async with aiohttp.ClientSession() as sess:
                                for sym in new_coins:
                                    # Seed cache via REST — one TF at a time with delay
                                    for tf in get_active_tfs():
                                        candles = await self.client.async_get_candles(sess, sym, tf, semaphore=self._sem)
                                        if candles:
                                            await candle_cache.set(sym, tf, candles)
                                        await asyncio.sleep(0.3)   # 300ms gap per TF to avoid burst
                                    # Subscribe via WS for live updates
                                    for tf in get_active_tfs():
                                        coin_id = self.client._resolve_candle_coin(sym)
                                        sub = json.dumps({"method": "subscribe", "subscription": {
                                            "type": "candle", "coin": coin_id, "interval": tf}})
                                        await ws.send(sub)
                                    subscribed_coins.add(sym)
                                    logger.info(f"✅ WS candle subscribed: {sym} × {get_active_tfs()}")
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
                                if not sym or interval not in get_active_tfs(): continue

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
            # ── Watchdog: if _refreshing stuck > 3 min, force-clear it ──────
            if self._refreshing:
                stuck_for = time.time() - getattr(self, '_refreshing_since', time.time())
                if stuck_for > 180:
                    logger.warning(f"⚠️ _refreshing stuck for {stuck_for:.0f}s — force clearing (candle cache was blocked)")
                    self._refreshing = False

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
        self._refreshing_since = time.time()
        try:
            t0 = time.time()
            logger.info(f"🔄 Refreshing candles: {len(coins)} coins × {len(get_active_tfs())} TFs")
            # Single shared session for all requests — avoids TCP burst
            async with aiohttp.ClientSession() as session:
                for sym in coins:
                    for tf in get_active_tfs():
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
            for tf_key in get_active_tfs():
                tf_sig = tf_breakdown.get(tf_key, 'neutral')
                tf_data = (mtf.get('tf_results') or {}).get(tf_key, {})
                rsi_val = tf_data.get('rsi', '—') if tf_data else '—'
                self.bot._push_event('monitor',
                    f"{symbol} | score={SIGNAL_SCORES.get(tf_sig,0)} | {tf_sig} | {'high' if tf_key == best_tf else 'low'} | {tf_key} | {price:.6f}",
                    {'symbol': symbol, 'score': SIGNAL_SCORES.get(tf_sig, 0),
                     'direction': tf_sig, 'confidence': 'high' if tf_key == best_tf else 'low',
                     'price': price, 'timeframe': tf_key})

            # Record buy signal for daily stats panel
            if direction == 'buy':
                _coin_capital = self.bot.coins.get(symbol, {}).get('compound_capital',
                                self.bot.coins.get(symbol, {}).get('capital', _VT_INITIAL))
                _record_buy_signal(
                    symbol=symbol, price=price,
                    timeframe=best_tf,
                    score=mtf_score,
                    executed=False,
                    capital=_coin_capital
                )

                # ── Push SIGNAL event — Step 1 of trade flow ──────────────────
                tf_res    = (mtf.get('tf_results') or {}).get(best_tf, {})
                rsi_now   = tf_res.get('rsi', 0)
                macd_now  = tf_res.get('macd', 'neutral')
                vol_now   = tf_res.get('vol', False)
                self.bot._push_event('signal',
                    f"📶 SIGNAL — {symbol} @ ${price:.6f} | TF={best_tf} | RSI={rsi_now:.1f} | MACD={macd_now}",
                    {'symbol': symbol, 'price': price, 'tf': best_tf,
                     'rsi': rsi_now, 'macd': macd_now, 'vol': vol_now,
                     'score': mtf_score, 'confidence': confidence,
                     'step': 'signal'})

                # ── Step 2: Confirmation checks — Support, EMA9>EMA21, Volume ──
                candles_best = candle_cache.get(symbol, best_tf) or []
                support_lvl  = self.bot._find_support_zone(candles_best, lookback=20) if len(candles_best) >= 5 else None
                near_sup     = False
                dist_pct     = 0.0
                if support_lvl and support_lvl > 0:
                    dist_pct = (price - support_lvl) / support_lvl * 100
                    near_sup = dist_pct <= 1.5

                ema9_arr = ema21_arr = []
                if len(candles_best) >= 22:
                    closes_b  = [float(c[4]) for c in candles_best]
                    ema9_arr  = _ema_fn(closes_b, 9)
                    ema21_arr = _ema_fn(closes_b, 21)
                ema_bull_now = bool(ema9_arr and ema21_arr and ema9_arr[-1] > ema21_arr[-1])

                layers_ok = sum([near_sup, ema_bull_now, bool(vol_now)])

                # Push confirmation check event to UI
                self.bot._push_event('support_check',
                    f"🔍 CHECK — {symbol} | Support={'✅' if near_sup else '❌'}({dist_pct:.1f}%) | EMA={'✅' if ema_bull_now else '❌'} | Vol={'✅' if vol_now else '❌'} | {layers_ok}/3 passed",
                    {'symbol': symbol, 'price': price, 'tf': best_tf,
                     'support': support_lvl, 'dist_pct': round(dist_pct, 3),
                     'near_support': near_sup, 'ema_bull': ema_bull_now,
                     'vol_ok': bool(vol_now), 'layers': layers_ok,
                     'step': 'support_check'})

                # ── Step 3: BUY or SKIP decision ─────────────────────────────
                _confirm_ok = layers_ok >= 2

                if not _confirm_ok:
                    # Build skip reason
                    skip_reasons = []
                    if not near_sup:
                        skip_reasons.append(f"Support door ({dist_pct:.1f}%)")
                    if not ema_bull_now:
                        skip_reasons.append("EMA9<EMA21 (trend nahi)")
                    if not vol_now:
                        skip_reasons.append("Volume weak")
                    skip_msg = " | ".join(skip_reasons) if skip_reasons else "Confirmation fail"
                    self.bot._push_event('warn',
                        f"⛔ SKIP — {symbol} @ ${price:.6f} | TF={best_tf} | {skip_msg}",
                        {'symbol': symbol, 'price': price, 'tf': best_tf,
                         'skip_reasons': skip_reasons, 'layers': layers_ok,
                         'step': 'skip'})
                    direction = 'neutral'  # block actual buy below

            # ── Same-TF Momentum Confirmation ─────────────────────────────────
            # direction already set to 'neutral' if confirmation failed (SKIP path above)
            _3m_momentum_ok = True
            _buy_confirmed  = (direction == 'buy')

            # Virtual tracker — BUY/SELL cycle with SL / TP / Trailing Stop
            vt_pos = _vt_fund.get(symbol, {})
            if vt_pos.get('buy_price'):
                # Update peak price for trailing stop
                with _vt_lock:
                    if price > _vt_fund[symbol].get('peak_price', price):
                        _vt_fund[symbol]['peak_price'] = price

                entry_price = vt_pos.get('entry_price', vt_pos['buy_price'])
                peak_price  = _vt_fund[symbol].get('peak_price', entry_price)

                _coin_cfg   = self.bot.coins.get(symbol, {})
                _sl_pct     = _coin_cfg.get('stop_loss',     VT_SL_PCT)
                _tp_pct     = _coin_cfg.get('take_profit',   VT_TP_PCT)
                _trail_pct  = _coin_cfg.get('trailing_stop', VT_TRAIL_PCT)

                sl_price    = entry_price * (1 - _sl_pct    / 100)
                tp_price    = entry_price * (1 + _tp_pct    / 100)

                vt_exit_reason = None
                if price <= sl_price:
                    vt_exit_reason = f'SL {_sl_pct}%'
                elif price >= tp_price:
                    vt_exit_reason = f'TP {_tp_pct}%'
                else:
                    # No price-based exit — check signal on locked TF
                    vt_tf     = vt_pos.get('timeframe', best_tf)
                    vt_tf_sig = (mtf.get('tf_results') or {}).get(vt_tf, {}).get('signal', 'neutral')
                    if vt_tf_sig == 'sell':
                        vt_exit_reason = 'signal'

                if vt_exit_reason:
                    _record_sell_signal(symbol, price, exit_reason=vt_exit_reason)
                    logger.info(f"[VT] SELL {symbol} @ {price:.4f} reason={vt_exit_reason} | pnl={((price/entry_price)-1)*100:.2f}%")
            else:
                # No position — BUY signal on any TF opens virtual position on that TF
                # Cooldown: wait at least 60s after last sell before re-buying same coin
                _vt_last_sell = _vt_fund.get(symbol, {}).get('last_sell_ts', 0)
                _vt_cooldown_ok = (__import__('time').time() - _vt_last_sell) >= 60
                if _buy_confirmed and _vt_cooldown_ok and _3m_momentum_ok:
                    _coin_capital = self.bot.coins.get(symbol, {}).get('compound_capital',
                                    self.bot.coins.get(symbol, {}).get('capital', _VT_INITIAL))
                    _vt_on_buy(symbol, price, best_tf, initial_fund=_coin_capital)
                    logger.info(f"[VT] BUY  {symbol} @ {price:.4f} TF={best_tf}")

            if has_holding:
                holding   = self.bot.holdings[symbol]
                entries   = holding['entries']
                avg_entry = sum(e['usdt'] for e in entries) / sum(e['amount'] for e in entries)
                trade_tf  = holding.get('trade_tf', best_tf)  # usi TF pe monitor karo jis pe buy hua

                # Peak price update for trailing stop
                peak = holding.get('peak_price', avg_entry)
                if price > peak:
                    holding['peak_price'] = price
                    peak = price

                # Fixed % exits — SL + TP only (no trailing stop)
                sl_price    = avg_entry * (1 - VT_SL_PCT   / 100)
                tp_price    = avg_entry * (1 + VT_TP_PCT   / 100)

                # 1. Stop Loss
                if price <= sl_price:
                    holding['last_sell_ts'] = __import__('time').time()
                    await self._run_order(self.bot._execute_sell, symbol, price, f'sl_{VT_SL_PCT}pct')
                    return

                # 2. Take Profit
                if price >= tp_price:
                    holding['last_sell_ts'] = __import__('time').time()
                    await self._run_order(self.bot._execute_sell, symbol, price, f'tp_{VT_TP_PCT}pct')
                    return

                # 3. Signal-based sell — same TF pe bearish reversal
                tf_sig = (mtf.get('tf_results') or {}).get(trade_tf, {}).get('signal', 'neutral')
                if tf_sig == 'sell':
                    holding['last_sell_ts'] = __import__('time').time()
                    await self._run_order(self.bot._execute_sell, symbol, price, f'signal_sell_{trade_tf}')
                    return

            else:
                # 60s cooldown after sell before re-buying same coin
                _last_sell = self.bot.holdings.get(symbol, {}).get('last_sell_ts', 0)
                _cooldown_ok = (__import__('time').time() - _last_sell) >= 60
                if _buy_confirmed and trade_cap > 0 and _cooldown_ok and _3m_momentum_ok:
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
        self.bracket_orders  = {}        # {symbol: {'tp_oid': int|None, 'sl_oid': int|None, 'coin_str': str, 'size': float}}
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
            'HPUMP':'PUMPFUN','FXRP':'XRP','TRX1':'TRX',
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
                # ── Place bracket TP + SL orders immediately after fill ──
                self._place_bracket_orders(symbol, coin_str, size, filled_px)
                return {'success': True, 'price': filled_px, 'size': size, 'raw': result}
            return None
        except Exception as e: logger.error(f"Live buy error {symbol}: {e}"); return None

    # ── Bracket orders: place TP + SL as GTC limits after buy ─────────────────
    def _place_bracket_orders(self, symbol, coin_str, size, fill_price):
        """Place TP (limit GTC) and SL (limit GTC) immediately after a live buy fill."""
        tp_pct    = self.coins.get(symbol, {}).get('take_profit',   0.5) / 100
        sl_pct    = self.coins.get(symbol, {}).get('stop_loss',     1.5) / 100
        tp_price  = round(fill_price * (1 + tp_pct), 6)
        sl_price  = round(fill_price * (1 - sl_pct), 6)
        tp_oid = None; sl_oid = None
        try:
            r = self.exchange.order(coin_str, is_buy=False, sz=round(size, 6),
                                    limit_px=tp_price,
                                    order_type={"limit": {"tif": "Gtc"}}, reduce_only=False)
            if r.get('status') == 'ok':
                st = r.get('response',{}).get('data',{}).get('statuses',[{}])[0]
                tp_oid = st.get('resting',{}).get('oid') or st.get('filled',{}).get('oid')
                logger.info(f"[BRACKET] TP order {symbol} @ {tp_price:.6f} oid={tp_oid}")
            else:
                logger.warning(f"[BRACKET] TP order failed {symbol}: {r}")
        except Exception as e:
            logger.error(f"[BRACKET] TP order error {symbol}: {e}")
        try:
            r = self.exchange.order(coin_str, is_buy=False, sz=round(size, 6),
                                    limit_px=sl_price,
                                    order_type={"limit": {"tif": "Gtc"}}, reduce_only=False)
            if r.get('status') == 'ok':
                st = r.get('response',{}).get('data',{}).get('statuses',[{}])[0]
                sl_oid = st.get('resting',{}).get('oid') or st.get('filled',{}).get('oid')
                logger.info(f"[BRACKET] SL order {symbol} @ {sl_price:.6f} oid={sl_oid}")
            else:
                logger.warning(f"[BRACKET] SL order failed {symbol}: {r}")
        except Exception as e:
            logger.error(f"[BRACKET] SL order error {symbol}: {e}")
        self.bracket_orders[symbol] = {
            'tp_oid': tp_oid, 'sl_oid': sl_oid,
            'coin_str': coin_str, 'size': round(size, 6),
            'fill_price': fill_price
        }
        self._push_event('info',
            f"[BRACKET] {symbol} TP@{tp_price:.4f} SL@{sl_price:.4f} placed (maker GTC)",
            {'symbol': symbol, 'tp_price': tp_price, 'sl_price': sl_price})

    def _cancel_bracket_orders(self, symbol):
        """Cancel both TP and SL bracket orders for a symbol (call before any sell)."""
        b = self.bracket_orders.pop(symbol, None)
        if not b:
            return
        coin_str = b.get('coin_str', '')
        for label, oid in [('TP', b.get('tp_oid')), ('SL', b.get('sl_oid'))]:
            if not oid:
                continue
            try:
                idx = None
                # coin_str is like @10234 — extract numeric part
                if coin_str.startswith('@'):
                    idx_raw = coin_str[1:]
                    r = self.exchange.cancel(coin_str, int(oid))
                    logger.info(f"[BRACKET] Cancelled {label} order {symbol} oid={oid}: {r.get('status')}")
                else:
                    r = self.exchange.cancel(coin_str, int(oid))
                    logger.info(f"[BRACKET] Cancelled {label} order {symbol} oid={oid}: {r.get('status')}")
            except Exception as e:
                logger.warning(f"[BRACKET] Cancel {label} error {symbol} oid={oid}: {e}")

    def _update_bracket_sl(self, symbol, new_sl_price):
        """Cancel existing SL and place a new one at new_sl_price (trailing stop update)."""
        b = self.bracket_orders.get(symbol)
        if not b or not b.get('coin_str'):
            return
        coin_str = b['coin_str']
        size     = b['size']
        # Cancel old SL
        old_sl_oid = b.get('sl_oid')
        if old_sl_oid:
            try:
                self.exchange.cancel(coin_str, int(old_sl_oid))
                logger.info(f"[BRACKET] Trailing: cancelled old SL {symbol} oid={old_sl_oid}")
            except Exception as e:
                logger.warning(f"[BRACKET] Trailing: cancel SL error {symbol}: {e}")
        # Place new SL at updated trailing price
        new_sl_oid = None
        try:
            r = self.exchange.order(coin_str, is_buy=False, sz=size,
                                    limit_px=round(new_sl_price, 6),
                                    order_type={"limit": {"tif": "Gtc"}}, reduce_only=False)
            if r.get('status') == 'ok':
                st = r.get('response',{}).get('data',{}).get('statuses',[{}])[0]
                new_sl_oid = st.get('resting',{}).get('oid') or st.get('filled',{}).get('oid')
                logger.info(f"[BRACKET] Trailing: new SL {symbol} @ {new_sl_price:.6f} oid={new_sl_oid}")
        except Exception as e:
            logger.error(f"[BRACKET] Trailing: new SL error {symbol}: {e}")
        self.bracket_orders[symbol]['sl_oid'] = new_sl_oid

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
    def add_coin(self, symbol, capital, timeframe='auto', stop_loss=1.5, trailing_stop=1.0, take_profit=0.5):
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
        for k in ['capital', 'timeframe', 'timeframes', 'stop_loss', 'trailing_stop', 'take_profit', 'enabled']:
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
                        'trend_dir': market.get('trend_dir', 'sideways'),
                        'holding': total_held, 'avg_entry': avg_entry,
                        'pnl_pct': round(pnl, 2),
                        'peak_price': holding.get('peak_price',0) if holding else 0}
            except Exception as e:
                logger.error(f"get_coins {sym}: {e}")
                return {**self.coins.get(sym,{}),
                        'price':0,'rsi':50,'macd_signal':'neutral','volume_signal':False,
                        'signal':'neutral','mtf_score':0,'best_timeframe':'N/A',
                        'confidence':'low','trend_dir':'sideways',
                        'holding':0,'avg_entry':0,'pnl_pct':0,'peak_price':0}

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
                'cache_keys': sum(1 for sym in self.coins for tf in get_active_tfs()
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

    def _find_support_zone(self, candles, lookback=20):
        """
        Find nearest support zone from recent candle lows.
        Returns support price level.
        """
        if len(candles) < lookback:
            lookback = len(candles)
        recent = candles[-lookback:]
        lows   = [float(c[3]) for c in recent]
        # Support = lowest low of lookback period
        return min(lows)

    def _signal_for_candles(self, candles):
        """
        Signal Detection (pure):
        BUY:  MACD bull cross (last 2 candles) + RSI 10-80 + Higher High (uptrend confirm)
        SELL: MACD bear cross (last 2 candles) + RSI > 52
        Confirmation (support/EMA/volume) is done AFTER signal in _process_coin.
        """
        if not candles or len(candles) < 35:
            return 'neutral', 50.0, 'neutral', False, 0.0, 'sideways'

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
            return 'neutral', rsi_now, 'neutral', vol_sig, atr, 'sideways'

        hist_now     = macd_ln[-1] - sig_ln[-1]
        macd_sig_str = 'bullish' if hist_now > 0 else ('bearish' if hist_now < 0 else 'neutral')

        def _bull_cross_recent(ml, sl, window=2):
            for i in range(1, min(window+1, len(ml))):
                if ml[-(i+1)] <= sl[-(i+1)] and ml[-i] > sl[-i]:
                    return True
            return False
        def _bear_cross_recent(ml, sl, window=2):
            for i in range(1, min(window+1, len(ml))):
                if ml[-(i+1)] >= sl[-(i+1)] and ml[-i] < sl[-i]:
                    return True
            return False

        macd_bull_cross = _bull_cross_recent(macd_ln, sig_ln)
        macd_bear_cross = _bear_cross_recent(macd_ln, sig_ln)

        # Uptrend check: last candle made a higher high than previous candle
        highs  = [float(c[2]) for c in candles]
        lows   = [float(c[3]) for c in candles]
        higher_high = highs[-1] > highs[-2]
        lower_low   = lows[-1] < lows[-2]

        # trend_dir: uptrend / downtrend / sideways
        if higher_high and not lower_low:
            trend_dir = 'uptrend'
        elif lower_low and not higher_high:
            trend_dir = 'downtrend'
        else:
            trend_dir = 'sideways'

        if macd_bear_cross and rsi_now > 52:
            return 'sell', rsi_now, macd_sig_str, vol_sig, atr, trend_dir

        # BUY: MACD bull cross + RSI in range + uptrend (higher high) only — no layers here
        if macd_bull_cross and 10 <= rsi_now <= 80 and higher_high:
            return 'buy', rsi_now, macd_sig_str, vol_sig, atr, trend_dir

        return 'neutral', rsi_now, macd_sig_str, vol_sig, atr, trend_dir

    def _mtf_scan(self, symbol: str) -> dict:
        tf_results  = {}

        # Per-coin TFs override global if set
        coin_cfg    = self.coins.get(symbol, {})
        coin_tfs    = coin_cfg.get('timeframes')  # list or None
        active      = coin_tfs if (coin_tfs and len(coin_tfs) > 0) else get_active_tfs()

        # Higher TF = more reliable signal → scan in DESCENDING order (1d first, 1m last)
        TF_PRIORITY = ['4h','2h','1h','30m','15m','5m']
        active_sorted = [tf for tf in TF_PRIORITY if tf in active]
        # Any TF not in priority list goes at end (shouldn't happen but safety)
        active_sorted += [tf for tf in active if tf not in TF_PRIORITY]

        for tf in active_sorted:
            candles = candle_cache.get(symbol, tf)
            if not candles:
                tf_results[tf] = {'signal': 'neutral', 'score': 0, 'rsi': 50.0,
                                  'macd': 'neutral', 'vol': False, 'atr': 0.0, 'trend_dir': 'sideways'}
                continue
            signal, rsi, macd, vol, atr, trend_dir = self._signal_for_candles(candles)
            score = SIGNAL_SCORES.get(signal, 0)
            tf_results[tf] = {'signal': signal, 'score': score, 'rsi': rsi,
                              'macd': macd, 'vol': vol, 'atr': atr, 'trend_dir': trend_dir}

        # Simple rule: jis TF pe signal mile trade karo.
        # Agar dono buy + sell hon to highest TF (HTF-priority) wala wins.
        direction  = 'neutral'
        confidence = 'low'
        best_tf    = None
        best_atr   = 0.0

        buy_tfs  = [tf for tf in active_sorted if tf_results[tf]['signal'] == 'buy']
        sell_tfs = [tf for tf in active_sorted if tf_results[tf]['signal'] == 'sell']

        if buy_tfs or sell_tfs:
            htf_buy  = TF_PRIORITY.index(buy_tfs[0])  if buy_tfs  else 99
            htf_sell = TF_PRIORITY.index(sell_tfs[0]) if sell_tfs else 99
            if htf_buy <= htf_sell:
                best_tf   = buy_tfs[0]
                direction = 'buy'
                confidence = 'high' if len(buy_tfs) > 1 else 'medium'
            else:
                best_tf   = sell_tfs[0]
                direction = 'sell'
                confidence = 'high' if len(sell_tfs) > 1 else 'medium'

        if best_tf is None:
            # Fallback: pick highest active TF as reference
            best_tf  = active_sorted[0] if active_sorted else '1h'
            best_atr = tf_results.get(best_tf, {}).get('atr', 0.0)
        else:
            best_atr = tf_results[best_tf]['atr']

        best        = tf_results.get(best_tf, {'score':0,'rsi':50.0,'macd':'neutral','vol':False})
        total_score = best['score']
        capital_pct = 1.0 if direction == 'buy' else 0.0

        # trend_dir: ALWAYS best_tf's own value — must stay consistent with RSI/MACD/signal
        # (a cross-TF fallback here used to show a 'green' trend from a different TF than
        #  the one actually checked for the BUY/SELL signal, which looked like everything
        #  was green while the real best_tf trend was sideways — so BUY never fired)
        trend_dir_final = best.get('trend_dir', 'sideways')

        return {'total_score': total_score, 'confidence': confidence,
                'direction': direction, 'best_timeframe': best_tf,
                'capital_pct': capital_pct, 'atr': best_atr,
                'active_tfs': active_sorted,
                'tf_breakdown': {tf: v['signal'] for tf, v in tf_results.items()},
                'tf_results': tf_results,
                'buy_tfs': buy_tfs, 'sell_tfs': sell_tfs,
                'rsi': best['rsi'], 'macd_signal': best['macd'], 'volume_signal': best['vol'],
                'trend_dir': trend_dir_final,
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
                    'tf_breakdown': mtf['tf_breakdown'], 'trend_dir': mtf.get('trend_dir','sideways')}
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
                # Don't push to UI — just log silently, no screen popup
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

        amount = (capital * (1 - _VT_TAKER_FEE)) / actual_price   # deduct buy fee from capital
        buy_fee = round(capital * _VT_TAKER_FEE, 6)
        # Extract TF from reason string e.g. 'signal_buy_4h' → '4h'
        trade_tf = reason.split('_')[-1] if '_' in reason else '1h'
        trade  = {'type':'BUY','symbol':symbol,'price':actual_price,'amount':amount,
                  'usdt':capital,'buy_fee':buy_fee,'reason':reason,'mode':mode_tag,'order_id':order_id,
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
        # ── Push ORDER PLACED event — Step 3 of trade flow ───────────────────
        self._push_event('order_placed',
            f"ORDER PLACED — {mode_tag} BUY {symbol} @ ${actual_price:.6f} | ${capital:.4f} USDC | TF={trade_tf}",
            {'symbol': symbol, 'price': actual_price, 'usdt': capital,
             'mode': mode_tag, 'tf': trade_tf, 'order_id': order_id,
             'step': 'order_placed'})
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

        # ── Cancel any open bracket orders before executing sell ──
        if self.live_mode:
            self._cancel_bracket_orders(symbol)

        if self.live_mode:
            result = self._live_sell(symbol, total_amt, price)
            if result:
                actual_price = result['price']
                order_id     = str(result.get('raw',{}).get('response',{}).get('data',{}).get('statuses',[{}])[0].get('resting',{}).get('oid',''))
                mode_tag     = 'LIVE'
            else:
                self._push_event('error', f"Live sell failed {symbol}", {'symbol':symbol})
                return None

        gross_out  = actual_price * total_amt
        # Live sell uses GTC limit (maker = 0%), sim uses taker estimate
        sell_fee   = round(gross_out * (_VT_MAKER_FEE if self.live_mode else _VT_TAKER_FEE), 6)
        buy_fee    = sum(t.get('buy_fee', 0.0) for t in self.trades if t.get('type') == 'BUY' and t.get('symbol') == symbol and t.get('buy_fee'))
        pnl_usdt   = round(gross_out - sell_fee - total_usdt, 4)   # net after both fees
        pnl_pct    = (actual_price - avg_entry) / avg_entry * 100
        # Find the matching buy trade to pull signal_price and buy_price
        buy_signal_price = None; buy_fill_price = None
        for tr in reversed(self.trades):
            if tr.get('type') == 'BUY' and tr.get('symbol') == symbol:
                buy_signal_price = tr.get('signal_price')
                buy_fill_price   = tr.get('buy_price', tr.get('price'))
                break
        trade    = {'type':'SELL','symbol':symbol,'price':actual_price,'amount':total_amt,
                    'usdt':gross_out,'sell_fee':sell_fee,'total_fee':round(sell_fee+buy_fee,6),
                    'avg_entry':avg_entry,
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
        # Reset signal cycle — next BUY for this coin will count again
        _record_sell_signal(symbol, actual_price)
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

# Load virtual tracker + cycle state from Supabase on startup
_vt_load()

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
    result        = bot_engine.add_coin(symbol, capital, timeframe, stop_loss, trailing_stop, take_profit)
    # Apply timeframes array if provided (from global TF panel)
    tfs = d.get('timeframes')
    if tfs and isinstance(tfs, list):
        valid_tfs = [tf for tf in tfs if tf in ALL_TIMEFRAMES]
        if valid_tfs and symbol in bot_engine.coins:
            bot_engine.coins[symbol]['timeframes'] = valid_tfs
    return jsonify(result)

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

@app.route('/api/memory', methods=['GET'])
def get_memory():
    proc = psutil.Process(os.getpid())
    mem = proc.memory_info()
    rss_mb = mem.rss / 1024 / 1024
    limit_mb = 500
    pct = (rss_mb / limit_mb) * 100
    return jsonify({
        'rss_mb': round(rss_mb, 1),
        'limit_mb': limit_mb,
        'percent': round(pct, 1),
        'status': 'critical' if pct > 85 else 'warning' if pct > 65 else 'ok'
    })

# ── Signal Store — IST 5:30 AM daily window, Supabase persisted 30 days ──
def _ist_day_key(ts=None):
    from datetime import datetime, timezone, timedelta
    IST_OFF = timedelta(hours=5, minutes=30)
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)
    dt_ist = dt_utc + IST_OFF
    if dt_ist.hour < 5 or (dt_ist.hour == 5 and dt_ist.minute < 30):
        dt_ist -= timedelta(days=1)
    return dt_ist.strftime('%Y-%m-%d')

def _ist_day_start_ts(day_key):
    from datetime import datetime, timezone
    d = datetime.strptime(day_key, '%Y-%m-%d')
    # 05:30 IST = 00:00 UTC
    return d.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp()

def _save_signals_sb(day_key, signals):
    if not SUPABASE_OK: return
    try:
        from datetime import datetime, timezone, timedelta
        _supabase.table('bot_data').upsert({
            'key': f'signals_{day_key}',
            'value': signals,
            'updated_at': datetime.now(timezone.utc).isoformat()
        }).execute()
        # Auto-delete keys older than 30 days
        cutoff_key = 'signals_' + (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
        try:
            _supabase.table('bot_data').delete().lt('key', cutoff_key).like('key', 'signals_%').execute()
        except Exception:
            pass
    except Exception as e:
        logger.warning(f'signals sb save: {e}')

def _load_signals_sb(day_key):
    if not SUPABASE_OK: return []
    try:
        res = _supabase.table('bot_data').select('value').eq('key', f'signals_{day_key}').execute()
        if res.data: return res.data[0].get('value') or []
    except Exception:
        pass
    return []

_signal_store      = {}
_signal_store_lock = __import__('threading').Lock()

# Candle duration in seconds per timeframe
_TF_SECONDS = {'15m': 900, '30m': 1800, '1h': 3600, '2h': 7200, '4h': 14400}

def _record_buy_signal(symbol, price, timeframe, score, executed=False, capital=None):
    import time as _t
    from datetime import datetime, timezone, timedelta

    ts  = _t.time()
    key = _ist_day_key(ts)
    IST_OFF = timedelta(hours=5, minutes=30)
    time_str = (datetime.fromtimestamp(ts, tz=timezone.utc) + IST_OFF).strftime('%H:%M:%S')
    sig = {'symbol': symbol, 'price': price, 'timeframe': timeframe,
           'score': score, 'time': time_str, 'ts': ts, 'executed': executed}

    candle_secs = _TF_SECONDS.get(timeframe, 900)

    with _signal_store_lock:
        if key not in _signal_store:
            _signal_store[key] = _load_signals_sb(key)
        # Dedup: same coin + same TF within same candle window = skip
        for ex in reversed(_signal_store[key]):
            if ex['symbol'] == symbol and ex['timeframe'] == timeframe:
                if abs(ex['ts'] - ts) < candle_secs:
                    return  # already recorded this candle
                break  # older signals don't matter
        _signal_store[key].append(sig)
        sigs_copy = list(_signal_store[key])
    # Save in background thread — don't block the bot loop
    __import__('threading').Thread(
        target=_save_signals_sb, args=(key, sigs_copy), daemon=True
    ).start()
    # Virtual tracker BUY is handled in _process_coin directly

def _record_sell_signal(symbol, price=None, exit_reason='signal'):
    """Call this when a SELL fires — resets the cycle so next BUY will be counted."""
    _signal_state_set(symbol, 'sell')
    if price:
        _vt_on_sell(symbol, price, exit_reason=exit_reason)

@app.route('/api/prices', methods=['GET'])
def get_prices():
    """Lightweight price-only endpoint — called every 2s for live price updates."""
    syms = list(bot_engine.coins.keys())
    prices = {}
    for sym in syms:
        p = bot_engine.client.get_spot_price(sym)
        if p: prices[sym] = p
    return jsonify(prices)

@app.route('/api/signals/today', methods=['GET'])
def signals_today():
    from datetime import datetime, timezone, timedelta
    req_date = request.args.get('date')
    day_key  = req_date if req_date else _ist_day_key()
    IST_OFF  = timedelta(hours=5, minutes=30)
    day_start_ts = _ist_day_start_ts(day_key)
    day_end_ts   = day_start_ts + 86400

    with _signal_store_lock:
        if day_key not in _signal_store:
            _signal_store[day_key] = _load_signals_sb(day_key)
        # Only use _record_buy_signal store — no double-counting from events feed
        signals = sorted(_signal_store[day_key], key=lambda x: x['ts'], reverse=True)

    # Per-coin summary
    coin_counts = {}
    for s in signals:
        sym = s['symbol']
        if sym not in coin_counts:
            coin_counts[sym] = {'count': 0, 'last_price': 0, 'last_time': '', 'executed': 0, '_ts': 0}
        coin_counts[sym]['count'] += 1
        if s['ts'] > coin_counts[sym]['_ts']:
            coin_counts[sym].update({'last_price': s['price'], 'last_time': s['time'], '_ts': s['ts']})
        if s['executed']:
            coin_counts[sym]['executed'] += 1
    for v in coin_counts.values(): v.pop('_ts', None)

    d1 = datetime.strptime(day_key, '%Y-%m-%d')
    d2 = d1 + timedelta(days=1)
    window = f"{d1.strftime('%d %b')} 05:30 IST → {d2.strftime('%d %b')} 05:30 IST"

    return jsonify({
        'total': len(signals), 'signals': signals[:100],
        'by_coin': coin_counts, 'date': window, 'day_key': day_key
    })
@app.route('/api/virtual/summary', methods=['GET'])
def virtual_summary():
    """Virtual P&L tracker — shows hypothetical performance based on BUY/SELL signals."""
    return jsonify(_vt_get_summary())

@app.route('/api/live_positions', methods=['GET'])
def live_positions():
    """Live open positions (VT) with entry, SL, TP, current price, unrealized PnL + 3m chart."""

    with _vt_lock:
        out = []
        for sym, op in _vt_fund.items():
            if not op.get('buy_price'):
                continue
            entry   = op['buy_price']
            amount  = op.get('amount', 0)
            buy_fee = op.get('buy_fee', 0)
            fund_in = op.get('fund', 0)
            orig_fund = round(fund_in + buy_fee, 4)

            _ccfg    = bot_engine.coins.get(sym, {})
            sl_pct   = _ccfg.get('stop_loss',     VT_SL_PCT)
            tp_pct   = _ccfg.get('take_profit',   VT_TP_PCT)
            sl_price = round(entry * (1 - sl_pct / 100), 6)
            tp_price = round(entry * (1 + tp_pct / 100), 6)

            live_price = None
            try:
                live_price = bot_engine.client.get_spot_price(sym)
            except Exception:
                pass

            unreal_pnl = None
            unreal_pct = None
            if live_price and amount:
                gross = round(amount * live_price, 6)
                unreal_pnl = round(gross - orig_fund, 4)
                unreal_pct = round((live_price - entry) / entry * 100, 3)

            entry_ts = op.get('entry_ts', 0)
            held_s   = int(time.time() - entry_ts) if entry_ts else 0

            # ── Chart data — uses the trade's entry timeframe ──────────────────
            chart_3m = None
            try:
                entry_tf_key = op.get('timeframe', '5m') or '5m'
                raw3 = candle_cache.get(sym, entry_tf_key) or []
                if len(raw3) < 10:
                    raw3 = client.get_candles(sym, entry_tf_key, lookback=CANDLE_LOOKBACK) or []
                chart_3m = _build_chart_payload(raw3)
            except Exception:
                pass

            out.append({
                'symbol':     sym,
                'entry':      entry,
                'sl_price':   sl_price,
                'tp_price':   tp_price,
                'sl_pct':     sl_pct,
                'tp_pct':     tp_pct,
                'live_price': live_price,
                'amount':     round(amount, 8),
                'orig_fund':  orig_fund,
                'unreal_pnl': unreal_pnl,
                'unreal_pct': unreal_pct,
                'entry_time': op.get('buy_time', '—'),
                'entry_tf':   op.get('timeframe', '—'),
                'held_s':     held_s,
                'peak_price': op.get('peak_price'),
                'chart_3m':   chart_3m,
            })
        out.sort(key=lambda x: x.get('entry_ts', 0) if x.get('entry_ts') else 0, reverse=True)
    return jsonify(out)



@app.route('/api/chart_candles', methods=['GET'])
def chart_candles():
    """Return candle + EMA data for a given symbol and timeframe — used by live position TF chart switcher."""
    sym = (request.args.get('symbol') or '').upper().strip()
    tf  = (request.args.get('tf') or '5m').strip()
    if not sym:
        return jsonify({'error': 'symbol required'}), 400
    valid_tfs = ['1m','3m','5m','15m','30m','1h','2h','4h']
    if tf not in valid_tfs:
        tf = '5m'
    try:
        raw = candle_cache.get(sym, tf) or []
        if len(raw) < 10:
            # Try live fetch from backend
            raw = client.get_candles(sym, tf, lookback=CANDLE_LOOKBACK) or []
        payload = _build_chart_payload(raw)
        if not payload:
            return jsonify({'candles': [], 'ema9': [], 'ema21': []})
        payload['symbol'] = sym
        payload['tf'] = tf
        return jsonify(payload)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/vt/state', methods=['GET'])
def vt_state_debug():
    """Debug: show current _vt_fund state — which coins are stuck in position."""
    with _vt_lock:
        out = {}
        for sym, v in _vt_fund.items():
            out[sym] = {
                'buy_price': v.get('buy_price'),
                'entry_price': v.get('entry_price'),
                'peak_price': v.get('peak_price'),
                'fund': v.get('fund'),
                'timeframe': v.get('timeframe'),
                'buy_time': v.get('buy_time'),
                'in_position': bool(v.get('buy_price')),
            }
    return jsonify({'vt_fund': out, 'coins_in_position': [s for s,v in out.items() if v['in_position']]})

@app.route('/api/vt/fund', methods=['POST'])
def vt_fund_adjust():
    """Add or withdraw from a coin's VT fund. Body: {symbol, amount} — positive=add, negative=withdraw."""
    data   = request.get_json() or {}
    symbol = (data.get('symbol') or '').upper().strip()
    try:
        amount = float(data.get('amount', 0))
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'error': 'Invalid amount'}), 400

    if not symbol:
        return jsonify({'ok': False, 'error': 'Symbol required'}), 400

    with _vt_lock:
        if symbol not in _vt_fund:
            return jsonify({'ok': False, 'error': f'{symbol} not found in VT'}), 404
        current = _vt_fund[symbol].get('fund', 0)
        new_val  = round(current + amount, 6)
        if new_val <= 0:
            return jsonify({'ok': False, 'error': 'Fund cannot go to 0 or negative'}), 400
        _vt_fund[symbol]['fund'] = new_val
        # Also update initial_fund so growth% recalculates correctly
        _vt_fund[symbol]['initial_fund'] = new_val if not _vt_fund[symbol].get('buy_price') else _vt_fund[symbol].get('initial_fund', new_val)

    _save_vt_state()
    logger.info(f"[VT] Fund adjust {symbol}: {current:.4f} → {new_val:.4f} ({'+' if amount>=0 else ''}{amount})")
    return jsonify({'ok': True, 'symbol': symbol, 'old': current, 'new': new_val})

@app.route('/api/vt/close/<symbol>', methods=['POST'])
def vt_close_position(symbol):
    """Manually close a VT position at current market price."""
    symbol = symbol.upper().strip()
    with _vt_lock:
        entry = _vt_fund.get(symbol)
        if not entry or not entry.get('buy_price'):
            return jsonify({'ok': False, 'error': f'{symbol} not in VT position'}), 400
    # Get current price
    price = bot_engine.client.get_spot_price(symbol)
    if not price:
        return jsonify({'ok': False, 'error': f'Could not fetch price for {symbol}'}), 500
    _vt_on_sell(symbol, price, exit_reason='manual')
    logger.info(f"[VT] Manual close {symbol} @ {price}")
    return jsonify({'ok': True, 'symbol': symbol, 'close_price': price})

@app.route('/api/vt/settings', methods=['GET', 'POST'])
def vt_settings():
    """GET or SET global VT SL/TP/Trail percentages. POST body: {sl_pct, tp_pct, trail_pct}"""
    global VT_SL_PCT, VT_TP_PCT, VT_TRAIL_PCT
    if request.method == 'GET':
        return jsonify({'sl_pct': VT_SL_PCT, 'tp_pct': VT_TP_PCT, 'trail_pct': VT_TRAIL_PCT})
    data = request.get_json() or {}
    changed = []
    if 'sl_pct' in data:
        v = float(data['sl_pct'])
        if 0.1 <= v <= 10:
            VT_SL_PCT = round(v, 2); changed.append(f'SL={v}%')
        else:
            return jsonify({'ok': False, 'error': 'sl_pct must be 0.1–10'}), 400
    if 'tp_pct' in data:
        v = float(data['tp_pct'])
        if 0.1 <= v <= 10:
            VT_TP_PCT = round(v, 2); changed.append(f'TP={v}%')
        else:
            return jsonify({'ok': False, 'error': 'tp_pct must be 0.1–10'}), 400
    if 'trail_pct' in data:
        v = float(data['trail_pct'])
        if 0.1 <= v <= 10:
            VT_TRAIL_PCT = round(v, 2); changed.append(f'Trail={v}%')
        else:
            return jsonify({'ok': False, 'error': 'trail_pct must be 0.1–10'}), 400
    logger.info(f"[VT] Settings updated: {', '.join(changed)}")
    return jsonify({'ok': True, 'sl_pct': VT_SL_PCT, 'tp_pct': VT_TP_PCT, 'trail_pct': VT_TRAIL_PCT})

@app.route('/api/virtual/reset', methods=['POST'])
def virtual_reset():
    """Reset virtual P&L tracker (clears all virtual trades and stats)."""
    global _vt_fund, _vt_trades, _vt_stats
    with _vt_lock:
        _vt_fund.clear()
        _vt_trades.clear()
        _vt_stats.clear()
    # Also reset cycle state so next signal is counted fresh
    with _coin_signal_state_lock:
        _coin_signal_state.clear()
    # Clear from Supabase too
    if SUPABASE_OK:
        try:
            _supabase.table('bot_data').delete().eq('key', 'virtual_tracker').execute()
        except: pass
    return jsonify({'success': True, 'message': 'Virtual tracker reset'})

@app.route('/api/debug/btceth', methods=['GET'])
def debug_btceth():
    import requests as _req
    try:
        r = _req.post('https://api.hyperliquid.xyz/info',
                      json={'type': 'spotMetaAndAssetCtxs'}, timeout=10)
        data = r.json()
        meta, ctxs = data[0], data[1]
        result = {'universe_234': None, 'universe_235': None, 'ctx_hits': [], 'first_3_ctxs': ctxs[:3]}
        for u in meta.get('universe', []):
            if u.get('index') == 234: result['universe_234'] = u
            if u.get('index') == 235: result['universe_235'] = u
        # Find UBTC/UETH by name match in ctxs
        for i, c in enumerate(ctxs):
            coin = c.get('coin','')
            if coin in ['@234','@235','BTC/USDC','ETH/USDC','UBTC/USDC','UETH/USDC']:
                result['ctx_hits'].append({'array_pos': i, **c})
        # Also search universe array position for UBTC/UETH
        for pos, u in enumerate(meta.get('universe',[])):
            if u.get('name','') in ['UBTC/USDC','UETH/USDC']:
                ctx = ctxs[pos] if pos < len(ctxs) else None
                result['ctx_hits'].append({'by_position': pos, 'uni_name': u['name'], 'ctx': ctx})
        result['ws_cache_234'] = price_cache.get('@234')
        result['ws_cache_235'] = price_cache.get('@235')
        result['ws_cache_BTC'] = price_cache.get('BTC')
        result['ws_cache_ETH'] = price_cache.get('ETH')
        result['markpx_cache_btc'] = client._markpx_cache.get('BTC')
        result['markpx_cache_ubtc'] = client._markpx_cache.get('UBTC')
        result['markpx_cache_eth'] = client._markpx_cache.get('ETH')
        result['markpx_cache_ueth'] = client._markpx_cache.get('UETH')
        # Also fetch allMids directly
        try:
            r2 = _req.post('https://api.hyperliquid.xyz/info', json={'type': 'allMids'}, timeout=5)
            mids = r2.json()
            result['allMids_BTC'] = mids.get('BTC')
            result['allMids_ETH'] = mids.get('ETH')
            result['allMids_@234'] = mids.get('@234')
            result['allMids_@235'] = mids.get('@235')
        except Exception as e2:
            result['allMids_error'] = str(e2)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/timeframes', methods=['GET'])
def get_timeframes():
    active = get_active_tfs()
    return jsonify({'all': ALL_TIMEFRAMES, 'enabled': active})

@app.route('/api/timeframes', methods=['POST'])
def set_timeframes():
    data = request.get_json() or {}
    tfs = data.get('enabled', [])
    valid = [tf for tf in tfs if tf in ALL_TIMEFRAMES]
    if not valid:
        return jsonify({'error': 'No valid timeframes provided'}), 400
    with _enabled_tfs_lock:
        _enabled_tfs.clear()
        _enabled_tfs.update(valid)
    logger.info(f"⏱️ Timeframes updated: {get_active_tfs()}")
    return jsonify({'success': True, 'enabled': get_active_tfs()})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
