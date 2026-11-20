#!/usr/bin/env python3
"""
DS218play x402 Payment Server — v3 "agent-grade" crypto data.
Runs on Synology DS218play (keep memory footprint small).

What agents pay for is not raw passthrough (they can call CoinGecko themselves)
but *aggregated, cross-venue and derived* signals they would otherwise have to
stitch together. Feeds are tiered:

  basic    ($0.010)  single-source, lightly enriched snapshots
  premium  ($0.025)  cross-venue / multi-call aggregation + derived fields
  bundle   ($0.050)  composite regime read built from several feeds

Payment flow: verify -> build data -> settle -> respond.
Agents are only charged when data is actually delivered.
"""

import base64
import hashlib
import json
import os
import secrets
import statistics
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

PAY_TO = os.getenv("EVM_ADDRESS", "0xC9eebecb1d0AfF4fb2B9978516E075A33639892C")

# Public HTTPS origin agents reach you on (reverse proxy / tunnel). REQUIRED for discovery:
# indexers reject relative resource URLs.  e.g. https://signals.example.com
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

# Facilitator. CDP (Coinbase) is the only one that feeds the Coinbase Bazaar / agentic.market,
# and needs a CDP API key (Ed25519). Without keys we default to PayAI (mainnet, no key, has its own Bazaar).
# x402.org/facilitator is TESTNET ONLY — mainnet payments will never settle there.
CDP_API_KEY_ID = os.getenv("CDP_API_KEY_ID", "")
CDP_API_KEY_SECRET = os.getenv("CDP_API_KEY_SECRET", "")
CDP_FACILITATOR = "https://api.cdp.coinbase.com/platform/v2/x402"
PAYAI_FACILITATOR = "https://facilitator.payai.network"
FACILITATOR_BASE = os.getenv(
    "FACILITATOR_URL", CDP_FACILITATOR if (CDP_API_KEY_ID and CDP_API_KEY_SECRET) else PAYAI_FACILITATOR
).rstrip("/")

# Listing metadata shown in Bazaar / x402scan / agent catalogs
SERVICE_NAME = os.getenv("SERVICE_NAME", "NAS Crypto Signals")
# Bazaar accepts at most 5 tags (each <= 32 chars); more are silently dropped.
SERVICE_TAGS = [t.strip() for t in os.getenv(
    "SERVICE_TAGS", "crypto,market-data,defi,derivatives,trading-signals"
).split(",") if t.strip()][:5]
ICON_URL = os.getenv("ICON_URL", "")  # default: PUBLIC_BASE_URL/icon.png (served by this server)
# x402scan ownership proofs: EIP-191 signature(s) of PUBLIC_BASE_URL by the payTo key, comma-separated
OWNERSHIP_PROOFS = [p.strip() for p in os.getenv("OWNERSHIP_PROOFS", "").split(",") if p.strip()]
# Return the legacy v1 body on unpaid 402s (header is always v2). Only for old v1-only clients.
X402_V1_BODY = os.getenv("X402_V1_BODY", "0") == "1"

PRICE_USDC_ATOMIC = os.getenv("PRICE_USDC_ATOMIC", "10000")                  # $0.010 basic
PREMIUM_PRICE_USDC_ATOMIC = os.getenv("PREMIUM_PRICE_USDC_ATOMIC", "25000")  # $0.025 premium
BUNDLE_PRICE_USDC_ATOMIC = os.getenv("BUNDLE_PRICE_USDC_ATOMIC", "50000")    # $0.050 bundle

PORT = int(os.getenv("PORT", "8402"))
NETWORK = "eip155:8453"
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
VERSION = "3.1.1"

EVM_RPC = os.getenv("EVM_RPC_URL", "https://eth.llamarpc.com")
BASE_RPC = os.getenv("BASE_RPC_URL", "https://mainnet.base.org")
SUI_RPC = os.getenv("SUI_RPC_URL", "https://fullnode.mainnet.sui.io:443")
MEMPOOL_API = os.getenv("MEMPOOL_API", "https://mempool.space/api").rstrip("/")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")  # optional free "demo" key

# Yields feed parses a large JSON (~10+ MB). Set to 0 if the NAS gets tight on RAM.
ENABLE_HEAVY_FEEDS = os.getenv("ENABLE_HEAVY_FEEDS", "1") == "1"
# If an upstream fails, serve the last good copy for up to this long past its TTL.
MAX_STALE_SEC = int(os.getenv("MAX_STALE_SEC", "900"))
CACHE_MAX_ENTRIES = 24

MAJORS = [
    "BTC", "ETH", "SOL", "SUI", "BNB", "XRP", "ADA", "AVAX", "LINK", "DOGE",
    "OP", "ARB", "APT", "NEAR", "INJ",
]
POSITIONING_MAJORS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "SUI"]
UA = "DS218play-x402/3.0"

# ─────────────────────────────────────────────────────────────
# HTTP + SMALL HELPERS
# ─────────────────────────────────────────────────────────────


def _http_get_json(url: str, timeout: int = 8, headers: Optional[Dict[str, str]] = None) -> Any:
    h = {"Accept": "application/json", "User-Agent": UA}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_post(url: str, body: Dict, timeout: int = 10,
               headers: Optional[Dict[str, str]] = None) -> Tuple[Any, Dict[str, str]]:
    """POST JSON, return (json, response_headers). 4xx/5xx with a JSON body are returned, not raised."""
    raw = json.dumps(body).encode("utf-8")
    h = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": UA}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=raw, headers=h, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8") or "{}"), {k.upper(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", "replace")
        try:
            return json.loads(text), {k.upper(): v for k, v in exc.headers.items()}
        except ValueError:
            raise RuntimeError("HTTP {} from {}: {}".format(exc.code, url, text[:200]))


def _http_post_json(url: str, body: Dict, timeout: int = 10) -> Any:
    raw = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=raw,
        headers={"Content-Type": "application/json", "User-Agent": UA},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _cg_get(path: str, timeout: int = 10) -> Any:
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else None
    return _http_get_json("https://api.coingecko.com/api/v3" + path, timeout=timeout, headers=headers)


def _rpc(url: str, method: str, params: List, timeout: int = 8) -> Any:
    res = _http_post_json(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=timeout)
    if "error" in res:
        raise RuntimeError("{} {}: {}".format(url, method, res["error"]))
    return res.get("result")


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None or x == "" else float(x)
    except (TypeError, ValueError):
        return None


def _pct_change(new: Optional[float], old: Optional[float], nd: int = 2) -> Optional[float]:
    if new is None or not old:
        return None
    return round((new - old) / old * 100, nd)


def _clamp(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _annualize(rate: float, interval_h: float) -> float:
    return round(rate * (24.0 / interval_h) * 365 * 100, 2)


def _parallel(fns: Dict[str, Callable[[], Any]], workers: int = 6) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Run independent fetches concurrently; return (results, errors) keyed by name."""
    results: Dict[str, Any] = {}
    errors: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(workers, max(1, len(fns)))) as ex:
        futs = {name: ex.submit(fn) for name, fn in fns.items()}
        for name, fut in futs.items():
            try:
                results[name] = fut.result()
            except Exception as exc:
                errors[name] = str(exc)[:200]
    return results, errors


# ─────────────────────────────────────────────────────────────
# CACHE (per-key build lock + stale fallback)
# ─────────────────────────────────────────────────────────────

_CACHE_LOCK = threading.Lock()
_CACHE: Dict[str, Dict[str, Any]] = {}
_BUILD_LOCKS: Dict[str, threading.Lock] = {}


def _build_lock(key: str) -> threading.Lock:
    with _CACHE_LOCK:
        lock = _BUILD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _BUILD_LOCKS[key] = lock
        return lock


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    with _CACHE_LOCK:
        return _CACHE.get(key)


def _cached(key: str, ttl_sec: int, builder_fn: Callable[[], Dict]) -> Tuple[Dict, Dict]:
    """Return (data, meta). Only one thread rebuilds a key at a time, so a burst of
    paying agents never triggers N identical upstream calls (protects RAM + rate limits)."""
    entry = _cache_get(key)
    now = time.time()
    if entry is not None and now - entry["ts"] < ttl_sec:
        return entry["data"], {"age_sec": int(now - entry["ts"]), "stale": False}

    with _build_lock(key):
        entry = _cache_get(key)  # someone may have refreshed while we waited
        now = time.time()
        if entry is not None and now - entry["ts"] < ttl_sec:
            return entry["data"], {"age_sec": int(now - entry["ts"]), "stale": False}
        try:
            fresh = builder_fn()
        except Exception:
            if entry is not None and now - entry["ts"] < ttl_sec + MAX_STALE_SEC:
                return entry["data"], {"age_sec": int(now - entry["ts"]), "stale": True}
            raise
        with _CACHE_LOCK:
            _CACHE[key] = {"ts": now, "data": fresh}
            while len(_CACHE) > CACHE_MAX_ENTRIES:
                oldest = min(_CACHE.items(), key=lambda x: x[1]["ts"])[0]
                _CACHE.pop(oldest, None)
        return fresh, {"age_sec": 0, "stale": False}


# ─────────────────────────────────────────────────────────────
# BASIC FEEDS
# ─────────────────────────────────────────────────────────────

def build_market_signal() -> Dict:
    """Top liquid coins + momentum + derived positioning fields."""
    raw = _cg_get(
        "/coins/markets?vs_currency=usd"
        "&ids=bitcoin,ethereum,solana,sui,binancecoin,ripple,cardano,avalanche-2,chainlink,dogecoin,"
        "hyperliquid,tron"
        "&order=market_cap_desc&per_page=25&page=1&sparkline=false"
        "&price_change_percentage=1h,24h,7d,30d"
    )

    assets = {}
    for c in raw:
        symbol = (c.get("symbol") or "").upper()
        price, hi, lo = _f(c.get("current_price")), _f(c.get("high_24h")), _f(c.get("low_24h"))
        mcap, vol, ath = _f(c.get("market_cap")), _f(c.get("total_volume")), _f(c.get("ath"))
        range_pos = None
        if price is not None and hi is not None and lo is not None and hi > lo:
            range_pos = round((price - lo) / (hi - lo), 3)
        assets[symbol] = {
            "id": c.get("id"),
            "name": c.get("name"),
            "usd": price,
            "market_cap": mcap,
            "market_cap_rank": c.get("market_cap_rank"),
            "volume_24h": vol,
            "high_24h": hi,
            "low_24h": lo,
            "change_1h_pct": c.get("price_change_percentage_1h_in_currency"),
            "change_24h_pct": c.get("price_change_percentage_24h_in_currency") or c.get("price_change_percentage_24h"),
            "change_7d_pct": c.get("price_change_percentage_7d_in_currency"),
            "change_30d_pct": c.get("price_change_percentage_30d_in_currency"),
            "ath": ath,
            "atl": c.get("atl"),
            # Derived
            "pct_from_ath": _pct_change(price, ath),
            "range_position_24h": range_pos,  # 0 = at 24h low, 1 = at 24h high
            "volume_to_mcap": round(vol / mcap, 4) if vol and mcap else None,  # turnover / attention
        }

    changes = [a["change_24h_pct"] for a in assets.values() if a["change_24h_pct"] is not None]
    return {
        "feed": "market_signal",
        "source": "coingecko",
        "assets": assets,
        "breadth": {
            "up_24h": sum(1 for x in changes if x > 0),
            "down_24h": sum(1 for x in changes if x <= 0),
            "avg_change_24h_pct": round(sum(changes) / len(changes), 2) if changes else None,
        },
        "note": "Multi-timeframe momentum, distance from ATH, 24h range position, turnover, breadth.",
    }


def _binance_funding_intervals() -> Dict[str, int]:
    """Binance moved some perps to 4h/1h funding; annualizing them as 8h is wrong."""
    try:
        info = _http_get_json("https://fapi.binance.com/fapi/v1/fundingInfo", timeout=6)
        return {i["symbol"]: int(i.get("fundingIntervalHours") or 8) for i in info if i.get("symbol")}
    except Exception:
        return {}


def build_funding_rates() -> Dict:
    """Binance perpetual funding + basis. Sorted by extremity."""
    raw = _http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=8)
    intervals = _binance_funding_intervals()
    wanted = {b + "USDT" for b in MAJORS}

    rates = {}
    for item in raw:
        sym = item.get("symbol")
        if sym not in wanted:
            continue
        rate = _f(item.get("lastFundingRate")) or 0.0
        mark, index = _f(item.get("markPrice")) or 0.0, _f(item.get("indexPrice")) or 0.0
        h = intervals.get(sym, 8)
        rates[sym] = {
            "funding_rate": rate,
            "funding_rate_pct": round(rate * 100, 6),
            "funding_interval_h": h,
            "annualized_pct": _annualize(rate, h),
            "mark_price": mark,
            "index_price": index,
            "basis_bps": round((mark - index) / index * 10000, 2) if index else None,
            "next_funding_time": int(item.get("nextFundingTime") or 0),
        }

    sorted_rates = dict(sorted(rates.items(), key=lambda x: abs(x[1]["annualized_pct"]), reverse=True))
    return {
        "feed": "funding_rates",
        "source": "binance_futures",
        "rates": sorted_rates,
        "note": "Positive = longs pay shorts (crowded long). Annualized uses each pair's real funding interval.",
    }


def build_fear_greed() -> Dict:
    """Crypto Fear & Greed Index + regime tag + short-term trend."""
    raw = _http_get_json("https://api.alternative.me/fng/?limit=7&format=json", timeout=8)
    data = raw.get("data") or []
    history = [
        {
            "value": int(d.get("value") or 0),
            "classification": d.get("value_classification"),
            "timestamp": int(d.get("timestamp") or 0),
        }
        for d in data
    ]
    current = history[0] if history else {"value": 0, "classification": "Unknown"}
    value = current["value"]

    if value <= 25:
        regime = "extreme_fear"
    elif value <= 45:
        regime = "fear"
    elif value <= 55:
        regime = "neutral"
    elif value <= 75:
        regime = "greed"
    else:
        regime = "extreme_greed"

    vals = [h["value"] for h in history]
    return {
        "feed": "fear_greed",
        "source": "alternative.me",
        "value": value,
        "classification": current["classification"],
        "regime": regime,
        "change_1d": value - vals[1] if len(vals) > 1 else None,
        "avg_7d": round(sum(vals) / len(vals), 1) if vals else None,
        "history_7d": history,
        "note": "0 = Extreme Fear, 100 = Extreme Greed. Useful as a market regime filter.",
    }


def build_defi_tvl() -> Dict:
    """Chain-level DeFi TVL + top protocols."""
    chains_raw = _http_get_json("https://api.llama.fi/v2/chains", timeout=10)
    chains_sorted = sorted(
        [c for c in chains_raw if c.get("tvl")],
        key=lambda x: float(x.get("tvl") or 0),
        reverse=True,
    )[:15]
    chains = {}
    for c in chains_sorted:
        name = c.get("name") or c.get("gecko_id") or "unknown"
        chains[name] = {
            "tvl_usd": round(float(c.get("tvl") or 0), 0),
            "token_symbol": c.get("tokenSymbol"),
            "chain_id": c.get("chainId"),
        }

    protocols_raw = _http_get_json("https://api.llama.fi/protocols", timeout=12)
    top_protocols = []
    for p in sorted(protocols_raw, key=lambda x: float(x.get("tvl") or 0), reverse=True)[:12]:
        top_protocols.append({
            "name": p.get("name"),
            "slug": p.get("slug"),
            "tvl_usd": round(float(p.get("tvl") or 0), 0),
            "category": p.get("category"),
            "change_1d": p.get("change_1d"),
            "change_7d": p.get("change_7d"),
            "chains": (p.get("chains") or [])[:4],
        })
    del protocols_raw  # large; free early on the NAS

    return {
        "feed": "defi_tvl",
        "source": "defillama",
        "chains": chains,
        "top_protocols": top_protocols,
        "note": "Chain TVL + top protocols. Agents use for sector allocation and risk regime.",
    }


def build_global_market() -> Dict:
    """Total mcap, volume, dominance (incl. stablecoin dominance)."""
    raw = _cg_get("/global", timeout=8)
    data = raw.get("data") or {}
    dom = data.get("market_cap_percentage") or {}
    stable_dom = sum(_f(dom.get(k)) or 0 for k in ("usdt", "usdc", "dai", "usde"))
    return {
        "feed": "global_market",
        "source": "coingecko",
        "active_cryptocurrencies": data.get("active_cryptocurrencies"),
        "markets": data.get("markets"),
        "total_market_cap_usd": (data.get("total_market_cap") or {}).get("usd"),
        "total_volume_24h_usd": (data.get("total_volume") or {}).get("usd"),
        "market_cap_percentage": dom,
        "btc_dominance_pct": round(_f(dom.get("btc")) or 0, 2),
        "eth_dominance_pct": round(_f(dom.get("eth")) or 0, 2),
        "stablecoin_dominance_pct": round(stable_dom, 2),  # rising = risk-off / cash on sidelines
        "market_cap_change_24h_pct": data.get("market_cap_change_percentage_24h_usd"),
        "updated_at": data.get("updated_at"),
        "note": "BTC + stablecoin dominance and total mcap are classic regime inputs.",
    }


def _evm_gas(rpc_url: str) -> Dict:
    block = int(_rpc(rpc_url, "eth_blockNumber", []) or "0x0", 16)
    gas_gwei = round(int(_rpc(rpc_url, "eth_gasPrice", []) or "0x0", 16) / 1e9, 4)
    out: Dict[str, Any] = {"latest_block": block, "gas_price_gwei": gas_gwei}
    try:
        fh = _rpc(rpc_url, "eth_feeHistory", ["0x5", "latest", [10, 50, 90]]) or {}
        base_fees = fh.get("baseFeePerGas") or []
        rewards = fh.get("reward") or []
        if base_fees:
            out["next_base_fee_gwei"] = round(int(base_fees[-1], 16) / 1e9, 4)
        if rewards:
            for i, label in enumerate(("low", "medium", "high")):
                col = [int(r[i], 16) for r in rewards if len(r) > i]
                if col:
                    out["priority_fee_{}_gwei".format(label)] = round(statistics.median(col) / 1e9, 4)
        ratios = fh.get("gasUsedRatio") or []
        if ratios:
            out["avg_block_utilization"] = round(sum(ratios) / len(ratios), 3)
    except Exception:
        pass
    return out


def build_gas_pulse() -> Dict:
    """Ethereum (EIP-1559 detail) + Base + Sui gas for execution agents."""
    res, errs = _parallel({
        "evm": lambda: _evm_gas(EVM_RPC),
        "base": lambda: _evm_gas(BASE_RPC),
        "sui": lambda: _rpc(SUI_RPC, "suix_getReferenceGasPrice", []),
    }, workers=3)
    if "evm" not in res:
        raise RuntimeError("Ethereum RPC failed: {}".format(errs.get("evm")))
    return {
        "feed": "gas_pulse",
        "evm": res["evm"],
        "base": res.get("base"),
        "sui": {"reference_gas_price_mist": res.get("sui")},
        "errors": errs or None,
    }


def build_btc_network() -> Dict:
    """Bitcoin fee market, mempool backlog, difficulty adjustment, hashrate."""
    res, errs = _parallel({
        "fees": lambda: _http_get_json(MEMPOOL_API + "/v1/fees/recommended"),
        "mempool": lambda: _http_get_json(MEMPOOL_API + "/mempool"),
        "difficulty": lambda: _http_get_json(MEMPOOL_API + "/v1/difficulty-adjustment"),
        "hashrate": lambda: _http_get_json(MEMPOOL_API + "/v1/mining/hashrate/3d"),
    }, workers=4)
    if "fees" not in res:
        raise RuntimeError("mempool.space fees failed: {}".format(errs.get("fees")))

    fees, mp = res["fees"], res.get("mempool") or {}
    diff, hr = res.get("difficulty") or {}, res.get("hashrate") or {}
    vsize = _f(mp.get("vsize"))
    total_fee = _f(mp.get("total_fee"))
    hashrate = _f(hr.get("currentHashrate"))
    return {
        "feed": "btc_network",
        "source": "mempool.space",
        "fees_sat_vb": {
            "fastest": fees.get("fastestFee"),
            "half_hour": fees.get("halfHourFee"),
            "hour": fees.get("hourFee"),
            "economy": fees.get("economyFee"),
            "minimum": fees.get("minimumFee"),
        },
        "mempool": {
            "tx_count": mp.get("count"),
            "vsize_vb": vsize,
            "blocks_to_clear": round(vsize / 1_000_000, 1) if vsize else None,
            "total_fees_btc": round(total_fee / 1e8, 4) if total_fee else None,
        },
        "difficulty": {
            "epoch_progress_pct": diff.get("progressPercent"),
            "estimated_change_pct": diff.get("difficultyChange"),
            "remaining_blocks": diff.get("remainingBlocks"),
            "estimated_retarget_ms": diff.get("estimatedRetargetDate"),
        },
        "hashrate_ehs": round(hashrate / 1e18, 2) if hashrate else None,
        "errors": errs or None,
    }


def build_trending() -> Dict:
    """What retail is searching for right now — narrative / attention signal."""
    raw = _cg_get("/search/trending", timeout=8)
    coins = []
    for c in (raw.get("coins") or [])[:15]:
        it = c.get("item") or {}
        d = it.get("data") or {}
        coins.append({
            "id": it.get("id"),
            "symbol": (it.get("symbol") or "").upper(),
            "name": it.get("name"),
            "market_cap_rank": it.get("market_cap_rank"),
            "price_usd": _f(d.get("price")),
            "change_24h_pct": _f((d.get("price_change_percentage_24h") or {}).get("usd")),
            "market_cap": d.get("market_cap"),
            "volume_24h": d.get("total_volume"),
        })
    categories = []
    for cat in (raw.get("categories") or [])[:8]:
        d = cat.get("data") or {}
        categories.append({
            "name": cat.get("name"),
            "market_cap_change_1h_pct": _f(cat.get("market_cap_1h_change")),
            "market_cap_change_24h_pct": _f((d.get("market_cap_change_percentage_24h") or {}).get("usd")),
        })
    return {
        "feed": "trending",
        "source": "coingecko",
        "coins": coins,
        "categories": categories,
        "note": "Most-searched coins and hottest categories. Attention often leads flows.",
    }


# ─────────────────────────────────────────────────────────────
# PREMIUM FEEDS (cross-venue / multi-call / derived)
# ─────────────────────────────────────────────────────────────

def _venue_binance() -> Dict[str, Dict]:
    raw = _http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=8)
    intervals = _binance_funding_intervals()
    wanted = {b + "USDT": b for b in MAJORS}
    out = {}
    for item in raw:
        base = wanted.get(item.get("symbol"))
        if not base:
            continue
        rate = _f(item.get("lastFundingRate")) or 0.0
        h = intervals.get(item["symbol"], 8)
        out[base] = {
            "funding_rate": rate,
            "interval_h": h,
            "annualized_pct": _annualize(rate, h),
            "mark_price": _f(item.get("markPrice")),
        }
    return out


def _venue_bybit() -> Dict[str, Dict]:
    raw = _http_get_json("https://api.bybit.com/v5/market/tickers?category=linear", timeout=8)
    wanted = {b + "USDT": b for b in MAJORS}
    out = {}
    for item in (raw.get("result") or {}).get("list") or []:
        base = wanted.get(item.get("symbol"))
        if not base:
            continue
        rate = _f(item.get("fundingRate")) or 0.0
        h = int(_f(item.get("fundingIntervalHour")) or 8)
        out[base] = {
            "funding_rate": rate,
            "interval_h": h,
            "annualized_pct": _annualize(rate, h),
            "mark_price": _f(item.get("markPrice")),
            "open_interest_usd": _f(item.get("openInterestValue")),
            "volume_24h_usd": _f(item.get("turnover24h")),
        }
    return out


def _venue_hyperliquid() -> Dict[str, Dict]:
    raw = _http_post_json("https://api.hyperliquid.xyz/info", {"type": "metaAndAssetCtxs"}, timeout=8)
    universe = (raw[0] or {}).get("universe") or []
    ctxs = raw[1] or []
    wanted = set(MAJORS)
    out = {}
    for meta, ctx in zip(universe, ctxs):
        base = meta.get("name")
        if base not in wanted:
            continue
        rate = _f(ctx.get("funding")) or 0.0
        mark = _f(ctx.get("markPx"))
        oi = _f(ctx.get("openInterest"))
        out[base] = {
            "funding_rate": rate,
            "interval_h": 1,
            "annualized_pct": _annualize(rate, 1),
            "mark_price": mark,
            "open_interest_usd": round(oi * mark, 0) if oi is not None and mark else None,
            "volume_24h_usd": _f(ctx.get("dayNtlVlm")),
        }
    return out


def build_funding_arb() -> Dict:
    """Same asset, three venues: where is funding cheapest / richest right now?"""
    venues, errors = _parallel({
        "binance": _venue_binance,
        "bybit": _venue_bybit,
        "hyperliquid": _venue_hyperliquid,
    }, workers=3)
    if not venues:
        raise RuntimeError("All funding venues failed: {}".format(errors))

    assets: Dict[str, Dict] = {}
    for base in MAJORS:
        per_venue = {v: d[base] for v, d in venues.items() if base in d}
        if not per_venue:
            continue
        entry: Dict[str, Any] = {"venues": per_venue}
        ois = [x.get("open_interest_usd") for x in per_venue.values() if x.get("open_interest_usd")]
        entry["open_interest_usd_tracked"] = round(sum(ois), 0) if ois else None
        anns = {v: x["annualized_pct"] for v, x in per_venue.items()}
        entry["avg_annualized_pct"] = round(sum(anns.values()) / len(anns), 2)
        if len(anns) >= 2:
            lo_v = min(anns, key=anns.get)
            hi_v = max(anns, key=anns.get)
            entry["spread_annualized_pct"] = round(anns[hi_v] - anns[lo_v], 2)
            entry["long_on"] = lo_v    # pay / receive the least as a long
            entry["short_on"] = hi_v   # collect the most as a short
        assets[base] = entry

    opps = sorted(
        [
            {"asset": a, "spread_annualized_pct": e["spread_annualized_pct"],
             "long_on": e["long_on"], "short_on": e["short_on"]}
            for a, e in assets.items() if "spread_annualized_pct" in e
        ],
        key=lambda x: x["spread_annualized_pct"],
        reverse=True,
    )
    return {
        "feed": "funding_arb",
        "sources": sorted(venues.keys()),
        "venue_errors": errors or None,
        "assets": assets,
        "top_opportunities": opps[:5],
        "note": ("Gross annualized funding spread between venues, normalized for each venue's interval "
                 "(Hyperliquid = 1h). Excludes fees, slippage, margin cost and funding changes."),
    }


def _binance_positioning(base: str) -> Dict:
    sym = base + "USDT"
    root = "https://fapi.binance.com/futures/data"

    def last(path: str, limit: int = 1) -> List[Dict]:
        rows = _http_get_json("{}/{}?symbol={}&period=1h&limit={}".format(root, path, sym, limit), timeout=6)
        return sorted(rows or [], key=lambda r: int(r.get("timestamp") or 0))

    g = last("globalLongShortAccountRatio")
    t = last("topLongShortPositionRatio")
    tk = last("takerlongshortRatio")
    oi = last("openInterestHist", limit=25)

    out: Dict[str, Any] = {}
    if g:
        out["global_long_short_ratio"] = _f(g[-1].get("longShortRatio"))
        out["global_long_pct"] = round((_f(g[-1].get("longAccount")) or 0) * 100, 2)
    if t:
        out["top_trader_long_short_ratio"] = _f(t[-1].get("longShortRatio"))
    if tk:
        r = _f(tk[-1].get("buySellRatio"))
        out["taker_buy_sell_ratio_1h"] = r
        if r is not None:
            out["taker_bias"] = "buy" if r > 1.05 else "sell" if r < 0.95 else "neutral"
    if len(oi) >= 2:
        v_now, v_old = _f(oi[-1].get("sumOpenInterestValue")), _f(oi[0].get("sumOpenInterestValue"))
        c_now, c_old = _f(oi[-1].get("sumOpenInterest")), _f(oi[0].get("sumOpenInterest"))
        out["open_interest_usd"] = round(v_now, 0) if v_now else None
        out["oi_change_24h_pct"] = _pct_change(v_now, v_old)
        contracts_chg = _pct_change(c_now, c_old)
        out["oi_contracts_change_24h_pct"] = contracts_chg  # strips out price effect
        if contracts_chg is not None:
            out["oi_signal"] = (
                "leverage_building" if contracts_chg > 3
                else "deleveraging" if contracts_chg < -3
                else "stable"
            )
    # Retail vs smart-money divergence: retail long-heavy while top traders are not
    g_r, t_r = out.get("global_long_short_ratio"), out.get("top_trader_long_short_ratio")
    if g_r and t_r:
        out["retail_vs_top_divergence"] = round(g_r - t_r, 3)
    return out


def build_positioning() -> Dict:
    """Who is long, who is short, and is leverage building — Binance futures."""
    res, errs = _parallel({b: (lambda b=b: _binance_positioning(b)) for b in POSITIONING_MAJORS}, workers=6)
    if not res:
        raise RuntimeError("Positioning fetch failed: {}".format(errs))
    return {
        "feed": "positioning",
        "source": "binance_futures",
        "period": "1h",
        "assets": res,
        "errors": errs or None,
        "note": ("Long/short ratios (all accounts vs top traders), taker flow, 24h OI change. "
                 "oi_contracts_change strips price moves so it reflects new leverage."),
    }


def build_stablecoins() -> Dict:
    """Stablecoin supply = dry powder entering/leaving crypto. Plus depeg alerts."""
    raw = _http_get_json("https://stablecoins.llama.fi/stablecoins?includePrices=true", timeout=12)
    rows = []
    tot = {"now": 0.0, "d": 0.0, "w": 0.0, "m": 0.0}
    for a in raw.get("peggedAssets") or []:
        if a.get("pegType") != "peggedUSD":
            continue
        cur = _f((a.get("circulating") or {}).get("peggedUSD")) or 0.0
        d = _f((a.get("circulatingPrevDay") or {}).get("peggedUSD"))
        w = _f((a.get("circulatingPrevWeek") or {}).get("peggedUSD"))
        m = _f((a.get("circulatingPrevMonth") or {}).get("peggedUSD"))
        d, w, m = (cur if d is None else d), (cur if w is None else w), (cur if m is None else m)
        tot["now"] += cur
        tot["d"] += d
        tot["w"] += w
        tot["m"] += m
        rows.append({
            "symbol": a.get("symbol"),
            "name": a.get("name"),
            "mechanism": a.get("pegMechanism"),
            "supply_usd": round(cur, 0),
            "change_1d_pct": _pct_change(cur, d),
            "change_7d_pct": _pct_change(cur, w),
            "change_30d_pct": _pct_change(cur, m),
            "price": _f(a.get("price")),
        })
    del raw

    rows.sort(key=lambda r: r["supply_usd"], reverse=True)
    depegs = [
        {"symbol": r["symbol"], "price": r["price"], "supply_usd": r["supply_usd"],
         "deviation_pct": round((r["price"] - 1) * 100, 3)}
        for r in rows
        if r["supply_usd"] >= 100_000_000 and r["price"] is not None and abs(r["price"] - 1) > 0.005
    ]
    return {
        "feed": "stablecoins",
        "source": "defillama",
        "total_supply_usd": round(tot["now"], 0),
        "change_1d_usd": round(tot["now"] - tot["d"], 0),
        "change_7d_usd": round(tot["now"] - tot["w"], 0),
        "change_7d_pct": _pct_change(tot["now"], tot["w"], 3),
        "change_30d_pct": _pct_change(tot["now"], tot["m"], 3),
        "top": rows[:12],
        "depeg_alerts": depegs,
        "note": "Growing stablecoin supply = fresh liquidity. Depeg alert = >0.5% off peg with >$100M supply.",
    }


def _llama_overview(kind: str, data_type: str) -> Dict:
    return _http_get_json(
        "https://api.llama.fi/overview/{}?excludeTotalDataChart=true"
        "&excludeTotalDataChartBreakdown=true&dataType={}".format(kind, data_type),
        timeout=12,
    )


def build_dex_volume() -> Dict:
    """Onchain trading activity — total DEX volume + leaders."""
    raw = _llama_overview("dexs", "dailyVolume")
    protos = [p for p in raw.get("protocols") or [] if _f(p.get("total24h"))]
    protos.sort(key=lambda p: _f(p.get("total24h")) or 0, reverse=True)
    total24 = _f(raw.get("total24h")) or 0
    top = [{
        "name": p.get("displayName") or p.get("name"),
        "volume_24h_usd": round(_f(p.get("total24h")) or 0, 0),
        "share_pct": round((_f(p.get("total24h")) or 0) / total24 * 100, 2) if total24 else None,
        "change_1d_pct": _f(p.get("change_1d")),
        "change_7d_pct": _f(p.get("change_7d")),
        "chains": (p.get("chains") or [])[:4],
    } for p in protos[:12]]
    return {
        "feed": "dex_volume",
        "source": "defillama",
        "total_24h_usd": round(total24, 0),
        "total_7d_usd": _f(raw.get("total7d")),
        "change_1d_pct": _f(raw.get("change_1d")),
        "change_7d_pct": _f(raw.get("change_7d")),
        "change_30d_pct": _f(raw.get("change_1m")),
        "top_protocols": top,
    }


def build_fees_revenue() -> Dict:
    """Which protocols actually make money — fundamentals for token agents."""
    res, errs = _parallel({
        "fees": lambda: _llama_overview("fees", "dailyFees"),
        "revenue": lambda: _llama_overview("fees", "dailyRevenue"),
    }, workers=2)
    if "fees" not in res:
        raise RuntimeError("DefiLlama fees failed: {}".format(errs.get("fees")))
    rev_by_name = {
        p.get("name"): _f(p.get("total24h"))
        for p in (res.get("revenue") or {}).get("protocols") or []
    }
    protos = [p for p in res["fees"].get("protocols") or [] if _f(p.get("total24h"))]
    protos.sort(key=lambda p: _f(p.get("total24h")) or 0, reverse=True)
    top = []
    for p in protos[:15]:
        fees24 = _f(p.get("total24h")) or 0
        rev24 = rev_by_name.get(p.get("name"))
        top.append({
            "name": p.get("displayName") or p.get("name"),
            "category": p.get("category"),
            "fees_24h_usd": round(fees24, 0),
            "revenue_24h_usd": round(rev24, 0) if rev24 is not None else None,
            "take_rate_pct": round(rev24 / fees24 * 100, 1) if rev24 is not None and fees24 else None,
            "fees_7d_usd": _f(p.get("total7d")),
            "change_7d_pct": _f(p.get("change_7d")),
        })
    return {
        "feed": "fees_revenue",
        "source": "defillama",
        "total_fees_24h_usd": _f(res["fees"].get("total24h")),
        "total_revenue_24h_usd": _f((res.get("revenue") or {}).get("total24h")),
        "top_protocols": top,
        "errors": errs or None,
        "note": "Fees = paid by users. Revenue = kept by protocol/holders. take_rate = revenue / fees.",
    }


def build_yields() -> Dict:
    """Best stablecoin yields in pools with real size, with spike detection (heavy feed)."""
    raw = _http_get_json("https://yields.llama.fi/pools", timeout=25)
    pools = raw.get("data") or []
    del raw

    stable, large = [], []
    for p in pools:
        tvl = _f(p.get("tvlUsd")) or 0
        apy = _f(p.get("apy"))
        if tvl < 10_000_000 or apy is None or apy <= 0 or apy > 200:
            continue
        mean30 = _f(p.get("apyMean30d"))
        row = {
            "pool_id": p.get("pool"),
            "project": p.get("project"),
            "chain": p.get("chain"),
            "symbol": p.get("symbol"),
            "tvl_usd": round(tvl, 0),
            "apy": round(apy, 2),
            "apy_base": _f(p.get("apyBase")),
            "apy_reward": _f(p.get("apyReward")),
            "apy_mean_30d": round(mean30, 2) if mean30 is not None else None,
            "apy_change_7d": _f(p.get("apyPct7D")),
            "il_risk": p.get("ilRisk"),
            # Spike = current APY > 2x its 30d mean; often temporary incentives
            "spike": bool(mean30 and apy > 2 * mean30),
        }
        if p.get("stablecoin"):
            stable.append(row)
        large.append(row)
    del pools

    stable.sort(key=lambda r: r["apy"], reverse=True)
    large.sort(key=lambda r: r["tvl_usd"], reverse=True)
    sustained = [r for r in stable if not r["spike"]]
    return {
        "feed": "yields",
        "source": "defillama",
        "filters": "tvl >= $10M, 0 < apy <= 200%",
        "top_stablecoin_yields": stable[:15],
        "top_sustained_stablecoin_yields": sustained[:10],
        "largest_pools": large[:10],
        "note": "spike=true means APY is >2x its 30-day mean (often temporary rewards). Not risk-rated.",
    }


# ─────────────────────────────────────────────────────────────
# BUNDLE: market regime (composite of other feeds, shares their cache)
# ─────────────────────────────────────────────────────────────

def build_regime() -> Dict:
    """One call -> a compact risk-on / risk-off read with the reasons behind it."""
    keys = ["fear_greed", "global_market", "market_signal", "funding_rates", "stablecoins"]

    def get(key: str) -> Tuple[Dict, Dict]:
        f = FEEDS_BY_KEY[key]
        return _cached(key, f["ttl"], f["builder"])

    res, errs = _parallel({k: (lambda k=k: get(k)) for k in keys}, workers=5)
    if len(res) < 2:
        raise RuntimeError("Not enough inputs for regime: {}".format(errs))
    data = {k: v[0] for k, v in res.items()}
    ages = {k: v[1]["age_sec"] for k, v in res.items()}

    comps: Dict[str, float] = {}
    flags: List[str] = []
    inputs: Dict[str, Any] = {}

    fg = data.get("fear_greed")
    if fg:
        comps["sentiment"] = round(_clamp((fg["value"] - 50) / 50.0), 3)
        inputs["fear_greed"] = fg["value"]
        if fg["value"] <= 20:
            flags.append("extreme_fear_contrarian_watch")
        elif fg["value"] >= 80:
            flags.append("extreme_greed_froth")

    ms = data.get("market_signal")
    if ms:
        btc = ms["assets"].get("BTC") or {}
        if btc.get("change_24h_pct") is not None:
            comps["btc_momentum"] = round(_clamp(btc["change_24h_pct"] / 5.0), 3)
            inputs["btc_change_24h_pct"] = btc["change_24h_pct"]
        b = ms.get("breadth") or {}
        n = (b.get("up_24h") or 0) + (b.get("down_24h") or 0)
        if n:
            comps["breadth"] = round((b["up_24h"] - b["down_24h"]) / float(n), 3)
            inputs["breadth_up_down"] = "{}/{}".format(b["up_24h"], b["down_24h"])

    gm = data.get("global_market")
    if gm and gm.get("market_cap_change_24h_pct") is not None:
        comps["total_mcap"] = round(_clamp(gm["market_cap_change_24h_pct"] / 5.0), 3)
        inputs["total_mcap_change_24h_pct"] = round(gm["market_cap_change_24h_pct"], 2)
        inputs["btc_dominance_pct"] = gm.get("btc_dominance_pct")

    sc = data.get("stablecoins")
    if sc and sc.get("change_7d_pct") is not None:
        comps["liquidity"] = round(_clamp(sc["change_7d_pct"] / 1.0), 3)
        inputs["stablecoin_supply_change_7d_pct"] = sc["change_7d_pct"]
        if sc.get("depeg_alerts"):
            flags.append("stablecoin_depeg")

    fr = data.get("funding_rates")
    leverage_heat = None
    if fr:
        majors = [fr["rates"][s]["annualized_pct"] for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT") if s in fr["rates"]]
        if majors:
            avg = sum(majors) / len(majors)
            leverage_heat = round(_clamp(avg / 30.0), 3)  # +1 ≈ 30%+ annualized = hot
            inputs["majors_avg_funding_annualized_pct"] = round(avg, 2)
            if avg > 20:
                flags.append("crowded_longs")
            elif avg < -5:
                flags.append("crowded_shorts_squeeze_risk")

    score = round(sum(comps.values()) / len(comps) * 100, 1) if comps else 0.0
    if score >= 30:
        regime = "risk_on"
    elif score <= -30:
        regime = "risk_off"
    elif abs(score) < 10:
        regime = "neutral"
    else:
        regime = "lean_risk_on" if score > 0 else "lean_risk_off"

    return {
        "feed": "regime",
        "risk_on_score": score,  # -100 (risk-off) .. +100 (risk-on)
        "regime": regime,
        "components": comps,     # each -1..+1, equally weighted
        "leverage_heat": leverage_heat,  # separate: crowding, not direction
        "flags": flags,
        "inputs": inputs,
        "inputs_age_sec": ages,
        "input_errors": errs or None,
        "note": "Heuristic composite of sentiment, momentum, breadth, total mcap and stablecoin liquidity. Not advice.",
    }


# ─────────────────────────────────────────────────────────────
# FEED REGISTRY (single source of truth for routing, 402s and every discovery surface)
# Descriptions: <= 500 chars (facilitators reject longer), describe the data and when to use it.
# Examples: small but real-shaped; they go into the Bazaar listing so agents know what they buy.
# ─────────────────────────────────────────────────────────────

FEEDS: List[Dict[str, Any]] = [
    # ── basic ──
    {"path": "/v1/signal", "key": "market_signal", "ttl": 45, "tier": "basic", "builder": build_market_signal,
     "title": "Crypto market signal",
     "description": ("Price, volume, market cap and 1h/24h/7d/30d momentum for 12 major cryptocurrencies, plus "
                     "derived fields: % from all-time high, position in the 24h range, volume/market-cap turnover "
                     "and market breadth. Use when an agent needs a one-call snapshot of large-cap crypto momentum."),
     "example": {"feed": "market_signal", "assets": {"BTC": {"usd": 91234.5, "change_24h_pct": 1.8,
                 "change_7d_pct": -3.2, "pct_from_ath": -27.6, "range_position_24h": 0.71, "volume_to_mcap": 0.021}},
                 "breadth": {"up_24h": 9, "down_24h": 3, "avg_change_24h_pct": 1.1}}},
    {"path": "/v1/funding", "key": "funding_rates", "ttl": 60, "tier": "basic", "builder": build_funding_rates,
     "title": "Binance perpetual funding rates",
     "description": ("Binance USDT-margined perpetual funding rates for 15 major pairs with interval-correct "
                     "annualization, mark/index price and basis in bps, sorted by most extreme. Use when checking "
                     "whether longs or shorts are crowded or sizing a funding carry trade."),
     "example": {"feed": "funding_rates", "rates": {"SOLUSDT": {"funding_rate": 0.00031, "funding_interval_h": 4,
                 "annualized_pct": 67.89, "basis_bps": 4.1}}}},
    {"path": "/v1/fear-greed", "key": "fear_greed", "ttl": 300, "tier": "basic", "builder": build_fear_greed,
     "title": "Crypto Fear & Greed index",
     "description": ("Crypto Fear & Greed Index (0-100) with a regime label from extreme_fear to extreme_greed, "
                     "1-day change, 7-day average and 7-day history. Use as a sentiment or regime filter before "
                     "opening risk."),
     "example": {"feed": "fear_greed", "value": 22, "classification": "Extreme Fear", "regime": "extreme_fear",
                 "change_1d": -8, "avg_7d": 31.4}},
    {"path": "/v1/defi", "key": "defi_tvl", "ttl": 180, "tier": "basic", "builder": build_defi_tvl,
     "title": "DeFi TVL by chain and protocol",
     "description": ("Total value locked for the top 15 chains and top 12 DeFi protocols with 1-day and 7-day "
                     "change and category. Use for chain or sector rotation and DeFi risk-on/risk-off reads."),
     "example": {"feed": "defi_tvl", "chains": {"Ethereum": {"tvl_usd": 61000000000}},
                 "top_protocols": [{"name": "Aave", "tvl_usd": 31000000000, "category": "Lending", "change_7d": 2.4}]}},
    {"path": "/v1/global", "key": "global_market", "ttl": 90, "tier": "basic", "builder": build_global_market,
     "title": "Global crypto market",
     "description": ("Total crypto market cap and 24h volume, BTC, ETH and stablecoin dominance, and 24h market "
                     "cap change. Use when an agent needs the overall market backdrop or a dominance-based regime input."),
     "example": {"feed": "global_market", "total_market_cap_usd": 3.1e12, "btc_dominance_pct": 58.1,
                 "stablecoin_dominance_pct": 7.9, "market_cap_change_24h_pct": 0.9}},
    {"path": "/v1/gas-pulse", "key": "gas_pulse", "ttl": 25, "tier": "basic", "builder": build_gas_pulse,
     "title": "Ethereum, Base and Sui gas",
     "description": ("Current gas on Ethereum (gas price, next EIP-1559 base fee, low/medium/high priority fees, "
                     "block utilization), Base, and the Sui reference gas price. Use when timing or pricing "
                     "onchain transactions."),
     "example": {"feed": "gas_pulse", "evm": {"latest_block": 23812345, "gas_price_gwei": 0.82,
                 "next_base_fee_gwei": 0.61, "priority_fee_medium_gwei": 0.05}, "base": {"gas_price_gwei": 0.006}}},
    {"path": "/v1/btc-network", "key": "btc_network", "ttl": 60, "tier": "basic", "builder": build_btc_network,
     "title": "Bitcoin fees and mempool",
     "description": ("Bitcoin fee tiers in sat/vB (fastest to economy), mempool size and blocks needed to clear "
                     "it, next difficulty adjustment estimate and network hashrate. Use when choosing a BTC fee "
                     "rate or monitoring network congestion and miner conditions."),
     "example": {"feed": "btc_network", "fees_sat_vb": {"fastest": 6, "half_hour": 4, "economy": 2},
                 "mempool": {"tx_count": 21000, "blocks_to_clear": 12.4},
                 "difficulty": {"estimated_change_pct": 1.3}, "hashrate_ehs": 1012.5}},
    {"path": "/v1/trending", "key": "trending", "ttl": 300, "tier": "basic", "builder": build_trending,
     "title": "Trending coins and categories",
     "description": ("The most-searched coins on CoinGecko right now with price and 24h change, plus the "
                     "hottest categories by market cap change. Use to spot narratives and retail attention."),
     "example": {"feed": "trending", "coins": [{"symbol": "HYPE", "market_cap_rank": 14, "change_24h_pct": 8.2}],
                 "categories": [{"name": "AI Agents", "market_cap_change_24h_pct": 6.1}]}},
    # ── premium ──
    {"path": "/v1/funding-arb", "key": "funding_arb", "ttl": 60, "tier": "premium", "builder": build_funding_arb,
     "title": "Cross-exchange funding arbitrage",
     "description": ("Perpetual funding for 15 majors on Binance, Bybit and Hyperliquid, normalized to annualized "
                     "% per venue's interval, with the cross-venue spread, cheapest venue to be long, richest "
                     "venue to be short, and open interest. Use when choosing where to hold a perp position or "
                     "screening delta-neutral funding trades."),
     "example": {"feed": "funding_arb", "top_opportunities": [{"asset": "ETH", "spread_annualized_pct": 21.4,
                 "long_on": "hyperliquid", "short_on": "bybit"}],
                 "assets": {"BTC": {"avg_annualized_pct": 9.8, "spread_annualized_pct": 4.2,
                                    "open_interest_usd_tracked": 12400000000}}}},
    {"path": "/v1/positioning", "key": "positioning", "ttl": 120, "tier": "premium", "builder": build_positioning,
     "title": "Derivatives positioning",
     "description": ("Binance futures positioning for BTC, ETH, SOL, XRP, DOGE and SUI: all-account vs top-trader "
                     "long/short ratios, 1h taker buy/sell ratio, 24h open interest change with price effect "
                     "removed, and a leverage_building / deleveraging flag. Use to gauge crowding and squeeze risk."),
     "example": {"feed": "positioning", "assets": {"BTC": {"global_long_short_ratio": 2.1,
                 "top_trader_long_short_ratio": 1.4, "taker_bias": "sell", "oi_change_24h_pct": 4.4,
                 "oi_contracts_change_24h_pct": 6.0, "oi_signal": "leverage_building"}}}},
    {"path": "/v1/stablecoins", "key": "stablecoins", "ttl": 600, "tier": "premium", "builder": build_stablecoins,
     "title": "Stablecoin supply flows and depegs",
     "description": ("Total USD stablecoin supply with 1d/7d/30d change, the top 12 issuers with their own flows, "
                     "and depeg alerts for coins over $100M trading more than 0.5% off peg. Use as a liquidity "
                     "inflow/outflow signal or to monitor stablecoin risk."),
     "example": {"feed": "stablecoins", "total_supply_usd": 312000000000, "change_7d_pct": 0.84,
                 "top": [{"symbol": "USDT", "supply_usd": 184000000000, "change_7d_pct": 0.6}], "depeg_alerts": []}},
    {"path": "/v1/dex-volume", "key": "dex_volume", "ttl": 600, "tier": "premium", "builder": build_dex_volume,
     "title": "DEX volume",
     "description": ("Total decentralized exchange volume for 24h and 7d with 1d/7d/30d change and the top 12 DEXs "
                     "by share and growth. Use to measure onchain trading activity and which venues are gaining."),
     "example": {"feed": "dex_volume", "total_24h_usd": 21000000000, "change_7d_pct": -2.1,
                 "top_protocols": [{"name": "Uniswap", "volume_24h_usd": 4100000000, "share_pct": 19.5}]}},
    {"path": "/v1/fees", "key": "fees_revenue", "ttl": 900, "tier": "premium", "builder": build_fees_revenue,
     "title": "Protocol fees and revenue",
     "description": ("Top 15 crypto protocols by 24h fees paid by users, with 24h revenue kept by the protocol, "
                     "take rate (revenue/fees) and 7d change. Use for fundamental token screening and "
                     "comparing protocol cash flows."),
     "example": {"feed": "fees_revenue", "top_protocols": [{"name": "Tether", "fees_24h_usd": 21000000,
                 "revenue_24h_usd": 21000000, "take_rate_pct": 100.0}]}},
    {"path": "/v1/yields", "key": "yields", "ttl": 900, "tier": "premium", "builder": build_yields, "heavy": True,
     "title": "Stablecoin yields",
     "description": ("Highest stablecoin yields in DeFi pools with at least $10M TVL, with base vs reward APY, "
                     "30-day mean APY and a spike flag when APY is over 2x its 30-day mean, plus the largest "
                     "pools by TVL. Use when allocating idle stablecoins. Not risk-rated."),
     "example": {"feed": "yields", "top_sustained_stablecoin_yields": [{"project": "aave-v3", "chain": "Ethereum",
                 "symbol": "USDC", "tvl_usd": 500000000, "apy": 5.1, "apy_mean_30d": 4.8, "spike": False}]}},
    # ── bundle ──
    {"path": "/v1/regime", "key": "regime", "ttl": 60, "tier": "bundle", "builder": build_regime,
     "title": "Crypto market regime score",
     "description": ("One composite crypto risk-on/risk-off score from -100 to +100 built from sentiment, BTC "
                     "momentum, breadth, total market cap change and stablecoin liquidity, with each component, "
                     "a separate leverage-heat reading and flags such as crowded_longs or stablecoin_depeg. "
                     "Use as a single regime gate for trading agents."),
     "example": {"feed": "regime", "risk_on_score": 18.4, "regime": "lean_risk_on",
                 "components": {"sentiment": -0.56, "btc_momentum": 0.36, "breadth": 0.5, "liquidity": 0.84},
                 "leverage_heat": 0.41, "flags": ["crowded_longs"]}},
]

if not ENABLE_HEAVY_FEEDS:
    FEEDS = [f for f in FEEDS if not f.get("heavy")]

_TIER_PRICE = {"basic": PRICE_USDC_ATOMIC, "premium": PREMIUM_PRICE_USDC_ATOMIC, "bundle": BUNDLE_PRICE_USDC_ATOMIC}
for _f_ in FEEDS:
    _f_["price"] = _TIER_PRICE[_f_["tier"]]
    assert len(_f_["description"]) <= 500, (_f_["path"], len(_f_["description"]))

FEEDS_BY_PATH = {f["path"]: f for f in FEEDS}
FEEDS_BY_KEY = {f["key"]: f for f in FEEDS}
for _dep in ("fear_greed", "global_market", "market_signal", "funding_rates", "stablecoins"):
    assert _dep in FEEDS_BY_KEY, _dep


# ─────────────────────────────────────────────────────────────
# CDP FACILITATOR AUTH (Ed25519 JWT, stdlib only — no pip needed on the NAS)
# ─────────────────────────────────────────────────────────────

_EP = 2 ** 255 - 19
_EL = 2 ** 252 + 27742317777372353535851937790883648493
_ED = -121665 * pow(121666, _EP - 2, _EP) % _EP


def _ed_add(P: Tuple, Q: Tuple) -> Tuple:
    a = (P[1] - P[0]) * (Q[1] - Q[0]) % _EP
    b = (P[1] + P[0]) * (Q[1] + Q[0]) % _EP
    c = 2 * P[3] * Q[3] * _ED % _EP
    d = 2 * P[2] * Q[2] % _EP
    e, f, g, h = b - a, d - c, d + c, b + a
    return (e * f % _EP, g * h % _EP, f * g % _EP, e * h % _EP)


def _ed_mul(s: int, P: Tuple) -> Tuple:
    Q = (0, 1, 1, 0)
    while s > 0:
        if s & 1:
            Q = _ed_add(Q, P)
        P = _ed_add(P, P)
        s >>= 1
    return Q


def _ed_base() -> Tuple:
    y = 4 * pow(5, _EP - 2, _EP) % _EP
    xx = (y * y - 1) * pow(_ED * y * y + 1, _EP - 2, _EP)
    x = pow(xx, (_EP + 3) // 8, _EP)
    if (x * x - xx) % _EP:
        x = x * pow(2, (_EP - 1) // 4, _EP) % _EP
    if x & 1:
        x = _EP - x
    return (x, y, 1, x * y % _EP)


_EB = _ed_base()


def _ed_encode(P: Tuple) -> bytes:
    zi = pow(P[2], _EP - 2, _EP)
    x, y = P[0] * zi % _EP, P[1] * zi % _EP
    return (y | ((x & 1) << 255)).to_bytes(32, "little")


def ed25519_sign(seed: bytes, msg: bytes) -> bytes:
    """RFC 8032 Ed25519. Uses `cryptography` if installed, else pure Python (fine for a few signs/sec)."""
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        return Ed25519PrivateKey.from_private_bytes(seed).sign(msg)
    except ImportError:
        pass
    h = hashlib.sha512(seed).digest()
    a = int.from_bytes(h[:32], "little")
    a = (a & ((1 << 254) - 8)) | (1 << 254)
    A = _ed_encode(_ed_mul(a, _EB))
    r = int.from_bytes(hashlib.sha512(h[32:] + msg).digest(), "little") % _EL
    R = _ed_encode(_ed_mul(r, _EB))
    k = int.from_bytes(hashlib.sha512(R + A + msg).digest(), "little") % _EL
    return R + ((r + k * a) % _EL).to_bytes(32, "little")


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def cdp_jwt(method: str, url: str) -> str:
    """Bearer JWT for one CDP request (valid 120s), per CDP API-key auth docs."""
    if "BEGIN EC" in CDP_API_KEY_SECRET:
        raise RuntimeError("CDP key is ECDSA; create an Ed25519 CDP API key (the default) for this server")
    secret = base64.b64decode(CDP_API_KEY_SECRET)
    if len(secret) not in (32, 64):
        raise RuntimeError("CDP_API_KEY_SECRET must be the base64 Ed25519 secret from the CDP portal")
    u = urllib.parse.urlparse(url)
    now = int(time.time())
    header = {"alg": "EdDSA", "typ": "JWT", "kid": CDP_API_KEY_ID, "nonce": secrets.token_hex(16)}
    claims = {"sub": CDP_API_KEY_ID, "iss": "cdp", "aud": ["cdp_service"], "nbf": now, "exp": now + 120,
              "uris": ["{} {}{}".format(method.upper(), u.netloc, u.path)]}
    signing_input = "{}.{}".format(
        _b64url(json.dumps(header, separators=(",", ":")).encode()),
        _b64url(json.dumps(claims, separators=(",", ":")).encode()),
    )
    return signing_input + "." + _b64url(ed25519_sign(secret[:32], signing_input.encode("ascii")))


# ─────────────────────────────────────────────────────────────
# x402 v2 PAYMENT LOGIC (v1 clients still accepted)
# ─────────────────────────────────────────────────────────────

BAZAAR_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "input": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "const": "http"},
                "method": {"type": "string", "enum": ["GET", "HEAD", "DELETE"]},
            },
            "required": ["type", "method"],
            "additionalProperties": False,
        },
        "output": {
            "type": "object",
            "properties": {"type": {"type": "string"}, "example": {"type": "object"}},
            "required": ["type"],
        },
    },
    "required": ["input"],
}

# Last Bazaar indexing result per path, from the facilitator's EXTENSION-RESPONSES header
_DISCOVERY_STATUS: Dict[str, Dict[str, Any]] = {}


def _icon_url() -> str:
    return ICON_URL or (PUBLIC_BASE_URL + "/icon.png" if PUBLIC_BASE_URL else "")


def _resource_url(feed: Dict) -> str:
    return PUBLIC_BASE_URL + feed["path"]


def _b64json(obj: Any) -> str:
    return base64.b64encode(json.dumps(obj, separators=(",", ":")).encode("utf-8")).decode("ascii")


def resource_info(feed: Dict) -> Dict[str, Any]:
    info = {
        "url": _resource_url(feed),
        "description": feed["description"],
        "mimeType": "application/json",
        "serviceName": SERVICE_NAME,
        "tags": SERVICE_TAGS,
    }
    if _icon_url():
        info["iconUrl"] = _icon_url()
    return info


def output_example(feed: Dict) -> Dict[str, Any]:
    return {"timestamp": 1763670000, "cache": {"age_sec": 12, "stale": False}, "data": feed["example"]}


def bazaar_extension(feed: Dict) -> Dict[str, Any]:
    return {
        "info": {
            "input": {"type": "http", "method": "GET"},
            "output": {"type": "json", "example": output_example(feed)},
        },
        "schema": BAZAAR_SCHEMA,
    }


def accepts_v2(feed: Dict) -> Dict[str, Any]:
    return {
        "scheme": "exact",
        "network": NETWORK,
        "amount": feed["price"],
        "asset": USDC_BASE,
        "payTo": PAY_TO,
        "maxTimeoutSeconds": 60,
        "extra": {"name": "USD Coin", "version": "2"},  # USDC EIP-712 domain on Base
    }


def payment_required_v2(feed: Dict, error: str) -> Dict[str, Any]:
    return {
        "x402Version": 2,
        "error": error,
        "resource": resource_info(feed),
        "accepts": [accepts_v2(feed)],
        "extensions": {"bazaar": bazaar_extension(feed)},
    }


def requirements_v1(feed: Dict) -> Dict[str, Any]:
    return {
        "scheme": "exact",
        "network": "base",
        "maxAmountRequired": feed["price"],
        "resource": _resource_url(feed),
        "description": feed["description"],
        "mimeType": "application/json",
        "payTo": PAY_TO,
        "maxTimeoutSeconds": 60,
        "asset": USDC_BASE,
        "outputSchema": {"input": {"type": "http", "method": "GET", "discoverable": True},
                         "output": output_example(feed)},
        "extra": {"name": "USD Coin", "version": "2"},
    }


def parse_payment_header(headers: Any) -> Tuple[Optional[Dict], int, str]:
    v2 = headers.get("PAYMENT-SIGNATURE")
    v1 = headers.get("X-PAYMENT")
    raw = v2 or v1
    if not raw:
        return None, 2, "PAYMENT-SIGNATURE header is required"
    try:
        raw = raw.strip()
        payload = json.loads(base64.b64decode(raw + "=" * (-len(raw) % 4)).decode("utf-8"))
    except Exception:
        return None, (2 if v2 else 1), "Malformed payment header (expected base64-encoded JSON)"
    if not isinstance(payload, dict):
        return None, (2 if v2 else 1), "Malformed payment payload"
    try:
        version = int(payload.get("x402Version") or (2 if v2 else 1))
    except (TypeError, ValueError):
        version = 2 if v2 else 1
    return payload, version, ""


def _same(a: Any, b: Any) -> bool:
    return str(a or "").lower() == str(b or "").lower()


def check_payload_matches(payload: Dict, version: int, feed: Dict) -> str:
    """Cheap local check before calling the facilitator. Returns '' if OK, else the reason."""
    if version >= 2:
        acc = payload.get("accepted") or {}
        want = accepts_v2(feed)
        for k in ("scheme", "network", "amount", "asset", "payTo"):
            if not _same(acc.get(k), want[k]):
                return "Payment does not match requirements ({} mismatch)".format(k)
    else:
        if payload.get("scheme") != "exact" or payload.get("network") not in ("base", NETWORK):
            return "Payment does not match requirements (scheme/network)"
    return ""


def _facilitator_post(op: str, body: Dict) -> Tuple[Dict, Dict[str, str]]:
    url = "{}/{}".format(FACILITATOR_BASE, op)
    headers = {}
    if CDP_API_KEY_ID and CDP_API_KEY_SECRET and FACILITATOR_BASE.startswith(CDP_FACILITATOR):
        headers["Authorization"] = "Bearer " + cdp_jwt("POST", url)
    res, hdrs = _http_post(url, body, timeout=12, headers=headers)
    return (res if isinstance(res, dict) else {}), hdrs


def _facilitator_body(payload: Dict, version: int, feed: Dict) -> Dict:
    reqs = accepts_v2(feed) if version >= 2 else requirements_v1(feed)
    return {"x402Version": version, "paymentPayload": payload, "paymentRequirements": reqs}


def verify_payment(payload: Dict, version: int, feed: Dict) -> Tuple[bool, str]:
    try:
        res, _ = _facilitator_post("verify", _facilitator_body(payload, version, feed))
    except Exception as exc:
        return False, "Facilitator error: {}".format(str(exc)[:200])
    if not res.get("isValid"):
        return False, res.get("invalidReason") or res.get("errorMessage") or "Verification failed"
    return True, "verified"


def settle_payment(payload: Dict, version: int, feed: Dict) -> Tuple[bool, str, Optional[Dict]]:
    try:
        res, hdrs = _facilitator_post("settle", _facilitator_body(payload, version, feed))
    except Exception as exc:
        return False, "Facilitator error: {}".format(str(exc)[:200]), None
    ext = hdrs.get("EXTENSION-RESPONSES")
    if ext:
        try:
            decoded = json.loads(base64.b64decode(ext + "=" * (-len(ext) % 4)).decode("utf-8"))
        except Exception:
            decoded = {"raw": ext[:300]}
        _DISCOVERY_STATUS[feed["path"]] = {"ts": int(time.time()), "bazaar": decoded.get("bazaar", decoded)}
    if not res.get("success", True):
        return False, res.get("errorReason", "Settlement failed"), None
    return True, "settled", res


def attach_discovery(payload: Dict, version: int, feed: Dict) -> None:
    """Bazaar indexes from the *settled* payload's resource + extensions.bazaar. Many clients don't echo
    them, so fill them in from our own declaration when missing (they are not part of the signature)."""
    if version >= 2:
        if not payload.get("resource"):
            payload["resource"] = resource_info(feed)
        ext = payload.get("extensions")
        if not isinstance(ext, dict):
            ext = {}
            payload["extensions"] = ext
        ext.setdefault("bazaar", bazaar_extension(feed))


# ─────────────────────────────────────────────────────────────
# DISCOVERY SURFACES (free): /.well-known/x402, /openapi.json, /llms.txt, /icon.png, /
# ─────────────────────────────────────────────────────────────

def _tier_line(tier: str) -> str:
    return "${:.3f} USDC".format(int(_TIER_PRICE[tier]) / 1e6)


def well_known_x402() -> Dict[str, Any]:
    doc: Dict[str, Any] = {
        "version": 1,
        "resources": [_resource_url(f) for f in FEEDS],
        "instructions": (
            "# {name}\n\nPay-per-request crypto market data over x402 (USDC on Base, `{net}`). "
            "Every endpoint is a plain GET with no parameters; an unpaid request returns HTTP 402 with a "
            "`PAYMENT-REQUIRED` header. You are only charged when data is delivered.\n\n"
            "Basic {b} · Premium {p} · Bundle {u}\n\nFull catalog: {base}/v1/feeds · OpenAPI: {base}/openapi.json"
        ).format(name=SERVICE_NAME, net=NETWORK, b=_tier_line("basic"), p=_tier_line("premium"),
                 u=_tier_line("bundle"), base=PUBLIC_BASE_URL),
    }
    if OWNERSHIP_PROOFS:
        doc["ownershipProofs"] = OWNERSHIP_PROOFS
    return doc


def openapi_doc() -> Dict[str, Any]:
    paths: Dict[str, Any] = {}
    for f in FEEDS:
        paths[f["path"]] = {"get": {
            "operationId": f["key"],
            "summary": f["title"],
            "description": f["description"],
            "tags": [f["tier"]],
            "x-payment-info": {
                "protocol": "x402", "scheme": "exact", "network": NETWORK, "asset": USDC_BASE,
                "amount": f["price"], "price_usd": int(f["price"]) / 1e6, "payTo": PAY_TO,
            },
            "responses": {
                "200": {"description": "Feed data (payment settled)",
                        "content": {"application/json": {"example": output_example(f)}}},
                "402": {"description": "Payment required. Base64 x402 v2 PaymentRequired in the "
                                       "PAYMENT-REQUIRED header; send PAYMENT-SIGNATURE to pay.",
                        "headers": {"PAYMENT-REQUIRED": {"schema": {"type": "string"}}}},
                "502": {"description": "Upstream source unavailable; payment was not settled."},
            },
        }}
    return {
        "openapi": "3.1.0",
        "info": {"title": SERVICE_NAME, "version": VERSION,
                 "description": "Pay-per-request crypto market, derivatives and DeFi data for agents via x402."},
        "servers": [{"url": PUBLIC_BASE_URL or "/"}],
        "paths": paths,
    }


def llms_txt() -> str:
    lines = [
        "# {}".format(SERVICE_NAME),
        "",
        "> Pay-per-request crypto market, derivatives and DeFi data for AI agents. "
        "Payment: x402 v2, USDC on Base ({}), payTo {}. No API key or account.".format(NETWORK, PAY_TO),
        "",
        "How to call: GET any endpoint below. Unpaid requests return HTTP 402 with a base64 `PAYMENT-REQUIRED` "
        "header; retry with a `PAYMENT-SIGNATURE` header from any x402 client. Agents are only charged "
        "when data is delivered (upstream failures return 502 unsettled).",
        "",
        "## Endpoints",
        "",
    ]
    for f in FEEDS:
        lines.append("- [{} ({})]({}): {}".format(
            f["title"], "${:.3f}".format(int(f["price"]) / 1e6), _resource_url(f), f["description"]))
    lines += ["", "## Machine-readable", "",
              "- [Feed catalog]({}/v1/feeds)".format(PUBLIC_BASE_URL),
              "- [OpenAPI]({}/openapi.json)".format(PUBLIC_BASE_URL),
              "- [x402 discovery]({}/.well-known/x402)".format(PUBLIC_BASE_URL), ""]
    return "\n".join(lines)


def _make_icon_png(size: int = 128) -> bytes:
    """Tiny procedurally drawn PNG (rising bars) so listings have an icon. Override with ICON_URL."""
    bg, fg = (16, 24, 40), (52, 211, 153)
    bars = [0.35, 0.55, 0.45, 0.75, 0.9]
    bw = size // (len(bars) * 2 + 1)
    rows = []
    for y in range(size):
        row = bytearray([0])
        for x in range(size):
            col = bg
            i, rem = divmod(x - bw, 2 * bw)
            if 0 <= i < len(bars) and rem < bw and x >= bw and y >= size - bw - int(bars[i] * (size - 2 * bw)) \
                    and y < size - bw:
                col = fg
            row += bytes(col)
        rows.append(bytes(row))

    def chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(b"".join(rows), 9)) + chunk(b"IEND", b""))


_ICON_PNG = _make_icon_png()


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 32


def _feed_public(f: Dict) -> Dict:
    return {
        "path": f["path"],
        "url": _resource_url(f),
        "title": f["title"],
        "tier": f["tier"],
        "price_usdc_atomic": f["price"],
        "price_usd": int(f["price"]) / 1e6,
        "network": NETWORK,
        "cache_ttl_sec": f["ttl"],
        "description": f["description"],
    }


CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "PAYMENT-SIGNATURE, X-PAYMENT, Content-Type",
    "Access-Control-Expose-Headers": "PAYMENT-REQUIRED, PAYMENT-RESPONSE, X-PAYMENT-RESPONSE",
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    timeout = 12

    def _send(self, status: int, raw: bytes, ctype: str, extra_headers: Optional[Dict[str, str]] = None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        for k, v in CORS_HEADERS.items():
            self.send_header(k, v)
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(raw)

    def _send_json(self, status: int, body: Any, extra_headers: Optional[Dict[str, str]] = None):
        self._send(status, json.dumps(body, separators=(",", ":")).encode("utf-8"), "application/json",
                   extra_headers)

    def _payment_required(self, feed: Dict, reason: str, version: int = 2):
        pr = payment_required_v2(feed, reason)
        headers = {"PAYMENT-REQUIRED": _b64json(pr)}
        if version == 1 or X402_V1_BODY:
            body = {"x402Version": 1, "error": reason, "accepts": [requirements_v1(feed)]}
        else:
            body = pr
        self._send_json(402, body, headers)

    def _handle_paid_feed(self, feed: Dict):
        payload, version, err = parse_payment_header(self.headers)
        if payload is None:
            self._payment_required(feed, err, version)
            return
        err = check_payload_matches(payload, version, feed)
        if err:
            self._payment_required(feed, err, version)
            return
        attach_discovery(payload, version, feed)

        ok, reason = verify_payment(payload, version, feed)
        if not ok:
            self._payment_required(feed, reason, version)
            return

        # Build BEFORE settling: if upstream is down the agent is not charged.
        try:
            data, meta = _cached(feed["key"], feed["ttl"], feed["builder"])
        except Exception as exc:
            self._send_json(502, {"error": "Upstream feed error (payment not settled): {}".format(str(exc)[:300])})
            return

        ok, reason, settlement = settle_payment(payload, version, feed)
        if not ok:
            self._payment_required(feed, reason, version)
            return

        headers = {}
        if settlement:
            headers["PAYMENT-RESPONSE" if version >= 2 else "X-PAYMENT-RESPONSE"] = _b64json(settlement)
        self._send_json(200, {
            "timestamp": int(time.time()),
            "payTo": PAY_TO,
            "settlement": settlement,
            "cache": meta,
            "data": data,
        }, headers)

    def do_OPTIONS(self):
        self._send(204, b"", "text/plain")

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        path = self.path.split("?", 1)[0].rstrip("/") or "/"

        if path == "/":
            self._send_json(200, {
                "name": SERVICE_NAME,
                "description": "Pay-per-request crypto market, derivatives and DeFi data for AI agents (x402, USDC on Base).",
                "payTo": PAY_TO,
                "network": NETWORK,
                "feeds": [f["path"] for f in FEEDS],
                "links": {"catalog": "/v1/feeds", "openapi": "/openapi.json", "llms": "/llms.txt",
                          "x402_discovery": "/.well-known/x402", "health": "/health"},
            })
            return

        if path == "/health":
            self._send_json(200, {
                "status": "online",
                "host": "Synology DS218play",
                "payTo": PAY_TO,
                "network": NETWORK,
                "facilitator": FACILITATOR_BASE,
                "public_base_url": PUBLIC_BASE_URL or None,
                "price_usdc_atomic": PRICE_USDC_ATOMIC,
                "feeds": len(FEEDS),
                "cached_keys": len(_CACHE),
                "version": VERSION,
            })
            return

        if path == "/v1/feeds":
            self._send_json(200, {
                "name": SERVICE_NAME,
                "payTo": PAY_TO,
                "network": NETWORK,
                "asset": USDC_BASE,
                "x402Version": 2,
                "tiers": {k: {"price_usdc_atomic": v, "price_usd": int(v) / 1e6} for k, v in _TIER_PRICE.items()},
                "endpoints": [_feed_public(f) for f in FEEDS],
                "note": "All endpoints are paid via x402 (USDC on Base). Agents pay per request; "
                        "you are only charged when data is delivered.",
            })
            return

        if path in ("/.well-known/x402", "/.well-known/x402.json"):
            self._send_json(200, well_known_x402())
            return
        if path == "/openapi.json":
            self._send_json(200, openapi_doc())
            return
        if path == "/llms.txt":
            self._send(200, llms_txt().encode("utf-8"), "text/plain; charset=utf-8")
            return
        if path in ("/icon.png", "/favicon.ico"):
            self._send(200, _ICON_PNG, "image/png", {"Cache-Control": "public, max-age=86400"})
            return
        if path == "/robots.txt":
            self._send(200, b"User-agent: *\nAllow: /\n", "text/plain")
            return
        if path == "/v1/discovery-status":
            self._send_json(200, {
                "facilitator": FACILITATOR_BASE,
                "cdp_bazaar_eligible": FACILITATOR_BASE.startswith(CDP_FACILITATOR),
                "public_base_url": PUBLIC_BASE_URL or None,
                "last_bazaar_result_by_path": _DISCOVERY_STATUS,
                "note": "Populated after a paid call settles. bazaar.status: success | processing | rejected.",
            })
            return

        feed = FEEDS_BY_PATH.get(path)
        if feed:
            self._handle_paid_feed(feed)
            return

        self._send_json(404, {"error": "Not found", "hint": "See /v1/feeds for available paid endpoints"})

    def log_message(self, fmt, *args):
        return  # keep logs quiet on the NAS


# ─────────────────────────────────────────────────────────────
# CLI: `python3 nas_pay_server.py --check` validates every endpoint with the CDP Bazaar
# ─────────────────────────────────────────────────────────────

def run_discovery_check() -> int:
    if not PUBLIC_BASE_URL.startswith("https://"):
        print("PUBLIC_BASE_URL must be your public https:// origin (now: {!r})".format(PUBLIC_BASE_URL))
        return 1
    bad = 0
    for f in FEEDS:
        url = _resource_url(f)
        try:
            res, _ = _http_post("https://api.cdp.coinbase.com/platform/v2/x402/validate",
                                {"resource": url, "method": "GET"}, timeout=30)
        except Exception as exc:
            res = {"error": str(exc)}
        ok = res.get("valid") is True
        outcome = (res.get("simulation") or {}).get("outcome")
        bad += 0 if ok else 1
        print("{:4} {:<20} valid={} outcome={} {}".format(
            "OK" if ok else "FAIL", f["path"], res.get("valid"), outcome,
            "" if ok else json.dumps(res)[:300]))
    return 1 if bad else 0


def _startup_warnings() -> None:
    if not PUBLIC_BASE_URL.startswith("https://"):
        print("WARNING: PUBLIC_BASE_URL is not a public https:// URL — indexers cannot list you.")
    if "x402.org" in FACILITATOR_BASE:
        print("WARNING: x402.org facilitator is testnet-only; Base mainnet payments will not settle.")
    if not FACILITATOR_BASE.startswith(CDP_FACILITATOR):
        print("NOTE: not using the CDP facilitator — you will not appear in the Coinbase Bazaar / agentic.market.")


if __name__ == "__main__":
    if "--check" in sys.argv:
        sys.exit(run_discovery_check())
    _startup_warnings()
    server = BoundedThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print("{} {} listening on 0.0.0.0:{} -> {}".format(SERVICE_NAME, VERSION, PORT, PAY_TO))
    print("Facilitator: {}   Public URL: {}".format(FACILITATOR_BASE, PUBLIC_BASE_URL or "(unset)"))
    print("Feeds: " + "  ".join(f["path"] for f in FEEDS))
    server.serve_forever()
