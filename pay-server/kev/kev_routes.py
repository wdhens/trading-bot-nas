"""KEV routes for the NAS x402 pay server. Framework-agnostic: the server's request handler calls two functions.

Flow per request (keeps the server's existing rule "only charged when data is delivered"):
    1. pre = pre_payment(path, query)            # BEFORE any x402 check
       pre.status == 400  -> return pre.body as JSON, HTTP 400, NO PAYMENT-REQUIRED header (free)
       pre.status == 402  -> proceed with the normal x402 flow at pre.price_usdc_atomic (unpaid -> 402 quote)
    2. after the payment signature is VERIFIED, but BEFORE settlement:
       status, body = SERVICE.respond(pre)       # 200 -> settle and return; 502/503 -> do NOT settle, return error
Nothing here touches the existing 19 feeds, wallets, ledger or payment code.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path

import kev_lookup as kl

ROUTES = {
    "/v1/kev": {"tier": "basic", "price_usd": 0.005, "price_usdc_atomic": "5000"},
    "/v1/kev/search": {"tier": "premium", "price_usd": 0.010, "price_usdc_atomic": "10000"},
}
MAX_CACHE_AGE_H = 7 * 24          # older catalog -> 503, unsettled (CISA updates several times a week)
HEALTH_MAX_AGE_H = 48             # source-health turns red earlier (2 missed daily fetches), before serving stops


@dataclass
class PrePayment:
    status: int                                   # 400 (free reject) or 402 (payment needed)
    path: str
    body: dict = field(default_factory=dict)      # error body for 400
    params: dict = field(default_factory=dict)    # cleaned params for 402
    price_usdc_atomic: str | None = None
    price_usd: float | None = None


def _single_values(query: dict) -> tuple[dict | None, str | None]:
    """Accept {k: str} or {k: [str]} (as parse_qs gives). Repeated parameters are rejected."""
    out = {}
    for k, v in (query or {}).items():
        if isinstance(v, (list, tuple)):
            if len(v) != 1:
                return None, f"parameter {k} given {len(v)} times"
            v = v[0]
        out[str(k)] = "" if v is None else str(v)
    return out, None


def pre_payment(path: str, query: dict) -> PrePayment:
    """Input check that MUST run before the x402 payment check. Pure function, no I/O."""
    path = path.rstrip("/") or path
    if path not in ROUTES:
        return PrePayment(400, path, {"error": "unknown route", "charged": False})
    params, err = _single_values(query)
    if err:
        return PrePayment(400, path, {"error": err, "charged": False})
    code, res = kl.validate_request(path, params)
    if code != 200:
        return PrePayment(400, path, res)
    r = ROUTES[path]
    return PrePayment(402, path, params=res, price_usdc_atomic=r["price_usdc_atomic"], price_usd=r["price_usd"])


class KevService:
    """Holds the newest cached catalog in memory; reloads when a newer cache file appears."""

    def __init__(self, cache_dir: Path = kl.DEFAULT_CACHE, now=None):
        self.cache_dir = Path(cache_dir)
        self._now = now or (lambda: dt.datetime.now(dt.timezone.utc))
        self._cat: kl.Catalog | None = None
        self._meta_name: str | None = None

    def _catalog(self) -> kl.Catalog:
        metas = sorted(self.cache_dir.glob("kev_*.meta.json"))
        if not metas:
            raise kl.KevError("no KEV cache yet")
        if self._cat is None or metas[-1].name != self._meta_name:
            self._cat = kl.load_latest(self.cache_dir)          # verifies sha256
            self._meta_name = metas[-1].name
        return self._cat

    def age_hours(self) -> float | None:
        try:
            fetched = self._catalog().meta.get("fetched_at")
            t = dt.datetime.strptime(fetched, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
            return (self._now() - t).total_seconds() / 3600
        except Exception:
            return None

    def health(self) -> dict:
        """Value for the caller to put under sources['cisa_kev'] in /v1/source-health."""
        age = self.age_hours()
        if age is None:
            return {"ok": False, "error": "no valid KEV cache"}
        return {"ok": age <= HEALTH_MAX_AGE_H, "age_hours": round(age, 1)}

    def respond(self, pre: PrePayment) -> tuple[int, dict]:
        """Call after payment VERIFICATION, before SETTLEMENT. Only settle on 200."""
        if pre.status != 402:
            return 400, pre.body
        try:
            cat = self._catalog()
            age = self.age_hours()
            if age is None or age > MAX_CACHE_AGE_H:
                return 503, {"error": "KEV catalog is stale; not charged", "charged": False}
            p = pre.params
            if pre.path == "/v1/kev":
                body = cat.lookup_cve(p["cve"])
            else:
                body = cat.search(p.get("vendor"), p.get("product"), p["min_quality"], p["limit"])
        except kl.KevError as e:
            return 503, {"error": f"KEV unavailable: {e}; not charged", "charged": False}
        except Exception:                                    # never settle on an unexpected error
            return 503, {"error": "KEV internal error; not charged", "charged": False}
        body["note"] = "Absence from the CISA KEV catalog is not proof that a product or CVE is safe."
        return 200, body
