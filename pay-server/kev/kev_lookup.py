"""CISA Known Exploited Vulnerabilities (KEV) lookup. Standalone, Python 3.12, standard library only.

Source: https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json (CC0 1.0).

Usage (JSON on stdout):
    python kev_lookup.py fetch                       # download into ./cache (never overwrites)
    python kev_lookup.py cve CVE-2021-44228
    python kev_lookup.py search --vendor apache --product log4j [--min-quality word] [--limit 20]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

FEED_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
USER_AGENT = "kev-lookup/0.1 (standalone research tool)"   # deliberately contact-free
DEFAULT_CACHE = Path(__file__).resolve().parent / "cache"
SOURCE_NOTE = "CISA Known Exploited Vulnerabilities catalog (CC0 1.0); not endorsed by CISA."
FIELDS = ("cveID", "vendorProject", "product", "vulnerabilityName", "dateAdded", "dueDate",
          "requiredAction", "knownRansomwareCampaignUse")
CVE_RE = re.compile(r"^CVE-\d{4}-\d{4,}$")
QUALITY = {"exact": 3, "word": 2, "substring": 1}   # higher = stronger match
MIN_SUBSTRING_LEN = 4                                # shorter queries only match exact/whole word (local-model debate: 'ios'->'FortiOS')
MAX_QUERY_LEN = 100


class KevError(Exception):
    """Bad feed, bad cache or bad query."""


# ---------------------------------------------------------------- fetch + cache
def _validate_feed(raw: bytes) -> dict:
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as e:
        raise KevError(f"feed is not valid JSON: {e}") from None
    vulns = data.get("vulnerabilities") if isinstance(data, dict) else None
    if not isinstance(vulns, list) or not vulns:
        raise KevError("feed has no 'vulnerabilities' list")
    for v in vulns:
        if not isinstance(v, dict) or not CVE_RE.match(str(v.get("cveID", ""))):
            raise KevError(f"feed entry without a valid cveID: {str(v)[:80]}")
    return data


def _http_get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _list_cache(cache_dir: Path) -> list[Path]:
    return sorted(cache_dir.glob("kev_*.meta.json"))


def fetch(cache_dir: Path = DEFAULT_CACHE, url: str = FEED_URL, getter=_http_get, now=None) -> dict:
    """Download the catalog and store it as a new, timestamped cache file.
    Never overwrites: files are opened with exclusive create ('x'). If the content is identical to the latest
    cache, no new data file is written; the fetch is only appended to fetch_log.jsonl."""
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    raw = getter(url)
    data = _validate_feed(raw)
    sha = hashlib.sha256(raw).hexdigest()
    fetched_at = (now or dt.datetime.now(dt.timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = _list_cache(cache_dir)
    latest_meta = json.loads(latest[-1].read_text(encoding="utf-8")) if latest else None
    duplicate = bool(latest_meta and latest_meta["sha256"] == sha)
    meta = {"fetched_at": fetched_at, "sha256": sha, "source_url": url, "bytes": len(raw),
            "catalogVersion": data.get("catalogVersion"), "dateReleased": data.get("dateReleased"),
            "count": len(data["vulnerabilities"])}
    if duplicate:
        meta["data_file"] = latest_meta["data_file"]
        meta["new_file"] = False
    else:
        stamp = fetched_at.replace("-", "").replace(":", "")
        base = f"kev_{stamp}_{sha[:12]}"
        with open(cache_dir / f"{base}.json", "xb") as f:           # 'x' = fail if exists
            f.write(raw)
        meta["data_file"] = f"{base}.json"
        meta["new_file"] = True
        with open(cache_dir / f"{base}.meta.json", "x", encoding="utf-8") as f:
            json.dump(meta, f, indent=1)
    with open(cache_dir / "fetch_log.jsonl", "a", encoding="utf-8") as f:   # append-only log
        f.write(json.dumps(meta) + "\n")
    return meta


KEEP_VERSIONS = 10


def prune_cache(cache_dir: Path = DEFAULT_CACHE, keep: int = KEEP_VERSIONS) -> list[str]:
    """Delete catalog versions older than the newest `keep` (data file + its meta file only).
    Never touches fetch_log.jsonl or any other file. Returns the deleted file names."""
    if keep < 1:
        raise KevError("keep must be >= 1")
    cache_dir = Path(cache_dir)
    metas = _list_cache(cache_dir)
    deleted = []
    for m in metas[:-keep]:
        data_file = json.loads(m.read_text(encoding="utf-8")).get("data_file", "")
        target = cache_dir / data_file
        if re.fullmatch(r"kev_\d{8}T\d{6}Z_[0-9a-f]{12}\.json", data_file) and target.is_file():
            target.unlink(); deleted.append(data_file)
        m.unlink(); deleted.append(m.name)
    return deleted


# ---------------------------------------------------------------- route input checks (run BEFORE payment)
ROUTE_CVE_RE = re.compile(r"^CVE-\d{4}-\d{4,7}$")
ROUTE_TERM_RE = re.compile(r"^[A-Za-z0-9 .\-]{2,64}$")


def validate_request(route: str, params: dict) -> tuple[int, dict]:
    """Pure check for the planned x402 routes. Returns (200, cleaned_params) or (400, error_body).
    A 400 must be returned without a payment requirement, so nobody pays for an invalid query.
    /v1/kev          : cve=CVE-YYYY-NNNN[NNN] (case-insensitive, trimmed)
    /v1/kev/search   : vendor and/or product, 2-64 chars, letters/digits/space/dot/dash;
                       optional min_quality (exact|word|substring, default word) and limit (1-100, default 25)."""
    def bad(msg): return 400, {"error": msg, "charged": False}
    allowed = {"/v1/kev": {"cve"}, "/v1/kev/search": {"vendor", "product", "min_quality", "limit"}}
    if route not in allowed:
        return bad("unknown route")
    extra = set(params) - allowed[route]
    if extra:
        return bad(f"unknown parameter(s): {', '.join(sorted(extra))}")
    if route == "/v1/kev":
        cve = str(params.get("cve", "")).strip().upper()
        if not ROUTE_CVE_RE.match(cve):
            return bad("cve must look like CVE-2021-44228")
        return 200, {"cve": cve}
    out = {}
    for k in ("vendor", "product"):
        v = params.get(k)
        if v is None or str(v).strip() == "":
            continue
        v = " ".join(str(v).split())
        if not ROUTE_TERM_RE.match(v) or not re.search(r"[A-Za-z0-9]", v):
            return bad(f"{k} must be 2-64 characters: letters, digits, spaces, dots, dashes (drop other punctuation)")
        out[k] = v
    if not out:
        return bad("give vendor and/or product")
    mq = params.get("min_quality", "word")
    if mq not in QUALITY:
        return bad("min_quality must be exact, word or substring")
    try:
        limit = int(params.get("limit", 25))
    except (TypeError, ValueError):
        return bad("limit must be an integer 1-100")
    if not 1 <= limit <= 100:
        return bad("limit must be an integer 1-100")
    out.update(min_quality=mq, limit=limit)
    return 200, out


def load_latest(cache_dir: Path = DEFAULT_CACHE) -> "Catalog":
    metas = _list_cache(Path(cache_dir))
    if not metas:
        raise KevError(f"no cache in {cache_dir}; run 'fetch' first")
    meta = json.loads(metas[-1].read_text(encoding="utf-8"))
    raw = (Path(cache_dir) / meta["data_file"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != meta["sha256"]:
        raise KevError(f"cache file {meta['data_file']} does not match its recorded sha256")
    return Catalog(_validate_feed(raw), meta)


# ---------------------------------------------------------------- matching
def _norm(s: str) -> str:
    """casefold, non-alphanumerics -> single space ('D-Link' -> 'd link', 'IOS XE' -> 'ios xe')."""
    return " ".join(re.sub(r"[^0-9a-z]+", " ", str(s).casefold()).split())


def _compact(s: str) -> str:
    return _norm(s).replace(" ", "")


def match_quality(query: str, field: str) -> str | None:
    """Best match of query against one field: 'exact' > 'word' > 'substring' > None.
    exact     : equal after normalisation, also ignoring spaces/punctuation ('dlink' == 'D-Link')
    word      : query's words appear as whole consecutive words ('ios' in 'IOS XE', 'http server' in 'Apache HTTP Server')
    substring : plain case-insensitive substring (spec behaviour); only for queries of >= 4 characters
                ('log4j' in 'Log4j2'); 'go' never substring-matches 'Google'."""
    q, f = _norm(query), _norm(field)
    if not q or not f:
        return None
    if q == f or _compact(query) == _compact(field):
        return "exact"
    if re.search(r"(?:^| )" + re.escape(q) + r"(?: |$)", f):
        return "word"
    if len(_compact(query)) >= MIN_SUBSTRING_LEN and (
            query.strip().casefold() in str(field).casefold() or _compact(query) in _compact(field)):
        return "substring"
    return None


class Catalog:
    def __init__(self, data: dict, meta: dict | None = None):
        self.data = data
        self.meta = meta or {}
        self.by_cve = {v["cveID"].upper(): v for v in data["vulnerabilities"]}

    def provenance(self) -> dict:
        return {"source": SOURCE_NOTE, "catalogVersion": self.data.get("catalogVersion"),
                "dateReleased": self.data.get("dateReleased"), "fetched_at": self.meta.get("fetched_at"),
                "sha256": self.meta.get("sha256"), "entries": len(self.by_cve)}

    @staticmethod
    def _entry(v: dict) -> dict:
        return {k: v.get(k) for k in FIELDS}

    def lookup_cve(self, cve: str) -> dict:
        q = str(cve).strip().upper()
        if not CVE_RE.match(q):
            raise KevError(f"not a CVE id: {cve!r} (expected CVE-YYYY-NNNN)")
        v = self.by_cve.get(q)
        return {"query": q, "known_exploited": v is not None, "entry": self._entry(v) if v else None,
                "provenance": self.provenance()}

    def search(self, vendor: str | None = None, product: str | None = None,
               min_quality: str = "substring", limit: int = 50) -> dict:
        if not (vendor or product):
            raise KevError("give vendor and/or product")
        if min_quality not in QUALITY:
            raise KevError(f"min_quality must be one of {list(QUALITY)}")
        for name, val in (("vendor", vendor), ("product", product)):
            if val is not None and (not _norm(val) or len(val) > MAX_QUERY_LEN):
                raise KevError(f"{name} must contain letters or digits and be at most {MAX_QUERY_LEN} characters")
        limit = max(1, min(int(limit), 500))
        hits = []
        for v in self.data["vulnerabilities"]:
            qual = []
            if vendor:
                mv = match_quality(vendor, v.get("vendorProject", ""))
                if not mv: continue
                qual.append(mv)
            if product:
                mp = match_quality(product, v.get("product", ""))
                if not mp: continue
                qual.append(mp)
            weakest = min(qual, key=QUALITY.get)          # an entry is only as strong as its weakest field match
            if QUALITY[weakest] < QUALITY[min_quality]:
                continue
            hits.append((QUALITY[weakest], v.get("dateAdded") or "", v, weakest))
        hits.sort(key=lambda h: (h[0], h[1]), reverse=True)  # best quality first, then newest
        notes = []
        for name, val in (("vendor", vendor), ("product", product)):
            if val and len(_compact(val)) < MIN_SUBSTRING_LEN:
                notes.append(f"{name} query shorter than {MIN_SUBSTRING_LEN} characters: substring matching disabled")
        return {"query": {"vendor": vendor, "product": product, "min_quality": min_quality},
                "total_matches": len(hits), "returned": min(len(hits), limit),
                "ransomware_known": sum(1 for h in hits if h[2].get("knownRansomwareCampaignUse") == "Known"),
                "results": [{**self._entry(v), "match_quality": q} for _, _, v, q in hits[:limit]],
                "notes": notes, "provenance": self.provenance()}


# ---------------------------------------------------------------- CLI
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="CISA KEV lookup (offline cache)")
    ap.add_argument("--cache", default=str(DEFAULT_CACHE))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("fetch")
    c = sub.add_parser("cve"); c.add_argument("cve")
    s = sub.add_parser("search")
    s.add_argument("--vendor"); s.add_argument("--product")
    s.add_argument("--min-quality", default="substring", choices=list(QUALITY))
    s.add_argument("--limit", type=int, default=50)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "fetch":
            out = fetch(Path(a.cache))
        elif a.cmd == "cve":
            out = load_latest(Path(a.cache)).lookup_cve(a.cve)
        else:
            out = load_latest(Path(a.cache)).search(a.vendor, a.product, a.min_quality, a.limit)
    except KevError as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        return 2
    print(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
