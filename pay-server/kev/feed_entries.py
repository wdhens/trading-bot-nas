"""Catalog snippets for the two KEV routes: /v1/feeds entries, OpenAPI paths, llms.txt lines, .well-known/x402 URLs.
Add-only helpers: they return NEW objects and never modify the existing 19 feed entries.
Run `python feed_entries.py` to write the snippet files (feeds_kev.json, openapi_kev.json, llms_kev.txt)."""
from __future__ import annotations

import copy
import json
from pathlib import Path

BASE = "https://strawberryking.synology.me"
NETWORK, ASSET = "eip155:8453", "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
PAY_TO = "0xC9eebecb1d0AfF4fb2B9978516E075A33639892C"   # existing payTo, unchanged
TAGS = ["security", "vulnerability", "cisa", "kev"]
SRC = "Source: CISA Known Exploited Vulnerabilities catalog (CC0), refreshed daily. Absence from KEV is not proof of safety. Not endorsed by CISA."

FEEDS = [
    {"path": "/v1/kev", "url": BASE + "/v1/kev?cve=CVE-2021-44228", "title": "Known exploited vulnerability check",
     "tier": "basic", "price_usdc_atomic": "5000", "price_usd": 0.005, "network": NETWORK, "cache_ttl_sec": 3600,
     "tags": TAGS,
     "description": "Is this CVE known to be exploited in the wild? Returns whether it is in the CISA KEV catalog, date added, "
                    "CISA due date, required action and known ransomware use. Parameter: cve=CVE-YYYY-NNNN. " + SRC},
    {"path": "/v1/kev/search", "url": BASE + "/v1/kev/search?vendor=cisco&product=ios", "title": "Known exploited vulnerabilities by vendor/product",
     "tier": "premium", "price_usdc_atomic": "10000", "price_usd": 0.01, "network": NETWORK, "cache_ttl_sec": 3600,
     "tags": TAGS,
     "description": "All CISA KEV entries for a vendor and/or product (2-64 chars), each labelled exact/word/substring match, "
                    "newest first, with dueDate, requiredAction and ransomware use. Parameters: vendor, product, "
                    "min_quality (exact|word|substring, default word), limit (1-100). " + SRC},
]

_EXAMPLE_CVE = {"query": "CVE-2021-44228", "known_exploited": True,
                "entry": {"cveID": "CVE-2021-44228", "vendorProject": "Apache", "product": "Log4j2",
                          "dateAdded": "2021-12-10", "dueDate": "2021-12-24", "knownRansomwareCampaignUse": "Known"}}


def openapi_paths() -> dict:
    def op(f, op_id, params):
        return {"get": {"operationId": op_id, "summary": f["title"], "description": f["description"], "tags": TAGS,
                        "parameters": params,
                        "x-payment-info": {"protocol": "x402", "scheme": "exact", "network": NETWORK, "asset": ASSET,
                                           "amount": f["price_usdc_atomic"], "price_usd": f["price_usd"], "payTo": PAY_TO},
                        "responses": {"200": {"description": "Lookup result (payment settled)"},
                                      "400": {"description": "Invalid parameters. Free: no payment requested."},
                                      "402": {"description": "Payment required (x402 v2, PAYMENT-REQUIRED header)."},
                                      "503": {"description": "Catalog unavailable or stale. Not charged."}}}}
    q = lambda n, req, desc, sch: {"name": n, "in": "query", "required": req, "description": desc, "schema": sch}
    p_cve = op(FEEDS[0], "kev_check_cve", [q("cve", True, "CVE id", {"type": "string", "pattern": r"^CVE-\d{4}-\d{4,7}$"})])
    p_cve["get"]["responses"]["200"]["content"] = {"application/json": {"example": _EXAMPLE_CVE}}
    term = {"type": "string", "minLength": 2, "maxLength": 64, "pattern": r"^[A-Za-z0-9 .\-]+$"}
    p_s = op(FEEDS[1], "kev_search", [q("vendor", False, "Vendor name (vendor and/or product required)", term),
                                      q("product", False, "Product name", term),
                                      q("min_quality", False, "exact|word|substring", {"type": "string", "enum": ["exact", "word", "substring"], "default": "word"}),
                                      q("limit", False, "1-100", {"type": "integer", "minimum": 1, "maximum": 100, "default": 25})])
    return {"/v1/kev": p_cve, "/v1/kev/search": p_s}


def llms_lines() -> str:
    return "".join(f"- [{f['title']} (${f['price_usd']:.3f})]({f['url']}): {f['description']}\n" for f in FEEDS)


def well_known_urls() -> list[str]:
    return [BASE + f["path"] for f in FEEDS]


def merge_feeds(existing: list[dict]) -> list[dict]:
    """Return existing feeds + KEV feeds. Existing entries are deep-copied, never modified. Refuses path collisions."""
    have = {e["path"] for e in existing}
    clash = have & {f["path"] for f in FEEDS}
    if clash:
        raise ValueError(f"path already exists: {sorted(clash)}")
    return copy.deepcopy(existing) + copy.deepcopy(FEEDS)


def merge_openapi_paths(existing_paths: dict) -> dict:
    clash = set(existing_paths) & set(openapi_paths())
    if clash:
        raise ValueError(f"openapi path already exists: {sorted(clash)}")
    out = copy.deepcopy(existing_paths); out.update(openapi_paths()); return out


if __name__ == "__main__":
    d = Path(__file__).resolve().parent
    (d / "feeds_kev.json").write_text(json.dumps(FEEDS, indent=1), encoding="utf-8")
    (d / "openapi_kev.json").write_text(json.dumps(openapi_paths(), indent=1), encoding="utf-8")
    (d / "llms_kev.txt").write_text(llms_lines(), encoding="utf-8")
    print("wrote feeds_kev.json, openapi_kev.json, llms_kev.txt")
