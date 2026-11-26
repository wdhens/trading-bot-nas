# KEV lookup: NAS package (pay-server/kev/)

Status: **prepared and tested offline on the PC; not merged, not deployed, routes not published.** Built 2026-11-26.
Decisions (routes, cache, price) and their reasons are in `../REVIEW_NOTE.md`.

## Patch description

This adds two paid x402 routes backed by the CISA Known Exploited Vulnerabilities catalog (CC0). It changes nothing in the existing 19 feeds, payTo, wallets, ledger or network.

| Route | Price | Answers |
|---|---|---|
| `GET /v1/kev?cve=CVE-YYYY-NNNN` | $0.005 (basic tier, 5000 atomic) | Is the CVE known-exploited? Also returns dateAdded, dueDate, requiredAction and knownRansomwareCampaignUse |
| `GET /v1/kev/search?vendor=&product=&min_quality=&limit=` | $0.01 (premium tier, 10000 atomic) | KEV entries for a vendor and/or product, each labelled with match quality (default `word`) |

**Charging rules:**
- An invalid query gets a **free 400** before any payment is requested. CVE must match `^CVE-\d{4}-\d{4,7}$`. Vendor/product must be 2-64 characters: letters, digits, spaces, dots and dashes. Repeated or unknown parameters are rejected.
- A missing, stale (>7 days) or tampered catalog, or any internal error, returns **503 and is not settled**, in line with the server's "only charged when data is delivered" rule.

| File | Purpose |
|---|---|
| `kev_lookup.py` | Core: fetch with a contact-free User-Agent, never-overwrite cache with sha256, `prune_cache(keep=10)`, matching, `validate_request()` |
| `kev_routes.py` | Framework-agnostic handlers: `pre_payment(path, query)` runs before the x402 check; `KevService.respond(pre)` runs after verification and before settlement; `KevService.health()` for source-health |
| `kev_fetch_daily.py` | Scheduler job: fetch, then keep the newest 10 versions. One JSON line of output; exit 0/1/2 |
| `feed_entries.py` | Add-only merge helpers and the snippet source for `/v1/feeds`, openapi, llms.txt and `.well-known/x402` |
| `feeds_kev.json`, `openapi_kev.json`, `llms_kev.txt` | Generated snippets (from `python feed_entries.py`) |
| `existing_feeds_baseline.json` | The 19 existing feeds (path/tier/price) as published on 2026-11-25 |
| `verify_existing_feeds.py` | Post-install check on two saved `/v1/feeds` files: the 19 entries are unchanged and only the 2 KEV routes were added |
| `tests/` | 45 offline tests (`test_package.py` 17, `test_kev_lookup.py` 28) plus the fixture |
| `.gitignore` | Excludes `cache/` and `__pycache__/` |

## Wiring into the pay server (pseudo-code)

The pay-server source was not available on the PC, so the exact hook names depend on its framework.

```python
from kev import kev_routes            # package folder pay-server/kev/ on sys.path
KEV = kev_routes.KevService(cache_dir=Path("pay-server/kev/cache"))

# in the request handler, for path in kev_routes.ROUTES, BEFORE the x402 middleware/check:
pre = kev_routes.pre_payment(path, query_params)
if pre.status == 400:
    return json_response(pre.body, status=400)          # no PAYMENT-REQUIRED header -> free
# normal x402 flow with amount = pre.price_usdc_atomic (unpaid -> 402 quote, as for the other feeds)
# after the payment signature is verified, before settle():
status, body = KEV.respond(pre)
if status != 200:
    return json_response(body, status=status)           # do NOT settle
settle(); return json_response(body, 200)

# source-health: sources["cisa_kev"] = KEV.health()
```

## Install (owner approves once; nobody has done any of this yet)

1. On a branch, copy this folder to `pay-server/kev/`. Change no existing file except the router/handler hook and the source-health dict.
2. On the NAS: `cd pay-server/kev && python3 -m unittest discover -s tests` must report `Ran 45 tests ... OK`.
3. Save the current feed list: `curl -s https://strawberryking.synology.me/v1/feeds > /tmp/before.json`.
4. Run `python3 kev_fetch_daily.py` once and check that it prints `"ok": true`.
5. Add the router hook and source-health entry from the pseudo-code above, then restart the pay server (owner action).
6. Free checks, no payment:
   - `GET /v1/kev?cve=bad` → 400, with no PAYMENT-REQUIRED header
   - `GET /v1/kev?cve=CVE-2021-44228` → 402 quoting 5000
   - `GET /v1/kev/search?vendor=cisco` → 402 quoting 10000
   - `/v1/source-health` shows `cisa_kev.ok: true`
7. Schedule `python3 kev_fetch_daily.py` daily in the Synology Task Scheduler, e.g. at 06:30.
8. Listing, only after steps 2-7 pass: merge `feeds_kev.json` into `/v1/feeds` with `merge_feeds()`, `openapi_kev.json` into `/openapi.json` paths with `merge_openapi_paths()`, append `llms_kev.txt` to `/llms.txt`, and add the two URLs to `/.well-known/x402`. Then: `curl -s .../v1/feeds > /tmp/after.json && python3 verify_existing_feeds.py /tmp/before.json /tmp/after.json` must print `"ok": true`.
9. Bazaar listing appears after the first settled payment. A self-paid test call is a blockchain transaction: the owner's decision, and it counts as own-wallet, not outside demand.
10. Review after 60 days on the Bazaar's public `l30DaysUniquePayers`; if it is 0, roll back.

## Rollback (the other 19 feeds are untouched)

1. Remove the router hook and the `cisa_kev` source-health line.
2. Remove the 2 entries from `/v1/feeds`, openapi and llms.txt, and the 2 URLs from `.well-known/x402`.
3. Delete the daily task. Optionally delete `pay-server/kev/`, including `cache/`.
4. Restart the pay server. Run `verify_existing_feeds.py` with the pre-install `before.json` and the current list: it should report only the KEV routes as missing, with no other change.
5. The ledger is untouched. KEV sales stay recorded as normal sales.

## Tested / not tested

- Tested on the PC with Python 3.12: 45/45 offline tests pass. Coverage:
  - free 400 before payment;
  - prices equal the existing tiers;
  - 503 and no settlement on a missing, stale or tampered cache;
  - catalog reload;
  - daily fetch, prune, and fetch failure keeping the old cache;
  - no path collision, and the 19 existing entries byte-identical after the add-only merge (openapi too);
  - the verify script catches changes and removals.
- The core module was also run live against the CISA feed (1,805 entries).
- A local-model code review (gpt-oss) raised 8 points. Taken: a clearer health threshold constant, a catch-all that returns 503 (never settle on an unexpected error), and a clearer health docstring. The rest were not bugs: every 400 body carries `charged: false`, the 402 params always include `min_quality`/`limit`, and the 48 h health vs 7 d serve limits are intentional.
- **Not tested:** the NAS's Python version and ARM CPU, the real router/x402 middleware hook (the source isn't on the PC), the Synology scheduler, a real payment.

## SHA-256 of each file

See `SHA256SUMS.txt`, created right after the last test run.
