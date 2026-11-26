"""Daily KEV catalog refresh for the NAS task scheduler.
    python3 kev_fetch_daily.py [--cache DIR] [--keep 10]
Fetches the CISA feed (never overwrites cache files), then keeps the newest N versions. Exit 0 = ok, 1 = fetch failed
(the old cache stays in use), 2 = prune failed. One JSON line on stdout for the scheduler log."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import kev_lookup as kl


def run(cache: Path, keep: int, getter=kl._http_get) -> tuple[int, dict]:
    try:
        meta = kl.fetch(cache, getter=getter)
    except Exception as e:                       # network, bad feed, disk
        return 1, {"ok": False, "stage": "fetch", "error": str(e)[:300]}
    try:
        deleted = kl.prune_cache(cache, keep=keep)
    except Exception as e:
        return 2, {"ok": False, "stage": "prune", "error": str(e)[:300], "fetch": meta}
    return 0, {"ok": True, "catalogVersion": meta.get("catalogVersion"), "count": meta.get("count"),
               "new_file": meta.get("new_file"), "sha256": meta.get("sha256"), "pruned": len(deleted) // 2}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=str(kl.DEFAULT_CACHE))
    ap.add_argument("--keep", type=int, default=kl.KEEP_VERSIONS)
    a = ap.parse_args(argv)
    code, out = run(Path(a.cache), a.keep)
    print(json.dumps(out))
    return code


if __name__ == "__main__":
    sys.exit(main())
