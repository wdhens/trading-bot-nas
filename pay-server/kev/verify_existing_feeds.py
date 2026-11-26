"""Post-install check (offline, on saved files): the 19 existing feeds are unchanged and only the 2 KEV feeds were added.
    python3 verify_existing_feeds.py BEFORE_feeds.json AFTER_feeds.json
Save the two files from /v1/feeds before and after the install (e.g. curl -s .../v1/feeds > before.json).
Exit 0 = ok, 1 = a difference was found (details on stdout)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
KEV_PATHS = {"/v1/kev", "/v1/kev/search"}


def _entries(doc) -> dict:
    eps = doc["endpoints"] if isinstance(doc, dict) and "endpoints" in doc else doc
    return {e["path"]: e for e in eps}


def check(before_doc, after_doc, baseline=None) -> list[str]:
    baseline = baseline or json.loads((HERE / "existing_feeds_baseline.json").read_text(encoding="utf-8"))
    b, a = _entries(before_doc), _entries(after_doc)
    problems = []
    base = {f["path"]: f for f in baseline["feeds"]}
    for p, f in base.items():
        if p not in b:
            problems.append(f"baseline feed missing BEFORE install: {p}")
        elif any(str(b[p].get(k)) != str(v) for k, v in f.items()):
            problems.append(f"BEFORE differs from 2026-11-25 baseline for {p} (tier/price changed earlier)")
    for p, e in b.items():
        if p not in a:
            problems.append(f"feed removed by install: {p}")
        elif json.dumps(e, sort_keys=True) != json.dumps(a[p], sort_keys=True):
            problems.append(f"feed changed by install: {p}")
    added = set(a) - set(b)
    if added != KEV_PATHS - set(b):
        problems.append(f"unexpected additions: {sorted(added - KEV_PATHS)}; missing KEV: {sorted(KEV_PATHS - set(a))}")
    return problems


if __name__ == "__main__":
    before, after = (json.loads(Path(p).read_text(encoding="utf-8")) for p in sys.argv[1:3])
    probs = check(before, after)
    print(json.dumps({"ok": not probs, "problems": probs}, indent=1))
    sys.exit(1 if probs else 0)
