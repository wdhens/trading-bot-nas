"""Offline tests for the NAS package. From nas-package/:  python -m unittest discover -s tests -v"""
import datetime as dt
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
PKG = HERE.parent
sys.path.insert(0, str(PKG))
import kev_lookup as kl            # noqa: E402
import kev_routes as kr            # noqa: E402
import kev_fetch_daily as kfd      # noqa: E402
import feed_entries as fe          # noqa: E402
import verify_existing_feeds as vef  # noqa: E402

FIXTURE = (HERE / "fixture_kev.json").read_bytes()
BASELINE = json.loads((PKG / "existing_feeds_baseline.json").read_text(encoding="utf-8"))
NOW = dt.datetime(2026, 11, 26, 8, 0, 0, tzinfo=dt.timezone.utc)


def getter(payload):
    return lambda url: payload


def existing_feeds():
    """The 19 existing feed entries as /v1/feeds lists them (baseline fields + typical extra fields)."""
    return [{**f, "url": "https://strawberryking.synology.me" + f["path"], "network": "eip155:8453",
             "description": f"existing feed {f['path']}"} for f in BASELINE["feeds"]]


class PrePaymentTests(unittest.TestCase):
    def test_invalid_input_is_free_400(self):
        cases = [("/v1/kev", {"cve": "nope"}), ("/v1/kev", {}), ("/v1/kev", {"cve": ["CVE-2021-44228", "CVE-2021-1"]}),
                 ("/v1/kev/search", {"vendor": "a"}), ("/v1/kev/search", {"vendor": "Notepad++"}),
                 ("/v1/kev/search", {}), ("/v1/kev/search", {"vendor": "cisco", "limit": "999"}),
                 ("/v1/kev/x", {"cve": "CVE-2021-44228"}), ("/v1/funding", {})]
        for path, q in cases:
            pre = kr.pre_payment(path, q)
            self.assertEqual(pre.status, 400, (path, q))
            self.assertFalse(pre.body["charged"])
            self.assertIsNone(pre.price_usdc_atomic)

    def test_valid_input_asks_payment_at_route_price(self):
        pre = kr.pre_payment("/v1/kev", {"cve": ["cve-2021-44228"]})       # parse_qs style
        self.assertEqual((pre.status, pre.price_usdc_atomic, pre.params), (402, "5000", {"cve": "CVE-2021-44228"}))
        pre = kr.pre_payment("/v1/kev/search/", {"vendor": "SAP"})
        self.assertEqual((pre.status, pre.price_usdc_atomic), (402, "10000"))
        self.assertEqual(pre.params["min_quality"], "word")

    def test_prices_match_existing_tiers(self):
        tiers = {f["tier"]: f["price_usdc_atomic"] for f in BASELINE["feeds"]}
        for r in kr.ROUTES.values():
            self.assertEqual(tiers[r["tier"]], r["price_usdc_atomic"])


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def svc(self, now=NOW):
        return kr.KevService(self.dir, now=lambda: now)

    def test_no_cache_is_503_unsettled(self):
        st, body = self.svc().respond(kr.pre_payment("/v1/kev", {"cve": "CVE-2021-44228"}))
        self.assertEqual(st, 503); self.assertFalse(body["charged"])
        self.assertFalse(self.svc().health()["ok"])

    def test_paid_lookup_and_search(self):
        kl.fetch(self.dir, getter=getter(FIXTURE), now=NOW - dt.timedelta(hours=2))
        s = self.svc()
        st, body = s.respond(kr.pre_payment("/v1/kev", {"cve": "CVE-2021-44228"}))
        self.assertEqual(st, 200); self.assertTrue(body["known_exploited"]); self.assertIn("not proof", body["note"])
        st, body = s.respond(kr.pre_payment("/v1/kev/search", {"vendor": "cisco", "product": "ios"}))
        self.assertEqual((st, body["total_matches"]), (200, 2))
        self.assertEqual(s.health(), {"ok": True, "age_hours": 2.0})

    def test_stale_cache_is_503_unsettled(self):
        kl.fetch(self.dir, getter=getter(FIXTURE), now=NOW - dt.timedelta(days=8))
        st, body = self.svc().respond(kr.pre_payment("/v1/kev", {"cve": "CVE-2021-44228"}))
        self.assertEqual(st, 503); self.assertFalse(body["charged"])

    def test_reloads_newer_catalog(self):
        kl.fetch(self.dir, getter=getter(FIXTURE), now=NOW - dt.timedelta(hours=5))
        s = self.svc()
        self.assertTrue(s.respond(kr.pre_payment("/v1/kev", {"cve": "CVE-2021-44228"}))[1]["known_exploited"])
        data = json.loads(FIXTURE); data["catalogVersion"] = "new"
        data["vulnerabilities"] = [v for v in data["vulnerabilities"] if v["cveID"] != "CVE-2021-44228"]
        kl.fetch(self.dir, getter=getter(json.dumps(data).encode()), now=NOW - dt.timedelta(hours=1))
        st, body = s.respond(kr.pre_payment("/v1/kev", {"cve": "CVE-2021-44228"}))
        self.assertFalse(body["known_exploited"]); self.assertEqual(body["provenance"]["catalogVersion"], "new")

    def test_tampered_cache_is_503(self):
        m = kl.fetch(self.dir, getter=getter(FIXTURE), now=NOW)
        (self.dir / m["data_file"]).write_bytes(FIXTURE.replace(b"Apache", b"Apachx"))
        st, _ = self.svc().respond(kr.pre_payment("/v1/kev", {"cve": "CVE-2021-44228"}))
        self.assertEqual(st, 503)

    def test_respond_refuses_rejected_request(self):
        st, _ = self.svc().respond(kr.pre_payment("/v1/kev", {"cve": "x"}))
        self.assertEqual(st, 400)


class DailyFetchTests(unittest.TestCase):
    def test_fetch_and_prune(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            for n in range(12):
                data = json.loads(FIXTURE); data["catalogVersion"] = f"v{n}"
                code, out = kfd.run(d, 10, getter=getter(json.dumps(data).encode()))
                self.assertEqual(code, 0, out)
            self.assertEqual(len(list(d.glob("kev_*.meta.json"))), 10)
            self.assertEqual(len((d / "fetch_log.jsonl").read_text(encoding="utf-8").splitlines()), 12)

    def test_fetch_failure_keeps_old_cache(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            kfd.run(d, 10, getter=getter(FIXTURE))
            def boom(url): raise OSError("network down")
            code, out = kfd.run(d, 10, getter=boom)
            self.assertEqual((code, out["stage"]), (1, "fetch"))
            self.assertEqual(kl.load_latest(d).provenance()["catalogVersion"], "2099.01.01")


class ExistingFeedsUnchangedTests(unittest.TestCase):
    def test_baseline_has_the_19_feeds(self):
        self.assertEqual(len(BASELINE["feeds"]), 19)
        self.assertEqual(len({f["path"] for f in BASELINE["feeds"]}), 19)

    def test_kev_paths_do_not_collide(self):
        paths = {f["path"] for f in BASELINE["feeds"]}
        self.assertFalse(paths & set(kr.ROUTES))
        self.assertEqual({f["path"] for f in fe.FEEDS}, set(kr.ROUTES))

    def test_merge_leaves_19_existing_entries_identical(self):
        before = existing_feeds()
        frozen = json.dumps(before, sort_keys=True)
        after = fe.merge_feeds(before)
        self.assertEqual(json.dumps(before, sort_keys=True), frozen)        # input not mutated
        self.assertEqual(json.dumps(after[:19], sort_keys=True), frozen)     # existing entries identical, same order
        self.assertEqual([f["path"] for f in after[19:]], ["/v1/kev", "/v1/kev/search"])
        with self.assertRaises(ValueError):
            fe.merge_feeds(after)                                           # no duplicate add

    def test_openapi_merge_add_only(self):
        existing = {f["path"]: {"get": {"operationId": f["path"]}} for f in BASELINE["feeds"]}
        frozen = json.dumps(existing, sort_keys=True)
        merged = fe.merge_openapi_paths(existing)
        self.assertEqual(json.dumps(existing, sort_keys=True), frozen)
        self.assertEqual(json.dumps({k: merged[k] for k in existing}, sort_keys=True), frozen)
        self.assertEqual(set(merged) - set(existing), {"/v1/kev", "/v1/kev/search"})

    def test_verify_script_detects_changes(self):
        before = {"endpoints": existing_feeds()}
        after = {"endpoints": fe.merge_feeds(existing_feeds())}
        self.assertEqual(vef.check(before, after), [])
        tampered = json.loads(json.dumps(after)); tampered["endpoints"][0]["price_usdc_atomic"] = "1"
        self.assertTrue(any("changed" in p for p in vef.check(before, tampered)))
        removed = {"endpoints": after["endpoints"][1:]}
        self.assertTrue(any("removed" in p for p in vef.check(before, removed)))


class SnippetTests(unittest.TestCase):
    def test_snippets(self):
        for f in fe.FEEDS:
            self.assertIn("CC0", f["description"]); self.assertIn("not proof of safety", f["description"])
            self.assertEqual(f["tags"], ["security", "vulnerability", "cisa", "kev"])
        oa = fe.openapi_paths()
        self.assertEqual(oa["/v1/kev"]["get"]["x-payment-info"]["amount"], "5000")
        self.assertEqual(oa["/v1/kev/search"]["get"]["x-payment-info"]["amount"], "10000")
        self.assertEqual(oa["/v1/kev"]["get"]["x-payment-info"]["payTo"], "0xC9eebecb1d0AfF4fb2B9978516E075A33639892C")
        self.assertEqual(fe.llms_lines().count("\n"), 2)


if __name__ == "__main__":
    unittest.main()
