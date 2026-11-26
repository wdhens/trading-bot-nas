"""Offline tests. Run from the kev-lookup folder:  python -m unittest discover -s tests -v"""
import datetime as dt
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import kev_lookup as kl  # noqa: E402

FIXTURE = (HERE / "fixture_kev.json").read_bytes()


def getter_for(payload: bytes):
    def g(url):
        assert url == kl.FEED_URL
        return payload
    return g


def t(n):  # deterministic fetch times
    return dt.datetime(2026, 11, 26, 7, 0, n, tzinfo=dt.timezone.utc)


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_fetch_writes_data_meta_and_sha(self):
        meta = kl.fetch(self.dir, getter=getter_for(FIXTURE), now=t(0))
        self.assertTrue(meta["new_file"])
        self.assertEqual(meta["sha256"], hashlib.sha256(FIXTURE).hexdigest())
        self.assertEqual(meta["fetched_at"], "2026-11-26T07:00:00Z")
        self.assertEqual((self.dir / meta["data_file"]).read_bytes(), FIXTURE)
        self.assertEqual(meta["count"], 9)

    def test_same_content_does_not_write_new_file(self):
        m1 = kl.fetch(self.dir, getter=getter_for(FIXTURE), now=t(0))
        m2 = kl.fetch(self.dir, getter=getter_for(FIXTURE), now=t(1))
        self.assertFalse(m2["new_file"])
        self.assertEqual(m1["data_file"], m2["data_file"])
        self.assertEqual(len(list(self.dir.glob("kev_*.json"))) - len(list(self.dir.glob("kev_*.meta.json"))), 1)
        log = (self.dir / "fetch_log.jsonl").read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(log), 2)

    def test_new_content_keeps_old_file_untouched(self):
        m1 = kl.fetch(self.dir, getter=getter_for(FIXTURE), now=t(0))
        old = (self.dir / m1["data_file"]).read_bytes()
        data = json.loads(FIXTURE); data["catalogVersion"] = "2099.01.02"
        m2 = kl.fetch(self.dir, getter=getter_for(json.dumps(data).encode()), now=t(5))
        self.assertTrue(m2["new_file"])
        self.assertNotEqual(m1["data_file"], m2["data_file"])
        self.assertEqual((self.dir / m1["data_file"]).read_bytes(), old)
        self.assertEqual(kl.load_latest(self.dir).provenance()["catalogVersion"], "2099.01.02")

    def test_never_overwrites_existing_file(self):
        kl.fetch(self.dir, getter=getter_for(FIXTURE), now=t(0))
        data = json.loads(FIXTURE); data["catalogVersion"] = "x"
        raw = json.dumps(data).encode()
        sha = hashlib.sha256(raw).hexdigest()
        (self.dir / f"kev_20261126T070000Z_{sha[:12]}.json").write_bytes(b"pre-existing")
        with self.assertRaises(FileExistsError):
            kl.fetch(self.dir, getter=getter_for(raw), now=t(0))
        self.assertEqual((self.dir / f"kev_20261126T070000Z_{sha[:12]}.json").read_bytes(), b"pre-existing")

    def test_tampered_cache_is_rejected(self):
        m = kl.fetch(self.dir, getter=getter_for(FIXTURE), now=t(0))
        p = self.dir / m["data_file"]
        p.write_bytes(FIXTURE.replace(b"Log4j2", b"Log4jX"))
        with self.assertRaises(kl.KevError):
            kl.load_latest(self.dir)

    def test_bad_feeds_rejected_and_nothing_written(self):
        for bad in (b"not json", b"{}", b'{"vulnerabilities": []}', b'{"vulnerabilities": [{"cveID": "X"}]}'):
            with self.assertRaises(kl.KevError):
                kl.fetch(self.dir, getter=getter_for(bad), now=t(0))
        self.assertEqual(list(self.dir.glob("kev_*")), [])

    def test_no_cache(self):
        with self.assertRaises(kl.KevError):
            kl.load_latest(self.dir)


class LookupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cat = kl.Catalog(json.loads(FIXTURE), {"fetched_at": "f", "sha256": "s"})

    def test_cve_found_with_fields(self):
        r = self.cat.lookup_cve("CVE-2021-44228")
        self.assertTrue(r["known_exploited"])
        e = r["entry"]
        self.assertEqual(e["knownRansomwareCampaignUse"], "Known")
        self.assertEqual(e["dateAdded"], "2021-12-10")
        self.assertEqual(e["dueDate"], "2021-12-24")
        self.assertIn("Apply updates", e["requiredAction"])
        self.assertIn("CC0", r["provenance"]["source"])

    def test_cve_normalised(self):
        self.assertTrue(self.cat.lookup_cve("  cve-2021-44228 ")["known_exploited"])

    def test_cve_not_in_catalog(self):
        r = self.cat.lookup_cve("CVE-2024-99999")
        self.assertFalse(r["known_exploited"]); self.assertIsNone(r["entry"])

    def test_cve_invalid(self):
        for bad in ("2021-44228", "CVE-21-1", "CVE-2021-12", "CVE-2021-44228; DROP", ""):
            with self.assertRaises(kl.KevError):
                self.cat.lookup_cve(bad)

    def ids(self, **kw):
        return [x["cveID"] for x in self.cat.search(**kw)["results"]]

    def quality(self, **kw):
        return {x["cveID"]: x["match_quality"] for x in self.cat.search(**kw)["results"]}

    def test_vendor_case_insensitive(self):
        self.assertEqual(sorted(self.ids(vendor="APACHE")), ["CVE-2021-41773", "CVE-2021-44228"])

    def test_product_substring_per_spec(self):
        q = self.quality(product="log4j")
        self.assertEqual(q, {"CVE-2021-44228": "substring"})

    def test_short_query_no_substring(self):
        # 'go' must not match 'Google' (vendor) and 'on' must not match 'Chromium'/'Devices' etc.
        self.assertEqual(self.ids(vendor="go"), [])
        r = self.cat.search(product="on")
        self.assertEqual([x["cveID"] for x in r["results"]], ["CVE-2019-5418"])   # 'Ruby on Rails' whole word
        self.assertEqual(r["results"][0]["match_quality"], "word")
        self.assertTrue(r["notes"])

    def test_ios_tiers(self):
        q = self.quality(vendor="cisco", product="ios")
        self.assertEqual(q, {"CVE-2018-0171": "exact", "CVE-2023-20198": "word"})
        self.assertEqual(self.ids(vendor="cisco", product="ios", min_quality="exact"), ["CVE-2018-0171"])

    def test_punctuation_variants(self):
        self.assertEqual(self.quality(vendor="dlink"), {"CVE-2020-25078": "exact"})
        self.assertEqual(self.quality(vendor="d link"), {"CVE-2020-25078": "exact"})
        self.assertEqual(self.quality(product="http server"), {"CVE-2021-41773": "exact"})

    def test_three_letter_query_no_substring(self):
        # 'ios' (3 chars) must not substring-match e.g. 'FortiOS'; only exact/word
        cat = kl.Catalog({"vulnerabilities": [
            {"cveID": "CVE-2020-0001", "vendorProject": "Fortinet", "product": "FortiOS"},
            {"cveID": "CVE-2020-0002", "vendorProject": "Apple", "product": "iOS and iPadOS"}]})
        self.assertEqual([x["cveID"] for x in cat.search(product="ios")["results"]], ["CVE-2020-0002"])

    def test_edge_does_not_word_match_edgerouter(self):
        self.assertEqual(self.quality(product="edge"), {"CVE-2017-9805": "substring"})
        self.assertEqual(self.ids(product="edge", min_quality="word"), [])

    def test_results_sorted_quality_then_newest(self):
        r = self.cat.search(vendor="cisco", product="ios")["results"]
        self.assertEqual(r[0]["match_quality"], "exact")

    def test_ransomware_count_and_limit(self):
        r = self.cat.search(vendor="apache", limit=1)
        self.assertEqual(r["total_matches"], 2); self.assertEqual(r["returned"], 1)
        self.assertEqual(r["ransomware_known"], 1)

    def test_bad_search_input(self):
        for kw in ({}, {"vendor": "   "}, {"vendor": "---"}, {"vendor": "a" * 101}, {"vendor": "x", "min_quality": "fuzzy"}):
            with self.assertRaises(kl.KevError):
                self.cat.search(**kw)


class RegexSafety(unittest.TestCase):
    def test_regex_metacharacters(self):
        cat = kl.Catalog(json.loads(FIXTURE))
        with self.assertRaises(kl.KevError):
            cat.search(product=".*")                  # no letters/digits -> rejected
        self.assertEqual([x["cveID"] for x in cat.search(product="(webp)")["results"]], ["CVE-2023-4863"])


class PruneTests(unittest.TestCase):
    def test_keeps_newest_n_and_log(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            for n in range(13):
                data = json.loads(FIXTURE); data["catalogVersion"] = f"v{n}"
                kl.fetch(d, getter=getter_for(json.dumps(data).encode()), now=t(n))
            log_before = (d / "fetch_log.jsonl").read_text(encoding="utf-8")
            (d / "unrelated.txt").write_text("keep me")
            deleted = kl.prune_cache(d, keep=10)
            self.assertEqual(len(deleted), 6)                       # 3 versions x (data + meta)
            self.assertEqual(len(list(d.glob("kev_*.meta.json"))), 10)
            self.assertEqual(len(list(d.glob("kev_*.json"))), 20)
            self.assertEqual((d / "fetch_log.jsonl").read_text(encoding="utf-8"), log_before)
            self.assertTrue((d / "unrelated.txt").exists())
            self.assertEqual(kl.load_latest(d).provenance()["catalogVersion"], "v12")
            self.assertEqual(kl.prune_cache(d, keep=10), [])          # idempotent

    def test_keep_must_be_positive(self):
        with self.assertRaises(kl.KevError):
            kl.prune_cache(Path("."), keep=0)


class RouteValidationTests(unittest.TestCase):
    def ok(self, route, params):
        code, body = kl.validate_request(route, params)
        self.assertEqual(code, 200, body); return body

    def bad(self, route, params):
        code, body = kl.validate_request(route, params)
        self.assertEqual(code, 400, body); self.assertFalse(body["charged"]); return body

    def test_cve_route(self):
        self.assertEqual(self.ok("/v1/kev", {"cve": " cve-2021-44228 "}), {"cve": "CVE-2021-44228"})
        self.ok("/v1/kev", {"cve": "CVE-2024-1234567"})
        for v in ("CVE-2021-12345678", "CVE-21-44228", "2021-44228", "", "CVE-2021-44228'--"):
            self.bad("/v1/kev", {"cve": v})
        self.bad("/v1/kev", {})
        self.bad("/v1/kev", {"cve": "CVE-2021-44228", "x": "1"})

    def test_search_route(self):
        self.assertEqual(self.ok("/v1/kev/search", {"vendor": "SAP"}),
                         {"vendor": "SAP", "min_quality": "word", "limit": 25})       # 3-letter vendors allowed
        self.ok("/v1/kev/search", {"vendor": "F5"})
        self.ok("/v1/kev/search", {"vendor": "D-Link", "product": "DIR 823X", "min_quality": "exact", "limit": "100"})
        for p in ({}, {"vendor": "a"}, {"vendor": "a" * 65}, {"vendor": "Notepad++"}, {"vendor": "x<script>"},
                  {"vendor": "---"}, {"vendor": "cisco", "limit": "0"}, {"vendor": "cisco", "limit": "abc"},
                  {"vendor": "cisco", "min_quality": "fuzzy"}, {"vendor": "cisco", "evil": "1"}):
            self.bad("/v1/kev/search", p)
        self.bad("/v1/other", {"cve": "CVE-2021-44228"})

    def test_validated_params_work_on_catalog(self):
        cat = kl.Catalog(json.loads(FIXTURE))
        _, p = kl.validate_request("/v1/kev/search", {"vendor": "cisco", "product": "ios"})
        r = cat.search(p.get("vendor"), p.get("product"), p["min_quality"], p["limit"])
        self.assertEqual(r["total_matches"], 2)


class CliTests(unittest.TestCase):
    def test_cli_cve_and_search(self):
        with tempfile.TemporaryDirectory() as d:
            kl.fetch(Path(d), getter=getter_for(FIXTURE), now=t(0))
            self.assertEqual(kl.main(["--cache", d, "cve", "CVE-2021-44228"]), 0)
            self.assertEqual(kl.main(["--cache", d, "search", "--vendor", "cisco"]), 0)
            self.assertEqual(kl.main(["--cache", d, "cve", "nope"]), 2)


if __name__ == "__main__":
    unittest.main()
