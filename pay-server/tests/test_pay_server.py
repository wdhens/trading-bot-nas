"""Offline tests for the pay server. Run: python3 -m unittest discover -s pay-server/tests
Uses the newest release folder (or PAY_RELEASE_DIR)."""
import importlib.util
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REL = os.environ.get("PAY_RELEASE_DIR") or os.path.join(
    HERE, "..", "releases", json.load(open(os.path.join(HERE, "..", "latest.json")))["version"])
os.environ.setdefault("PAY_DATA_DIR", tempfile.mkdtemp())


def load():
    sp = importlib.util.spec_from_file_location("nas_pay_server", os.path.join(REL, "nas_pay_server.py"))
    m = importlib.util.module_from_spec(sp)
    sys.modules["nas_pay_server"] = m
    sp.loader.exec_module(m)
    return m


m = load()


class BookTests(unittest.TestCase):
    ASKS = [(100.0, 1.0), (101.0, 1.0), (102.0, 10.0)]
    BIDS = [(99.0, 1.0), (98.0, 1.0), (97.0, 10.0)]

    def test_walk_exact_first_level(self):
        r = m.walk_book(self.ASKS, 50)
        self.assertEqual(r["slippage_bps"], 0.0)

    def test_walk_two_levels(self):
        r = m.walk_book(self.ASKS, 150)           # 1.0@100 + 0.4950@101
        self.assertAlmostEqual(r["avg_price"], 150 / (1 + 50 / 101), places=6)
        self.assertGreater(r["slippage_bps"], 0)

    def test_unfillable_is_none(self):
        self.assertIsNone(m.walk_book(self.ASKS[:1], 500))
        self.assertIsNone(m.walk_book([], 100))

    def test_metrics(self):
        r = m.book_metrics(self.BIDS, self.ASKS)
        self.assertAlmostEqual(r["spread_bps"], 1 / 99.5 * 1e4, places=1)
        self.assertEqual(r["slippage_bps"]["100"]["buy"] >= 0, True)
        self.assertIsNone(m.book_metrics([], self.ASKS))


class CrowdingTests(unittest.TestCase):
    def test_crowded_long(self):
        r = m.crowding_score([90, 80], 2.5, 1.8, 8)
        self.assertEqual(r["label"], "crowded_long")
        self.assertLessEqual(r["score"], 100)

    def test_crowded_short(self):
        r = m.crowding_score([-90], 0.4, 0.5, 5)
        self.assertEqual(r["label"], "crowded_short")

    def test_balanced_and_missing(self):
        r = m.crowding_score([2], 1.0, 1.0, None)
        self.assertEqual(r["label"], "balanced")
        self.assertIn("oi_contracts_change_24h", r["missing"])

    def test_no_inputs(self):
        r = m.crowding_score([], None, None, None)
        self.assertIsNone(r["score"])
        self.assertEqual(r["label"], "insufficient_data")


class LedgerTests(unittest.TestCase):
    def setUp(self):
        # fresh in-memory ledger for every test
        m._SALES.update({"count": 0, "usdc": 0.0, "by_path": {}, "last_ts": None, "ext_count": 0, "ext_usdc": 0.0,
                         "payers": {}, "self_count": 0, "self_usdc": 0.0, "daily": {}, "unk_count": 0,
                         "unk_usdc": 0.0, "quotes_daily": {}})
        self.feed = m.FEEDS_BY_PATH["/v1/signal"]
        self._self, self._from = set(m.SELF_WALLETS), m.OUTSIDE_FROM

    def tearDown(self):
        m.SELF_WALLETS.clear(); m.SELF_WALLETS.update(self._self); m.OUTSIDE_FROM = self._from

    def test_self_vs_external(self):
        m.record_sale(self.feed, {"payer": m.PAY_TO, "transaction": "0x1"})
        m.record_sale(self.feed, {"payer": "0xAbC0000000000000000000000000000000001234", "transaction": "0x2"})
        m.record_sale(self.feed, {"payer": "0xabc0000000000000000000000000000000001234", "transaction": "0x3"})
        s = m.public_stats()
        self.assertEqual(s["own_wallet_sales"]["sales"], 1)
        self.assertEqual(s["external"]["sales"], 2)
        self.assertEqual(s["external"]["paying_wallets"], 1)
        self.assertEqual(s["external"]["repeat_wallets"], 1)

    def test_test_payments_from_listed_paying_wallet_are_own(self):
        mine = "0x1111111111111111111111111111111111111111"
        m.SELF_WALLETS.add(mine)
        for _ in range(17):
            m.record_sale(self.feed, {"payer": mine})
        s = m.public_stats()
        self.assertEqual(s["external"]["sales"], 0)
        self.assertEqual(s["own_wallet_sales"]["sales"], 17)
        self.assertFalse(s["external"]["milestone_reached"])

    def test_unknown_payer_never_counts_as_outside(self):
        m.record_sale(self.feed, {})
        m.record_sale(self.feed, {"payer": None})
        s = m.public_stats()
        self.assertEqual(s["external"]["sales"], 0)
        self.assertEqual(s["unknown_payer_sales"]["sales"], 2)
        self.assertEqual(s["external"]["usdc"], 0)

    def test_sales_before_outside_from_are_own(self):
        m.OUTSIDE_FROM = int(__import__("time").time()) + 3600
        m.record_sale(self.feed, {"payer": "0x2222222222222222222222222222222222222222"})
        self.assertEqual(m.public_stats()["external"]["sales"], 0)
        self.assertEqual(m.public_stats()["own_wallet_sales"]["sales"], 1)

    def test_milestone_and_forwarded(self):
        f = dict(self.feed, price="25000000")   # $25
        m.record_sale(f, {"payer": "0x3333333333333333333333333333333333333333"})
        s = m.public_stats()
        self.assertTrue(s["external"]["milestone_reached"])
        old = m.FORWARDED_USD
        try:
            m.FORWARDED_USD = 25.0
            s2 = m.public_stats()
            self.assertFalse(s2["external"]["milestone_reached"])
            self.assertEqual(s2["external"]["unforwarded_usd"], 0.0)
        finally:
            m.FORWARDED_USD = old

    def test_quotes_daily(self):
        m.record_quote("/v1/signal"); m.record_quote("/v1/signal")
        self.assertEqual(sum(m.public_stats()["quotes_daily_last_30"].values()), 2)


class LighterGuardTests(unittest.TestCase):
    def test_quiet_window(self):
        import calendar
        t = lambda h, mi: calendar.timegm((2026, 11, 23, h, mi, 0))
        self.assertTrue(m.lighter_quiet_now(t(0, 10)))
        self.assertTrue(m.lighter_quiet_now(t(21, 10)))
        self.assertFalse(m.lighter_quiet_now(t(12, 0)))
        self.assertFalse(m.lighter_quiet_now(t(0, 30)))

    def test_budget(self):
        old_calls, old_q = m._http_get_json, m.LIGHTER_QUIET
        calls = []
        m._http_get_json = lambda url, timeout=8, headers=None: calls.append(url) or {}
        m.LIGHTER_QUIET = ""
        m._LIGHTER_STAMPS.clear()
        try:
            for _ in range(m.LIGHTER_MAX_PER_MIN):
                m._lighter_get("/x")
            with self.assertRaises(RuntimeError):
                m._lighter_get("/x")
            self.assertEqual(len(calls), m.LIGHTER_MAX_PER_MIN)
        finally:
            m._http_get_json, m.LIGHTER_QUIET = old_calls, old_q
            m._LIGHTER_STAMPS.clear()


class CatalogTests(unittest.TestCase):
    def test_feeds_wellformed(self):
        paths = [f["path"] for f in m.FEEDS]
        self.assertEqual(len(paths), len(set(paths)))
        for f in m.FEEDS:
            self.assertLessEqual(len(f["description"]), 500, f["path"])
            self.assertTrue(f["example"], f["path"])
            self.assertIn(f["tier"], ("basic", "premium", "bundle"))
            self.assertGreater(int(f["price"]), 0)
        self.assertLessEqual(len(m.FEEDS) + 8, m.CACHE_MAX_ENTRIES)

    def test_crowding_wording(self):
        self.assertIn("not a backtested signal", m.FEEDS_BY_PATH["/v1/crowding"]["description"])

    def test_new_feeds_present(self):
        self.assertIn("/v1/crowding", m.FEEDS_BY_PATH)
        self.assertIn("/v1/lighter-depth", m.FEEDS_BY_PATH)

    def test_payto_unchanged(self):
        self.assertEqual(m.PAY_TO.lower(), "0xc9eebecb1d0aff4fb2b9978516e075a33639892c")


if __name__ == "__main__":
    unittest.main()
