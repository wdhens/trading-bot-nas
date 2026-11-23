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
    def test_self_vs_external(self):
        feed = m.FEEDS_BY_PATH["/v1/signal"]
        before = m.public_stats()
        m.record_sale(feed, {"payer": m.PAY_TO, "transaction": "0x1"})
        m.record_sale(feed, {"payer": "0xAbC", "transaction": "0x2"})
        m.record_sale(feed, {"payer": "0xabc", "transaction": "0x3"})
        s = m.public_stats()
        self.assertEqual(s["own_wallet_sales"]["sales"] - before["own_wallet_sales"]["sales"], 1)
        self.assertEqual(s["external"]["sales"] - before["external"]["sales"], 2)
        self.assertEqual(s["external"]["paying_wallets"] - before["external"]["paying_wallets"], 1)
        self.assertGreaterEqual(s["external"]["repeat_wallets"], 1)


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

    def test_new_feeds_present(self):
        self.assertIn("/v1/crowding", m.FEEDS_BY_PATH)
        self.assertIn("/v1/lighter-depth", m.FEEDS_BY_PATH)

    def test_payto_unchanged(self):
        self.assertEqual(m.PAY_TO.lower(), "0xc9eebecb1d0aff4fb2b9978516e075a33639892c")


if __name__ == "__main__":
    unittest.main()
