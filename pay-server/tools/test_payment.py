#!/usr/bin/env python3
"""One-time x402 test payments from YOUR OWN wallet to the pay server (run on your PC, not on the NAS).

Why: the first settled payment lists each feed in the Coinbase Bazaar. Set PAY_SELF_WALLETS on the NAS to
this wallet first, so these payments count as own-wallet sales, not outside demand.

Setup (once):   py -3.12 -m pip install "x402[evm]" requests
Dry run:        py -3.12 test_payment.py            (no money moves)
Really pay:     py -3.12 test_payment.py --send     (one paid call per feed)
Key: only from environment variable PAY_TEST_PRIVATE_KEY. Never paste it into chat or a file.
Uses the official x402 Python client, so the payment format matches what the facilitator expects.
"""
import argparse, os, sys, time
import requests
from eth_account import Account
from x402 import x402ClientSync
from x402.mechanisms.evm.exact import ExactEvmScheme
from x402.mechanisms.evm.signers import EthAccountSigner
from x402.http.clients.requests import x402_requests

BASE = "https://strawberryking.synology.me"

def feed_list():
    d = requests.get(BASE + "/v1/feeds", timeout=20).json()
    return [(e["path"], int(e["price_usdc_atomic"])) for e in d["endpoints"]]

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--send", action="store_true"); a = ap.parse_args()
    key = os.environ.get("PAY_TEST_PRIVATE_KEY", "")
    if not key:
        sys.exit("Set PAY_TEST_PRIVATE_KEY in this PowerShell window first (not in a file).")
    acct = Account.from_key(key)
    print("paying wallet:", acct.address)
    feeds = feed_list(); total = sum(p for _, p in feeds)
    if not a.send:
        for path, price in feeds:
            print("DRY %-22s %.3f USDC" % (path, price / 1e6))
        print("total one-round: %.3f USDC (no money moved; add --send to pay)" % (total / 1e6))
        return
    client = x402ClientSync()
    client.register("eip155:*", ExactEvmScheme(EthAccountSigner(acct)))
    session = x402_requests(client)
    ok = 0
    for path, price in feeds:
        r = session.get(BASE + path, timeout=90)
        status = "PAID" if r.status_code == 200 else "FAIL"
        ok += r.status_code == 200
        print(status, path, r.status_code, "" if r.status_code == 200 else r.text[:200].replace("\n", " "))
        time.sleep(2)
    print("paid OK: %d of %d feeds" % (ok, len(feeds)))

if __name__ == "__main__":
    main()
