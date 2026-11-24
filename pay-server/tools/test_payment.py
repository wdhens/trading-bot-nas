#!/usr/bin/env python3
"""One-time x402 test payments from YOUR OWN wallet to the pay server (run on your PC, not on the NAS).

Why: the first settled payment lists each feed in the Coinbase Bazaar. Set PAY_SELF_WALLETS on the NAS to
this wallet first, so these payments count as own-wallet sales, not outside demand.

Setup (once):  pip install eth-account requests
Run dry (default, no money moves):   python3 test_payment.py
Really pay (one call per feed):      python3 test_payment.py --send
Key: only via environment variable PAY_TEST_PRIVATE_KEY. Never paste it into chat or a file in the repo.
"""
import argparse, base64, json, os, secrets, sys, time
import requests
from eth_account import Account
from eth_account.messages import encode_typed_data

BASE = "https://strawberryking.synology.me"
PAY_TO = "0xC9eebecb1d0AfF4fb2B9978516E075A33639892C"
USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
CHAIN_ID = 8453

def authorization_for(acct, value, pay_to):
    now = int(time.time())
    auth = {"from": acct.address, "to": pay_to, "value": int(value),
            "validAfter": now - 60, "validBefore": now + 300, "nonce": "0x" + secrets.token_hex(32)}
    full = {
        "types": {
            "EIP712Domain": [{"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                             {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"}],
            "TransferWithAuthorization": [{"name": "from", "type": "address"}, {"name": "to", "type": "address"},
                                          {"name": "value", "type": "uint256"}, {"name": "validAfter", "type": "uint256"},
                                          {"name": "validBefore", "type": "uint256"}, {"name": "nonce", "type": "bytes32"}]},
        "primaryType": "TransferWithAuthorization",
        "domain": {"name": "USD Coin", "version": "2", "chainId": CHAIN_ID, "verifyingContract": USDC},
        "message": auth,
    }
    sig = acct.sign_message(encode_typed_data(full_message=full)).signature.hex()
    if not sig.startswith("0x"):
        sig = "0x" + sig
    return auth, sig

def feed_list():
    d = requests.get(BASE + "/v1/feeds", timeout=20).json()
    return [(e["path"], int(e["price_usdc_atomic"])) for e in d["endpoints"]]

def pay_one(acct, path, requirement_json, accept):
    auth, sig = authorization_for(acct, accept["amount"], accept["payTo"])
    payload = {"x402Version": 2, "accepted": accept,
               "payload": {"signature": sig, "authorization": auth}, "resource": requirement_json["resource"]}
    hdr = base64.b64encode(json.dumps(payload).encode()).decode()
    r = requests.get(BASE + path, headers={"PAYMENT-SIGNATURE": hdr}, timeout=60)
    return r.status_code, r.headers.get("payment-response", ""), r.text[:900]

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--send", action="store_true"); a = ap.parse_args()
    key = os.environ.get("PAY_TEST_PRIVATE_KEY", "")
    if not key:
        sys.exit("Set PAY_TEST_PRIVATE_KEY in your environment first (not in a file).")
    acct = Account.from_key(key)
    print("paying wallet:", acct.address)
    total = 0
    for path, price in feed_list():
        total += price
        if not a.send:
            print("DRY %-22s %.3f USDC" % (path, price / 1e6)); continue
        r = requests.get(BASE + path, timeout=20)
        if r.status_code != 402:
            print("SKIP", path, r.status_code); continue
        req = json.loads(base64.b64decode(r.headers["payment-required"]))
        accept = req["accepts"][0]
        status, resp, body = pay_one(acct, path, req, accept)
        print("PAID" if status == 200 else "FAIL", path, status, body if status != 200 else "")
        time.sleep(3)
    print("total one-round: %.3f USDC" % (total / 1e6))

if __name__ == "__main__":
    main()
