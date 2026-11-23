# NAS pay server (x402)

A small pay-per-request data API for AI agents. An agent calls an endpoint, gets an HTTP 402 with the price,
pays in USDC on Base through x402, and receives the data. Payments go straight to the owner's wallet; the
server never holds a private key. It is stdlib-only Python and runs on a Synology DS218play (1 GB RAM).

* Live address: `https://strawberryking.synology.me`
* Catalogue with prices and examples: `/v1/feeds` (also `/openapi.json`, `/llms.txt`, `/.well-known/x402`)
* Free: `/health`, `/v1/stats` (sales totals), `/v1/source-health` (which upstream data sources are reachable)

## Feeds (prices are in `/v1/feeds`; USDC, Base mainnet)
| Tier | Price | Examples |
|---|---|---|
| basic | $0.005 | market signal, Binance funding, fear and greed, DeFi TVL, gas, BTC network, trending, Lighter markets |
| premium | $0.01 | Lighter funding vs Binance/Bybit/Hyperliquid, cross-venue funding arbitrage, positioning, stablecoin flows, DEX volume, fees, yields, **Lighter and Hyperliquid order-book depth and slippage**, **crowding score** |
| bundle | $0.02 | regime score |

Every feed returns source and cache age. Slippage is measured against the best price; add half the spread for the mid price. Nothing here is investment advice, and none of the scores is a
backtested trading signal. Indicative prices and slippage are not guaranteed execution prices.

## Calling a feed
Use any x402 v2 client. Coinbase's buyer quickstart (https://docs.cdp.coinbase.com/x402/quickstart-for-buyers)
shows this TypeScript pattern (copied from that page; not yet tested against this server):

```ts
import { CdpX402Client } from "@coinbase/cdp-sdk/x402";
import { wrapFetchWithPayment } from "@x402/fetch";

const client = new CdpX402Client();                 // mainnet; CDP credentials come from environment variables
const fetchWithPayment = wrapFetchWithPayment(globalThis.fetch, client);
const res = await fetchWithPayment("https://strawberryking.synology.me/v1/lighter-depth");
console.log(await res.json());
```
Python: see the same quickstart (`pip install "x402[evm,httpx]"`).

## Running and updating
* `nas_pay_server.py`: the server. `pay_tick.py`: watchdog and updater run by a Synology task every 5 minutes.
  It checks `latest.json` daily, tests a new version on a spare port, switches over, and rolls back on failure.
* Status for the owner: `pay-server-STATUS.txt` next to the files on the NAS.
* Tests: `python3 -m unittest discover -s pay-server/tests` (offline, no network).
* Environment (set in the Synology task, never in the repo): `PUBLIC_BASE_URL`, `CDP_API_KEY_ID`,
  `CDP_API_KEY_SECRET`. Optional: `PAY_SELF_WALLETS` (the owner's own paying wallets, comma separated: their test
  payments are not counted as outside sales), `PAY_FORWARDED_USD` (outside revenue already moved to the bot),
  `LIGHTER_MAX_PER_MIN` (default 20) and `LIGHTER_QUIET_UTC` (default `00:05-00:30,20:05-21:30`): the trading bot
  shares the home IP, so Lighter reads stay under a budget and pause while the bot runs.

This server is separate from the trading bot in the same repository: own folder, own task, own updater.
