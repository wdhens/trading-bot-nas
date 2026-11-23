#!/usr/bin/env python3
"""Watchdog + automatic updater for the x402 pay server on the Synology NAS.

Separate from the trading bot on purpose: own folder, own Task Scheduler task, own update
file. Nothing here touches the trading bot, and the bot's updater never touches this.

Run by the DSM task "pay-server" every 5 minutes (10 also works) (as admin, the user the server runs as):
  * if the server does not answer on /health, (re)start it,
  * once a day (11:xx UTC) look for a new version in
    https://raw.githubusercontent.com/wdhens/trading-bot-nas/main/pay-server/latest.json.
    A new version is checked (sha256), then started on a spare port and tested
    (/health, /v1/feeds, an unpaid feed must answer 402 with a v2 PAYMENT-REQUIRED header).
    Only then is the live server switched over; if the new one does not come up, the
    previous file is put back and restarted.
  * once a day, if PUBLIC_BASE_URL is a public https URL, run Coinbase's listing check
    (`nas_pay_server.py --check`) and note the result.
A short status goes to pay-server-STATUS.txt in the same folder. Settings and keys come
only from the DSM task's environment and are never written anywhere.
Python 3.8+ stdlib only (DSM's own python3).
"""
import base64
import datetime as dt
import hashlib
import json
import os
import py_compile
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

D = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(D, "nas_pay_server.py")
STATE = os.path.join(D, "pay_state.json")
LOG = os.path.join(D, "server.log")
TICKLOG = os.path.join(D, "pay_tick.log")
STATUS = os.path.join(D, "pay-server-STATUS.txt")
PIDFILE = os.path.join(D, "pay_server.pid")
PORT = int(os.environ.get("PORT", "8402"))
TEST_PORT = int(os.environ.get("PAY_TEST_PORT", "18403"))
URL = os.environ.get("PAY_UPDATE_URL",
                     "https://raw.githubusercontent.com/wdhens/trading-bot-nas/main/pay-server/latest.json")
UPDATE_HOUR = int(os.environ.get("PAY_UPDATE_HOUR", "11"))
PY = sys.executable or "python3"
TICK_MIN = int(os.environ.get("PAY_TICK_MIN", "5"))  # how often DSM runs this task (for the uptime %)
HISTORY = os.path.join(D, "pay-stats-history.jsonl")


def log(msg):
    line = "{} {}".format(dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M"), msg)
    print(line)
    try:
        with open(TICKLOG, "a") as f:
            f.write(line + "\n")
        with open(TICKLOG) as f:
            lines = f.readlines()
        if len(lines) > 3000:
            with open(TICKLOG, "w") as f:
                f.writelines(lines[-2000:])
    except OSError:
        pass


def load_state():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_state(st):
    with open(STATE + ".tmp", "w") as f:
        json.dump(st, f, indent=1)
    os.replace(STATE + ".tmp", STATE)


def status(lines):
    try:
        with open(STATUS, "w") as f:
            f.write("Pay server op de NAS, {} UTC\n".format(dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M")))
            f.write("\n".join(lines) + "\n")
    except OSError:
        pass


def get(url, timeout=6):
    """(status, headers, body) of a local GET; (0, {}, b'') if it does not answer."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, {k.upper(): v for k, v in r.headers.items()}, r.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.upper(): v for k, v in e.headers.items()}, e.read()
    except Exception:
        return 0, {}, b""


def health(port):
    code, _, body = get("http://127.0.0.1:{}/health".format(port))
    if code != 200:
        return None
    try:
        return json.loads(body.decode())
    except ValueError:
        return None


def start(path, port, logfile):
    env = dict(os.environ, PORT=str(port))
    out = open(logfile, "a")
    p = subprocess.Popen([PY, path], cwd=D, env=env, stdout=out, stderr=subprocess.STDOUT,
                         stdin=subprocess.DEVNULL, start_new_session=True)
    for _ in range(20):
        time.sleep(1)
        h = health(port)
        if h:
            return p, h
        if p.poll() is not None:
            break
    return p, None


def stop_live():
    """Stop the running live server: ours via its pid file, an older one (the former boot task)
    via its command line."""
    pids = set()
    try:
        with open(PIDFILE) as f:
            pids.add(int(f.read().strip()))
    except (OSError, ValueError):
        pass
    try:
        out = subprocess.run(["pgrep", "-f", "nas_pay_server.py"], stdout=subprocess.PIPE,
                             universal_newlines=True).stdout
        for x in out.split():
            try:  # only real python processes running the server file, never shells or editors
                with open("/proc/{}/cmdline".format(x), "rb") as f:
                    argv = [a.decode("utf-8", "replace") for a in f.read().split(b"\0") if a]
            except OSError:
                continue
            if argv and "python" in os.path.basename(argv[0]) and \
                    any(a.endswith("nas_pay_server.py") and "pay-update" not in a for a in argv[1:3]):
                pids.add(int(x))
    except (OSError, ValueError):
        pass
    pids.discard(os.getpid())
    pids.discard(os.getppid())
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
    for _ in range(10):
        if not health(PORT):
            return True
        time.sleep(1)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    time.sleep(2)
    return not health(PORT)


def start_live():
    p, h = start(SERVER, PORT, LOG)
    if h:
        with open(PIDFILE, "w") as f:
            f.write(str(p.pid))
    return h


def smoke_test(path):
    """Start a candidate on the spare port and check what agents and indexers see."""
    try:
        py_compile.compile(path, doraise=True)
    except py_compile.PyCompileError as e:
        return False, "does not compile: {}".format(str(e)[:200])
    p, h = start(path, TEST_PORT, os.path.join(D, "pay_test.log"))
    try:
        if not h:
            return False, "does not start (see pay_test.log)"
        code, _, body = get("http://127.0.0.1:{}/v1/feeds".format(TEST_PORT))
        if code != 200:
            return False, "/v1/feeds answered {}".format(code)
        feeds = json.loads(body.decode()).get("endpoints") or []
        if not feeds:
            return False, "no feeds listed"
        code, hdr, _ = get("http://127.0.0.1:{}{}".format(TEST_PORT, feeds[0]["path"]))
        if code != 402 or "PAYMENT-REQUIRED" not in hdr:
            return False, "unpaid feed answered {} without a payment request".format(code)
        pr = json.loads(base64.b64decode(hdr["PAYMENT-REQUIRED"]).decode())
        if pr.get("x402Version") != 2 or not pr.get("accepts"):
            return False, "payment request is not x402 v2"
        return True, "version {}, {} feeds, 402 OK".format(h.get("version"), len(feeds))
    except Exception as e:
        return False, "test error: {}".format(str(e)[:200])
    finally:
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except OSError:
            pass


def fetch(url, timeout=60):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def update(st):
    m = json.loads(fetch(URL).decode())
    if m.get("version") == st.get("version"):
        return "update: {} is the newest version".format(m.get("version"))
    new = os.path.join(D, "pay-update")
    shutil.rmtree(new, ignore_errors=True)
    os.makedirs(new)
    files = m.get("files") or {}
    for name, f in files.items():
        if name not in ("nas_pay_server.py", "pay_tick.py"):
            return "update {}: unexpected file {}, NOT installed".format(m.get("version"), name)
        data = fetch(f["url"])
        if hashlib.sha256(data).hexdigest() != f["sha256"]:
            shutil.rmtree(new, ignore_errors=True)
            return "update {}: checksum wrong, NOT installed".format(m.get("version"))
        with open(os.path.join(new, name), "wb") as out:
            out.write(data)
    if "nas_pay_server.py" in files:
        ok, why = smoke_test(os.path.join(new, "nas_pay_server.py"))
        if not ok:
            shutil.rmtree(new, ignore_errors=True)
            return "update {}: test failed, NOT installed ({})".format(m.get("version"), why)
        if os.path.exists(SERVER):
            shutil.copy2(SERVER, SERVER + ".previous")
        shutil.move(os.path.join(new, "nas_pay_server.py"), SERVER)
        stop_live()
        if not start_live():
            # roll back: the previous version keeps earning
            if os.path.exists(SERVER + ".previous"):
                shutil.copy2(SERVER + ".previous", SERVER)
            stop_live()
            start_live()
            shutil.rmtree(new, ignore_errors=True)
            return "update {}: did not start live, previous version restored".format(m.get("version"))
    if "pay_tick.py" in files:
        py_compile.compile(os.path.join(new, "pay_tick.py"), doraise=True)
        shutil.move(os.path.join(new, "pay_tick.py"), os.path.join(D, "pay_tick.py"))
    shutil.rmtree(new, ignore_errors=True)
    st["version"] = m.get("version")
    return "update: installed {} (tested on port {} first)".format(m.get("version"), TEST_PORT)


def listing_check():
    base = os.environ.get("PUBLIC_BASE_URL", "")
    if not base.startswith("https://"):
        return "listing check: PUBLIC_BASE_URL is not set to your https address yet"
    p = subprocess.run([PY, SERVER, "--check"], cwd=D, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       universal_newlines=True, timeout=600)
    lines = p.stdout.strip().splitlines()
    ok = sum(1 for l in lines if l.startswith("OK"))
    bad = [l[:160] for l in lines if l.startswith("FAIL")]
    return "listing check: {} OK, {} FAIL{}".format(ok, len(bad), ("; " + " | ".join(bad[:3])) if bad else "")


def uptime_note(st, ok, now):
    """Share of ticks in the last 30 days where the server answered (missed ticks = NAS off)."""
    days = st.setdefault("uptime", {})
    today = now.date().isoformat()
    days[today] = days.get(today, 0) + (1 if ok else 0)
    for k in sorted(days)[:-30]:
        del days[k]
    first = st.setdefault("uptime_since", now.isoformat())
    start = max(dt.datetime.fromisoformat(first), now - dt.timedelta(days=30))
    expected = max(1, int((now - start).total_seconds() // (TICK_MIN * 60)) + 1)
    return min(100.0, round(100.0 * sum(days.values()) / expected, 1))


def stats_note(st, today):
    code, _, body = get("http://127.0.0.1:{}/v1/stats".format(PORT))
    if code != 200:
        return ""
    s = json.loads(body)
    if st.get("last_history") != today:
        st["last_history"] = today
        with open(HISTORY, "a") as f:
            f.write(json.dumps({"date": today, "stats": s}, separators=(",", ":")) + "\n")
        c2, _, b2 = get("http://127.0.0.1:{}/v1/source-health".format(PORT), timeout=30)
        if c2 == 200:
            h = json.loads(b2)
            bad = [k for k, v in h.get("sources", {}).items() if not v.get("ok")]
            st["last_sources"] = "databronnen: {}/{} bereikbaar{}".format(
                h.get("ok"), h.get("total"), ("; stuk: " + ", ".join(bad)) if bad else "")
    if "external" in s:  # server 3.3+
        e = s["external"]
        out = ("verkopen van anderen: {} (${:.2f}) van {} betalers, {} terugkerend; eigen testbetalingen: {}; "
               "zonder betaleradres: {}").format(
            e.get("sales", 0), e.get("usdc", 0.0), e.get("paying_wallets", 0), e.get("repeat_wallets", 0),
            s.get("own_wallet_sales", {}).get("sales", 0), s.get("unknown_payer_sales", {}).get("sales", 0))
        if e.get("milestone_reached"):
            out += ("\nMIJLPAAL: nog ${:.2f} van anderen niet doorgestort. Controleer eerst of de betalers (zie /v1/stats) "
                    "niet jouw eigen wallets zijn. Vraag Claude om door te storten naar de bot (jij keurt goed).").format(
                e.get("unforwarded_usd", 0.0))
        return out
    return "verkopen: {} (${:.2f}) van {} betalers".format(
        s.get("sales_total", 0), s.get("usdc_total", 0.0), s.get("unique_payers", 0))


def main():
    st = load_state()
    now = dt.datetime.now(dt.timezone.utc)
    lines = []
    h = health(PORT)
    if not h:
        log("server not answering on port {}: starting it".format(PORT))
        h = start_live()
        msg = "server started (version {})".format(h.get("version")) if h else "server does NOT start: see server.log"
        log(msg)
        lines.append(msg)
    today = now.date().isoformat()
    if (now.hour == UPDATE_HOUR or not st.get("version")) and st.get("last_update") != today:
        st["last_update"] = today
        try:
            msg = update(st)
        except Exception as e:
            msg = "update check failed: {}".format(str(e)[:200])
        log(msg)
        lines.append(msg)
        st["last_update_msg"] = msg
        try:
            msg = listing_check()
        except Exception as e:
            msg = "listing check failed: {}".format(str(e)[:200])
        log(msg)
        lines.append(msg)
        st["last_listing"] = msg
    h = health(PORT)
    up = uptime_note(st, bool(h), now)
    try:
        sales = stats_note(st, today)
    except Exception as e:
        sales = "verkopen: onbekend ({})".format(str(e)[:80])
    save_state(st)
    if lines or not h or now.minute < TICK_MIN:
        last = st.get("last_update_msg", "")
        out = [x for x in lines if x != last] + [last,
               "nu: " + ("online, versie {}".format(h.get("version")) if h else "OFFLINE"),
               "bereikbaar (30 dagen): {}%".format(up), sales, st.get("last_sources", ""), st.get("last_listing", "")]
        status([x for x in out if x])


if __name__ == "__main__":
    main()
