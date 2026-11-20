# trading-bot-nas

Update channel for a personal trading bot that runs on a Synology NAS.

Each release holds `trading-bot-nas.tgz` (bot code plus its own Python for 64-bit ARM)
and `latest.json` (version, sha256, download URL). Once a day the NAS reads
`releases/latest/download/latest.json`. It downloads a newer package, checks its
sha256 and runs that package's own tests in a separate folder, and it installs the
package only if every test passes.

The packages contain no keys or other secrets. Those stay in the NAS Task Scheduler.

## pay-server/ (separate from the bot)

The x402 pay server and its watchdog/updater (`pay_tick.py`), run by its own Task Scheduler
task on the NAS. `pay-server/latest.json` lists the current files with their sha256; the NAS
checks it daily at 11:xx UTC, tests a new version on a spare port, and only then switches over.
No keys or settings are stored here: they stay in the NAS task.
