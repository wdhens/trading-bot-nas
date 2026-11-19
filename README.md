# trading-bot-nas

Update channel for a personal trading bot that runs on a Synology NAS.

Each release holds `trading-bot-nas.tgz` (bot code plus its own Python for 64-bit ARM)
and `latest.json` (version, sha256, download URL). Once a day the NAS reads
`releases/latest/download/latest.json`. It downloads a newer package, checks its
sha256 and runs that package's own tests in a separate folder, and it installs the
package only if every test passes.

The packages contain no keys or other secrets. Those stay in the NAS Task Scheduler.
