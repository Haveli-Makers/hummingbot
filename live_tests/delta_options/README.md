# delta_options live test scripts (temporary backup)

These are backup copies of the live test scripts for the `delta_options` connector. The working copies live in
`~/hm/temp/connectors/delta_options/`: run them from there, not from here (run from here, they would write account
data into `probe_output/` inside the repo).

**Remove this folder before raising the PR** (`git rm -r live_tests`); keep the copies in `temp/`.

| Script | What it does |
|---|---|
| `verify_delta_options.py` | Live checks: public data, `--with-keys`, `--trade` (orders that rest and get cancelled), `--fill` (tiny real fills) |
| `probe_settlement.py` | Read-only: what Delta keeps about expired options, and the account's settlement records |
| `hold_through_expiry.py` | `buy` then `check`: hold two calls through an expiry; `buy --stay`: the bot itself through an expiry |
| `rehearse_offline.py`, `rehearse_hold.py` | The same runs offline, against a simulated Delta |
| `rehearse_strangle.py` | `scripts/options_strangle_example.py` offline, under a real clock, through a simulated expiry |
