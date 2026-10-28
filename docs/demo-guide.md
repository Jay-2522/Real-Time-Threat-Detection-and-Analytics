# Local demonstration guide

The default event stream is synthetic. The Cowrie adapter is implemented and tested
with `data/samples/cowrie-synthetic.json`; it has not been deployed against a live
honeypot.

## Start and provision a local account

```bash
./scripts/start_full.sh --build
docker compose exec backend python -m app.auth.provision
```

The provisioning command prompts privately for a username and password. There are no
committed application credentials. Open `http://localhost:5173` and sign in with the
local account you just created. The UI is labeled **Streaming Security Analytics
(local demo)**. By default it shows Overview, Live Stream, ML Predictions, Alerts,
Incidents, and Detection Engineering; the other implemented pages are default-off
feature flags.

Grafana is optional:

```bash
./scripts/start_full.sh --dashboards
```

Its username is `admin`; its generated local password and published port are in the
ignored, mode-0600 `.env` created by `scripts/setup_local.py`.

## Replay the synthetic Cowrie sample

```bash
docker compose run --rm -T --no-deps \
  -v "$PWD/data/samples:/samples:ro" \
  telemetry-producer python -m app.main cowrie-replay \
  --file /samples/cowrie-synthetic.json --time-shift
./scripts/assert_honeypot_detection.sh
```

Every sample record is synthetic. The 20 failed logins from documentation address
`203.0.113.42` produce one `brute_force_detection` alert in its own incident.
The sample also deliberately constructs an RFC1918 grouping fixture: one remote-
service connection from `10.20.0.15` to `10.20.0.20` on port 22 and five failed
logins from `10.20.0.15`. Those records produce `lateral_movement` and
`brute_force_detection` alerts in a separate two-alert incident. Any rule hit
other than the one produced by the original 20-failed-login burst comes from
those deliberately constructed fixture records.

The assertion expects three fixture alerts and two incidents and checks their
authenticated dashboard API visibility. Open Alerts to inspect the three rows,
then open Incidents to see one event for `203.0.113.42` and two events for
`10.20.0.15`. This validates rule execution and entity-based correlation logic;
it does not demonstrate real attacker behavior or evidence from a live honeypot.

## Stop

```bash
./scripts/stop_all.sh
```
