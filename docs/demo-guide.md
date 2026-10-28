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

The assertion waits for `brute_force_detection` and `lateral_movement` alerts for
`203.0.113.42`, verifies that one incident links at least two alerts, and checks
authenticated dashboard API visibility. Then open Alerts, expand either matching
row to inspect its linked incident, and open Incidents. This demonstrates the
synthetic fixture and pipeline; it is not evidence from a live honeypot.

## Stop

```bash
./scripts/stop_all.sh
```
