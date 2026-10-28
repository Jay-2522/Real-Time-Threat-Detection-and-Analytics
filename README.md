# Streaming Security Analytics (local demo)

A single-node local demo that moves synthetic telemetry through Kafka, Spark, detection, durable storage, and an authenticated dashboard.

[![Fast Checks](https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics/actions/workflows/fast-checks.yml/badge.svg?branch=dev)](https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics/actions/workflows/fast-checks.yml?query=branch%3Adev)
[![Stack Smoke](https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics/actions/workflows/stack-smoke.yml/badge.svg?branch=dev)](https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics/actions/workflows/stack-smoke.yml?query=branch%3Adev)
[![Security Scans](https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics/actions/workflows/security-scans.yml/badge.svg?branch=dev)](https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics/actions/workflows/security-scans.yml?query=branch%3Adev)

## Quickstart

Prerequisites are Git, a running Docker engine, and either Docker Compose v2
(`docker compose`) or the compatible `docker-compose` command. If Docker runs in
a VM such as Colima, clone into a host directory shared with that VM. A first
image build takes longer than one minute; the measured fresh-clone time is
reported in the delivery notes for this documentation change.

~~~bash
export BRANCH=dev
git clone --branch "$BRANCH" \
  https://github.com/Jay-2522/Real-Time-Threat-Detection-and-Analytics.git \
  streaming-threat-analytics
cd streaming-threat-analytics
./scripts/start_full.sh --build

if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
else
  COMPOSE="docker-compose"
fi
$COMPOSE exec backend python -m app.auth.provision
~~~

For a disposable local demonstration, enter:

~~~text
Username: local-admin
Password: Local-only-demo-2026!
~~~

Those are documentation-only local credentials, not an automatic account or a secret. Never reuse them elsewhere. Open http://localhost:5173 and sign in.

The dashboard is labeled **Streaming Security Analytics (local demo)**. Its default
navigation is Overview, Live Stream, ML Predictions, Alerts, Incidents, and
Detection Engineering. Other implemented pages remain in the codebase behind
default-off feature flags.

Replay the repository's synthetic Cowrie sample:

~~~bash
$COMPOSE run --rm -T --no-deps \
  -v "$PWD/data/samples:/samples:ro" \
  telemetry-producer python -m app.main cowrie-replay \
  --file /samples/cowrie-synthetic.json --time-shift
./scripts/assert_honeypot_detection.sh
~~~

The assertion waits for the brute-force alert, its correlated incident, and authenticated dashboard API visibility. The Cowrie adapter is implemented and tested with this synthetic sample but has not been deployed against a live honeypot.

## What this is—and is not

This is a local, single-organization engineering lab for studying streaming delivery, event-time windows, detection evaluation, recovery, and an analyst-facing UI.

It is not:

- a traffic blocker, endpoint responder, or automated containment system;
- a replacement for a SIEM;
- a multi-tenant or multi-organization service;
- a multi-node or high-availability deployment;
- evidence of detection quality on a live network.

Events are synthetic by default. CIC-IDS-2017 is used only for offline evaluation and never enters the running stack. The separate hardened deployment is experimental and is not covered by CI.

## Architecture

~~~mermaid
flowchart LR
    P["Synthetic producer or Cowrie replay"] --> K["Kafka + ZooKeeper"]
    K --> S["Spark Structured Streaming local[2]"]
    S --> K
    K --> M["ML inference"]
    M --> K
    K --> A["Alert engine"]
    K --> T["Threat-intelligence engine"]
    K --> SS["Storage sink"]
    A --> PG["PostgreSQL source of truth"]
    T --> PG
    SS --> PG
    A --> ES["Elasticsearch search projection"]
    T --> ES
    SS --> ES
    PG --> API["FastAPI REST and WebSockets"]
    ES --> API
    API --> UI["React dashboard"]
    API --> PR["Prometheus"]
    M --> PR
    A --> PR
    T --> PR
    SS --> PR
    PR -. optional dashboards profile .-> G["Grafana"]
~~~

One event is published to Kafka, normalized by Spark, and written to an enriched topic. Spark also groups records into a 60-second event-time window with a 30-second slide and a 2-minute watermark. ML inference consumes the resulting 18-field feature record. The alert engine can evaluate enriched records directly or evaluate predictions and windowed features. The storage, alert, and threat services commit authoritative effects to PostgreSQL before committing Kafka offsets; Elasticsearch is filled asynchronously from a durable outbox.

The windowed path must wait for event time to advance far enough to close a window. The 60-second window and 2-minute watermark therefore make minutes of deliberate delay possible before processing and queueing time are added.

See [the current architecture](docs/architecture.md) and [delivery contract](docs/KAFKA_DELIVERY_CONTRACT.md).

## Results

### Offline detection evaluation

The evaluator uses the fixed day split in [eval/results/latest.md](eval/results/latest.md): Monday benign windows train the Isolation Forest, Tuesday tunes thresholds, and Wednesday through Friday are final test days.

| Detector | Test day | TN | FP | FN | TP | Precision | Recall | F1 | FPR | Alerts/benign hour |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Threshold rules | Wednesday | 38343 | 870 | 26 | 60 | 0.064516 | 0.697674 | 0.118110 | 0.022187 | 98.014184 |
| Isolation Forest | Wednesday | 38486 | 727 | 15 | 71 | 0.088972 | 0.825581 | 0.160633 | 0.018540 | 79.432624 |
| Random Forest (supervised reference upper bound) | Wednesday | 39213 | 0 | 85 | 1 | 1.000000 | 0.011628 | 0.022989 | 0.000000 | 0.000000 |
| All benign baseline | Wednesday | 39213 | 0 | 86 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Thursday | 35889 | 805 | 78 | 18 | 0.021871 | 0.187500 | 0.039173 | 0.021938 | 94.307692 |
| Isolation Forest | Thursday | 36040 | 654 | 77 | 19 | 0.028232 | 0.197917 | 0.049415 | 0.017823 | 77.076923 |
| Random Forest (supervised reference upper bound) | Thursday | 36694 | 0 | 96 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Thursday | 36694 | 0 | 96 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Friday | 33895 | 721 | 610 | 87 | 0.107673 | 0.124821 | 0.115615 | 0.020829 | 89.890110 |
| Isolation Forest | Friday | 34041 | 575 | 608 | 89 | 0.134036 | 0.127690 | 0.130786 | 0.016611 | 71.648352 |
| Random Forest (supervised reference upper bound) | Friday | 34616 | 0 | 696 | 1 | 1.000000 | 0.001435 | 0.002865 | 0.000000 | 0.000000 |
| All benign baseline | Friday | 34616 | 0 | 697 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |

High benign accuracy is not useful detection evidence:

| Test day | All-benign accuracy | Missed attack windows |
| --- | --- | --- |
| Wednesday | 0.997812 | 86 |
| Thursday | 0.997391 | 96 |
| Friday | 0.980262 | 697 |

Per-attack recall:

| Test day and attack | Threshold rules | Isolation Forest | Supervised reference | All benign |
| --- | ---: | ---: | ---: | ---: |
| Wednesday: DoS GoldenEye | 1.000000 | 1.000000 | 0.000000 | 0.000000 |
| Wednesday: DoS Hulk | 0.952381 | 0.904762 | 0.000000 | 0.000000 |
| Wednesday: DoS Slowhttptest | 0.473684 | 0.578947 | 0.000000 | 0.000000 |
| Wednesday: DoS slowloris | 0.851852 | 0.851852 | 0.037037 | 0.000000 |
| Wednesday: Heartbleed | 0.000000 | 0.909091 | 0.000000 | 0.000000 |
| Thursday: Infiltration | 0.642857 | 0.678571 | 0.000000 | 0.000000 |
| Thursday: Web Attack – Brute Force | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Thursday: Web Attack – Sql Injection | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Thursday: Web Attack – XSS | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Friday: Bot | 0.083333 | 0.086420 | 0.000000 | 0.000000 |
| Friday: DDoS | 1.000000 | 1.000000 | 0.000000 | 0.000000 |
| Friday: PortScan | 0.407407 | 0.407407 | 0.037037 | 0.000000 |

The honest reading is that precision is low, the web attacks are entirely missed, and the supervised reference does not provide a useful upper bound under this split. Only 4 of the 18 production features have a real equivalent in these flow windows, so the production Isolation Forest was not scored by filling the other fields with invented values.

### Single-node benchmarks

Current results are in [benchmarks/results/latest.md](benchmarks/results/latest.md). The pre-batching baseline is preserved in Git at commit d16a262.

Hardware for both result sets:

| Item | Value |
| --- | --- |
| machine | MacBook Air |
| chip | Apple M5 |
| ram | 16 GB |
| os | macOS 26.7.1 (25G241) |
| docker | client=29.9.1 server=29.5.2 |
| docker vm | Colima 4 CPU, 8 GiB RAM, aarch64 |

Before and after storage batching:

| Version | Requested events/s | Runs | Achieved median (range) | Persisted % median (range) | Max lag median (range) | First observed bottleneck |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Before | 200 | 3 | 200.00 (200.00–200.00) | 65.58 (58.91–66.36) | 88936 (85100–91844) | streaming-analytics-storage-sink |
| Before | 500 | 3 | 499.99 (499.96–500.00) | 24.52 (22.51–24.90) | 268406 (258772–278162) | streaming-analytics-storage-sink |
| Before | 1000 | 3 | 999.94 (999.85–999.98) | 16.76 (15.87–16.79) | 568727 (562251–571398) | streaming-analytics-storage-sink |
| After | 200 | 2 | 199.92 (199.92–199.93) | 100.00 (100.00–100.00) | 87244 (87002–87486) | streaming-analytics-threat-intelligence |
| After | 500 | 2 | 499.98 (499.97–500.00) | 100.00 (100.00–100.00) | 235000 (233667–236333) | streaming-analytics-threat-intelligence |
| After | 1000 | 2 | 999.99 (999.98–999.99) | 100.00 (100.00–100.00) | 463104 (453498–472709) | streaming-analytics-threat-intelligence |

The storage sink originally used one transaction and synchronous offset commit per event. It now batches receipt claims, business upserts, and outbox work, then advances offsets only after the transaction commits. Persistence reached 100% after drain at all three measured rates. Threat intelligence, which performs serialized entity and timeline updates, became the first observed lagging consumer. Latency above about 200 events/s is inflated by backlog; these figures are single-node laptop measurements, not capacity for another machine.

Historical engine comparison that motivated the default Spark local mode:

| Engine | Requested events/s | Achieved events/s | Median CPU % | Median memory MiB | Median window latency s | Drained |
| --- | --- | --- | --- | --- | --- | --- |
| Spark cluster | 200 | 200.00 (200.00–200.00) | 12.77 | 2110.89 | 135.85 | no |
| Spark local[2] | 200 | 200.00 | 0.96 | 943.00 | 151.09 | yes |
| Plain Python | 200 | 200.37 | 8.20 | 30.75 | 120.26 | yes |
| Spark cluster | 500 | 499.99 (499.96–500.00) | 23.07 | 2088.08 | 135.83 | no |
| Spark local[2] | 500 | 500.00 | 1.16 | 968.80 | 150.81 | yes |
| Plain Python | 500 | 501.29 | 15.90 | 37.67 | 120.11 | yes |
| Spark cluster | 1000 | 999.94 (999.85–999.98) | 46.85 | 2405.02 | 140.51 | no |
| Spark local[2] | 1000 | 1000.00 | 0.69 | 1083.39 | 148.08 | yes |
| Plain Python | 1000 | 1002.76 | 15.60 | 54.97 | 120.06 | yes |

The shared sample compared 600 common windows and 10800 feature values with 0 mismatches, 0 Spark-only windows, and 0 Python-only windows. The repository keeps Spark rather than replacing it; local mode removes unnecessary cluster coordination for this single-node stack.

Watermark trade-off at 200 events/s with 10% configured disorder:

| Watermark | Median end-to-end s | Observed out-of-order events | Missing closed-window memberships | Missing % | Drained |
| --- | --- | --- | --- | --- | --- |
| 120 s | 219.87 | 5988 | 15 | 0.03 | no (timeout) |
| 60 s | 164.06 | 5988 | 943 | 1.16 | no (timeout) |
| 30 s | 128.52 | 5988 | 2977 | 2.88 | no (timeout) |

Shorter watermarks reduced measured delay but dropped more late closed-window memberships. The default remains 2 minutes.

### Continuous integration

Fast Checks runs data-safety and shell checks, Python linting and tests, storage tests, a PostgreSQL migration integration, frontend lint/test/build, and a Chromium authentication workflow. Stack Smoke builds from an empty cache, starts clean volumes, validates the 18-feature runtime artifact, runs lineage, replays the synthetic Cowrie fixture, and checks the alert, incident, and dashboard API. Security Scans runs pip-audit, npm audit, and Trivy filesystem/image scans.

There are 30 opt-in live pipeline, recovery, and hardened-deployment test cases that are not run by these workflows because they require separately marked infrastructure, generated credentials, or destructive failure injection. Pytest collection currently finds 20 opt-in hardened cases, 9 live pipeline cases, and 1 live Spark-recovery case; the remaining recovery unit tests run in Fast Checks.

The Security Scans run on commit 6e6a64c reported findings rather than fixing or waiving them:

| Scanner/target | Critical | High | Medium | Low | Unknown |
| --- | ---: | ---: | ---: | ---: | ---: |
| pip-audit | 0 | 0 | 0 | 0 | 0 |
| npm audit | 0 | 1 | 3 | 0 | 0 |
| Trivy filesystem | 0 | 10 | 3 | 14 | 0 |
| Trivy container images | 34 | 1195 | 3398 | 2044 | 65 |

## Design decisions and trade-offs

- Kafka provides replayable stage boundaries. The local broker has one replica and ZooKeeper, so it does not provide broker high availability.
- Spark stays because the event-time implementation and feature values were tested. Spark local[2] drained through 1000 events/s in the comparison while the master/worker mode did not drain from 200 events/s.
- PostgreSQL is the source of truth because receipts, business effects, cooldowns, incidents, and outbox entries can share transactions.
- Redis was removed after tests showed that PostgreSQL continued receiving events and alerts and the dashboard stayed available with Redis stopped; no dashboard read path consumed its projections.
- Kafka UI and the telemetry consumer were removed because they duplicated broker inspection and validation paths without serving the application.
- Grafana is optional because Prometheus and the application remain usable without its dashboard container.
- At-least-once delivery plus stable identities and upserts accepts replay without accepting duplicate durable effects. Offsets never advance ahead of the database transaction.

## Known limitations

- Runtime events are synthetic by default. The Cowrie path has only been exercised with the checked-in synthetic sample, not a live honeypot.
- CIC-IDS-2017 is a lab dataset with known timestamp/header cleaning issues documented in the evaluation output. It does not support a live-network detection claim.
- Precision is low and all evaluated web-attack windows were missed.
- Four YAML rules are enabled. Three are disabled: impossible travel lacks two timestamped locations, privilege escalation lacks before/after privilege data, and process-chain detection lacks process/parent/command-line fields.
- Threat-intelligence enrichment uses hardcoded demonstration countries, domains, usernames, and hashes rather than maintained external feeds.
- Windowed alerts deliberately take minutes because of window closure and watermarking, and can take longer under backlog.
- The hardened deployment is experimental and excluded from CI.
- ZooKeeper and Elasticsearch remain. Neither has been replaced or evaluated as a multi-node service.
- No multi-node benchmark, multi-hour soak, host-loss exercise, or live-network detection study has been run.
- Elasticsearch can lag PostgreSQL while durable outbox work retries.
- The application is single-organization and has no blocking or response integration.

## Reproduce the results

### Evaluation

Download the MachineLearningCSV files from the [University of New Brunswick CIC-IDS-2017 page](https://www.unb.ca/cic/datasets/ids-2017.html). The repository does not contain the dataset. Keep the filenames expected by eval/data.py under a private directory, then run:

~~~bash
export CICIDS_DIR=/absolute/private/path/TrafficLabelling
python -m eval.run
~~~

Citation: Iman Sharafaldin, Arash Habibi Lashkari, and Ali A. Ghorbani, “Toward Generating a New Intrusion Detection Dataset and Intrusion Traffic Characterization,” ICISSP, 2018.

Dataset-independent evaluation tests:

~~~bash
pytest -q tests/reliability/test_detection_evaluation.py
~~~

### Benchmark

The full runner removes project volumes between runs and writes only benchmarks/results/latest.md:

~~~bash
python -m benchmarks.run --full
~~~

A shorter functional run is:

~~~bash
python -m benchmarks.run --quick
~~~

The full profile is expensive: its defaults are five rates, three 300-second throughput repetitions per rate, engine comparisons, a shared equivalence run, and three watermark runs. Do not compare results from different hardware or Docker resource limits as if they were the same experiment.

## Repository layout

| Path | Purpose |
| --- | --- |
| backend/ | FastAPI APIs, authentication, PostgreSQL models |
| frontend/ | React dashboard |
| kafka-producers/ | Synthetic producer and Cowrie adapter |
| spark-jobs/ | Spark local streaming, windows, features, checkpoints |
| ml-models/ | 18-field model contract, training, inference |
| alert-engine/ | Alert rules, cooldowns, incident correlation |
| threat-intelligence-engine/ | IOC demonstrations, YAML rules, entity timelines |
| storage-sink/ | Batched PostgreSQL persistence and search outbox |
| eval/ | CIC-IDS-2017 offline evaluation |
| benchmarks/ | Reproducible single-node benchmark runner and latest result |
| tests/ | Reliability, recovery, hardening, and browser tests |
| docs/ | Current guides; historical phase reports are under docs/archive/ |
| deploy/hardened/ | Experimental isolated deployment |
