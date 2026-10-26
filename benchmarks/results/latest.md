# Latest single-node benchmark

Generated: 2026-10-25T20:23:32.378976+00:00

These are single-node laptop measurements, not production capacity claims.

## Hardware

| Item | Value |
| --- | --- |
| machine | MacBook Air |
| chip | Apple M5 |
| ram | 16 GB |
| os | macOS 26.7.1 (25G241) |
| docker | client=29.9.1 server=29.5.2 |
| docker vm | Colima 4 CPU, 8 GiB RAM, aarch64 |

## Full-stack throughput

| Requested events/s | Runs | Achieved median (range) | Persisted % median (range) | Max lag median (range) | Clean runs | First observed bottleneck |
| --- | --- | --- | --- | --- | --- | --- |
| 50 | 3 | 50.00 (50.00–50.00) | 100.00 (100.00–100.00) | 4877 (4853–5102) | 3/3 | none observed |
| 200 | 3 | 200.00 (200.00–200.00) | 65.58 (58.91–66.36) | 88936 (85100–91844) | 0/3 | streaming-analytics-storage-sink |
| 500 | 3 | 499.99 (499.96–500.00) | 24.52 (22.51–24.90) | 268406 (258772–278162) | 0/3 | streaming-analytics-storage-sink |
| 1000 | 3 | 999.94 (999.85–999.98) | 16.76 (15.87–16.79) | 568727 (562251–571398) | 0/3 | streaming-analytics-storage-sink |
| 2000 | 3 | 1999.97 (1999.91–1999.98) | 8.83 (8.36–9.63) | 575379 (575056–856692) | 0/3 | Spark enriched path |

## Per-hop latency

| Hop | Samples | Median seconds | Median p95 seconds |
| --- | --- | --- | --- |
| raw topic → enriched topic | 3208489 | 13.81 | 31.92 |
| enriched topic → feature topic | 8600 | 137.74 | 152.39 |
| feature topic → prediction topic | 8600 | 0.80 | 1.67 |
| enriched topic → direct alert row | 56 | 177.83 | 309.03 |
| feature topic → windowed alert row | 953 | 47.67 | 48.28 |
| window first event → alert row | 953 | 240.99 | 242.50 |
| deliberate window + watermark | 953 | 153.18 | 171.14 |
| remaining window processing | 953 | 70.09 | 71.55 |

## Feature engine comparison

| Engine | Requested events/s | Achieved events/s | Median CPU % | Median memory MiB | Median window latency s | Drained |
| --- | --- | --- | --- | --- | --- | --- |
| Spark cluster | 50 | 50.00 (50.00–50.00) | 8.72 | 1931.00 | 137.45 | yes |
| Spark local[2] | 50 | 50.00 | 5.14 | 884.20 | 151.48 | yes |
| Plain Python | 50 | 50.12 | 2.50 | 26.81 | 121.02 | yes |
| Spark cluster | 200 | 200.00 (200.00–200.00) | 12.77 | 2110.89 | 135.85 | no |
| Spark local[2] | 200 | 200.00 | 0.96 | 943.00 | 151.09 | yes |
| Plain Python | 200 | 200.37 | 8.20 | 30.75 | 120.26 | yes |
| Spark cluster | 500 | 499.99 (499.96–500.00) | 23.07 | 2088.08 | 135.83 | no |
| Spark local[2] | 500 | 500.00 | 1.16 | 968.80 | 150.81 | yes |
| Plain Python | 500 | 501.29 | 15.90 | 37.67 | 120.11 | yes |
| Spark cluster | 1000 | 999.94 (999.85–999.98) | 46.85 | 2405.02 | 140.51 | no |
| Spark local[2] | 1000 | 1000.00 | 0.69 | 1083.39 | 148.08 | yes |
| Plain Python | 1000 | 1002.76 | 15.60 | 54.97 | 120.06 | yes |
| Spark cluster | 2000 | 1999.97 (1999.91–1999.98) | 65.84 | 2324.65 | 257.22 | no |
| Spark local[2] | 2000 | 1999.97 | 1.38 | 1131.52 | 256.07 | no |
| Plain Python | 2000 | 2005.58 | 20.80 | 86.91 | 120.03 | yes |

## Shared-sample feature equivalence

| Common windows | Compared values | Mismatched values | Mismatch rate | Spark-only windows | Python-only windows |
| --- | --- | --- | --- | --- | --- |
| 600 | 10800 | 0 | 0.00 | 0 | 0 |

## Watermark trade-off at 200 events/s and 10% configured disorder

| Watermark | Median end-to-end s | Observed out-of-order events | Missing closed-window memberships | Missing % | Drained |
| --- | --- | --- | --- | --- | --- |
| 120 s | 219.87 | 5988 | 15 | 0.03 | no (timeout) |
| 60 s | 164.06 | 5988 | 943 | 1.16 | no (timeout) |
| 30 s | 128.52 | 5988 | 2977 | 2.88 | no (timeout) |

## Scope

Profile: `full`; duration per load: 300 s; throughput repetitions: 3.
No multi-node test, multi-hour soak, failure injection, cloud network, or production-server hardware was measured.
Tail windows that cannot close without newer event time are excluded from closed-window loss calculations.
Spark Structured Streaming does not expose a conventional committed Kafka consumer group here; drain also requires one enriched output per delivered input.
