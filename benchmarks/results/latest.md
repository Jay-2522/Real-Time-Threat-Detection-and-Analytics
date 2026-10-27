# Latest single-node benchmark

Generated: 2026-10-26T17:57:04.611539+00:00

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
| 200 | 2 | 199.92 (199.92–199.93) | 100.00 (100.00–100.00) | 87244 (87002–87486) | 0/2 | streaming-analytics-threat-intelligence |
| 500 | 2 | 499.98 (499.97–500.00) | 100.00 (100.00–100.00) | 235000 (233667–236333) | 0/2 | streaming-analytics-threat-intelligence |
| 1000 | 2 | 999.99 (999.98–999.99) | 100.00 (100.00–100.00) | 463104 (453498–472709) | 0/2 | streaming-analytics-threat-intelligence |

## Per-hop latency

| Rate | Hop | Samples | Median seconds | Median p95 seconds |
| --- | --- | --- | --- | --- |
| 200 | raw topic → enriched topic | 120000 | 18.18 | 36.80 |
| 200 | enriched topic → feature topic | 1200 | 132.58 | 154.71 |
| 200 | feature topic → prediction topic | 1200 | 1.50 | 6.64 |
| 200 | enriched topic → direct alert row | 8 | 122.35 | 184.02 |
| 200 | feature topic → windowed alert row | 300 | 183.33 | 185.83 |
| 200 | window first event → alert row | 300 | 374.33 | 382.80 |
| 200 | deliberate window + watermark | 300 | 162.10 | 167.58 |
| 200 | remaining window processing | 300 | 212.20 | 215.49 |
| 500 | raw topic → enriched topic | 300000 | 15.14 | 34.67 |
| 500 | enriched topic → feature topic | 1200 | 130.93 | 139.68 |
| 500 | feature topic → prediction topic | 1200 | 1.39 | 2.29 |
| 500 | enriched topic → direct alert row | 10 | 197.83 | 342.22 |
| 500 | feature topic → windowed alert row | 200 | 362.21 | 362.65 |
| 500 | window first event → alert row | 200 | 534.19 | 535.38 |
| 500 | deliberate window + watermark | 200 | 147.63 | 147.72 |
| 500 | remaining window processing | 200 | 386.54 | 387.71 |
| 1000 | raw topic → enriched topic | 600000 | 20.90 | 32.94 |
| 1000 | enriched topic → feature topic | 1200 | 131.93 | 146.49 |
| 1000 | feature topic → prediction topic | 1200 | 0.55 | 1.19 |
| 1000 | enriched topic → direct alert row | 10 | 199.27 | 358.30 |
| 1000 | feature topic → windowed alert row | 100 | 304.45 | 305.08 |
| 1000 | window first event → alert row | 100 | 491.51 | 492.03 |
| 1000 | deliberate window + watermark | 100 | 135.53 | 135.58 |
| 1000 | remaining window processing | 100 | 355.98 | 356.51 |

## Startup and fixed-elapsed resources

| Rate | Run | Time to healthy s | Running services | Memory sample elapsed s | Total memory MiB |
| --- | --- | --- | --- | --- | --- |
| 200 | 1 | 70.24 | 13 | 60.01 | 3692.78 |
| 200 | 2 | 73.11 | 13 | 60.01 | 3490.49 |
| 500 | 1 | 71.39 | 13 | 60.00 | 3388.29 |
| 500 | 2 | 71.30 | 13 | 60.01 | 3788.29 |
| 1000 | 1 | 72.02 | 13 | 60.01 | 3560.28 |
| 1000 | 2 | 65.45 | 13 | 60.01 | 4474.00 |

## Feature engine comparison

Not measured in this throughput-only profile.

## Shared-sample feature equivalence

Not measured in this throughput-only profile.

## Watermark trade-off at 200 events/s and 10% configured disorder

Not measured in this throughput-only profile.

## Scope

Profile: `full`; duration per load: 300 s; throughput repetitions: 2.
No multi-node test, multi-hour soak, failure injection, cloud network, or production-server hardware was measured.
Tail windows that cannot close without newer event time are excluded from closed-window loss calculations.
Spark Structured Streaming does not expose a conventional committed Kafka consumer group here; drain also requires one enriched output per delivered input.
