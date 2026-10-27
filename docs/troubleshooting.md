# Troubleshooting

## Compose does not start

```bash
docker compose config --quiet
docker compose ps --all
docker compose logs --tail=200
```

Run `python3 scripts/setup_local.py` if `.env` or the private metrics credential is
missing. The script preserves existing settings and volumes.

## Dashboard is empty

The default producer is synthetic and the pipeline has multiple Kafka stages. Check
service health and then run the deterministic lineage check:

```bash
python3 scripts/health_check.py
./scripts/lineage_check.sh
```

Windowed output uses a one-minute window, a 30-second slide, and a two-minute
watermark, so its alerts can take minutes. Direct enriched-event rules do not need to
wait for window closure.

## Authentication

Fresh volumes have no human account. Create the one-time initial administrator with:

```bash
docker compose exec backend python -m app.auth.provision
```

Use the same hostname (`localhost`) for the frontend and API. The development-only
HTTP cookie exception is restricted to loopback origins.

## Elasticsearch is slow or unavailable

```bash
curl -fsS http://localhost:9200/_cluster/health
```

PostgreSQL is authoritative. Elasticsearch is populated from a durable outbox, so
search may be temporarily behind while retries continue.

## Resource pressure

Grafana is not part of the default profile. Stop it without stopping the measured
stack:

```bash
docker compose stop grafana
```

The latest measured laptop configuration and resource samples are in
`benchmarks/results/latest.md`.
