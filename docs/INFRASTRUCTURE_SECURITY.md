# Experimental hardened deployment

`deploy/hardened/compose.yml` is a separate experimental topology. It is not merged
with root Compose, is not covered by Fast Checks, Stack Smoke, or Security Scans, and
has not been benchmarked with the numbers in `benchmarks/results/latest.md`.

The definition demonstrates generated per-service credentials, TLS, Kafka ACLs,
network separation, non-root containers, read-only filesystems, and restricted Linux
capabilities. It retains its own secured Spark master/worker layout. That layout is
not part of the default stack, which uses Spark `local[2]`.

## Isolated test setup

Only use a new private directory outside the repository:

```bash
python3 scripts/provision_hardened.py \
  --directory /absolute/private/path/hardened-secrets \
  --test-only
export HARDENED_SECRETS_DIR=/absolute/private/path/hardened-secrets
docker compose -f deploy/hardened/compose.yml --profile setup run --rm permissions
```

The provisioner refuses to overwrite an existing directory. The `--test-only`
marker authorizes only the opt-in isolated tests; it does not turn the configuration
into a supported deployment. Keep its CA keys, passwords, generated stores, logs,
and test output outside Git.

## Boundaries

- Single broker, single PostgreSQL instance, and single Elasticsearch node remain
  single points of failure.
- Host loss, multi-node failover, backup restore, long soak behavior, and capacity
  are unverified.
- Generated certificates and passwords are test material, not an external secret or
  certificate-management system.
- Vulnerability workflows report findings without automatically fixing or waiving
  them.
- Historical hardened-stack investigations are preserved under `docs/archive/` and
  do not describe the default architecture.
