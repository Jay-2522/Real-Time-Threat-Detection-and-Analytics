# Infrastructure security and isolated deployment

Phase 3B is a security improvement, not production release approval. Use `dev` only.
The historical development cluster, Spark worker, failed Spark driver, checkpoints,
volumes and consumer offsets must remain intact. Never merge the two Compose files.
The hardened deployment provisions NEW empty state; it does not migrate the existing
ZooKeeper Kafka cluster to KRaft or copy historical application data.

## Trust boundaries and inventory

| Service | Development exposure/control | Hardened control |
| --- | --- | --- |
| PostgreSQL | Published port, password, no required TLS | Internal database network, verified TLS, SCRAM, four scoped runtime roles and pipeline row policies; schema administration is separate |
| Elasticsearch | Published port, security disabled | Internal search network, HTTP/transport TLS, separate scoped service users |
| Kafka | Published plaintext listeners, no client ACL enforcement | Internal messaging network, mTLS identities, default-deny topic/group ACLs, isolated KRaft state |
| ZooKeeper | Development-only coordination service | Excluded; no automatic migration of historical Kafka state |
| Spark master/worker/driver | Published management ports; worker configuration requests root; original worker actually runs as spark | Internal compute network; UID 185; authenticated encrypted RPC; read-only root filesystems; dedicated writable work/checkpoint volumes |
| Producer/consumer/ML/storage/alerts/threat | Development metrics ports, shared infrastructure credentials | UID 10001; no published ports; separate credentials; minimal network membership; read-only root filesystems |
| Backend | Development HTTP/reload and loopback-cookie exception | UID 10001; no host port/reload; secure cookies; only proxy address trusted; authenticated monitoring |
| Frontend/proxy | Vite development server | Built static assets; UID 101; HTTPS; CSP/HSTS; read-only root filesystem |
| Prometheus | Published unauthenticated HTTP | Internal monitoring network; Basic authentication; backend scrape bearer token |
| Grafana | Published HTTP administration | Internal monitoring network; file-based administrator password; anonymous access/signup disabled |

The preserved development deployment publishes loopback ports 2181, 7077,
8000, 8081–8082, 9090, 9092, 9101–9106, 9200, 13000, 15432 and 5173.
They are not verified as Internet-accessible. Only `127.0.0.1:8443` is published
by the separate hardened stack. Its certificate and CSP
are intentionally configured for `https://localhost:8443`; this is a reproducible
single-host security verification deployment, not an Internet hostname template.
Only the proxy joins an external bridge. Every other bridge is internal. The web
bridge reserves proxy `172.29.250.2` and backend `.3`; check subnet conflicts before
use. Do not widen Uvicorn's trusted proxy range.

Proxy-to-backend and monitoring HTTP stay unencrypted inside isolated, same-host
bridges. Worker metrics have no per-request authentication on the monitoring
bridge. Docker administrators and members of these bridges are trusted. This is
not adequate isolation against a compromised Docker host or arbitrary monitoring
network peers. Multi-host deployment requires authenticated encrypted links there.
Spark has a size-limited executable `/tmp` tmpfs because JNI codecs must load
native libraries; its root filesystem remains read-only. Standalone master/worker
UIs remain internal to the trusted compute network and have no public port.
Elasticsearch and Kafka retain writable vendor runtime configuration directories;
all services drop capabilities and forbid privilege escalation. The one-shot
permissions initializer is root with only CHOWN, DAC_OVERRIDE and FOWNER, no
network, and mounts only the new deployment volumes.

## Provision and start

Prerequisites: Docker Compose v2, Python 3, OpenSSL supporting `pkcs12 -jdktrust`,
and `htpasswd` with bcrypt support. Docker must have sufficient RAM for both the
preserved development stack and this separate deployment. Do not resolve resource
pressure by restarting or deleting preserved Spark containers/state.

Run from the repository root. Select the intended Docker context explicitly.
Use a new private directory outside version control and verified contract-v2 ML
artifacts. The provisioning scripts refuse to replace an existing deployment.

```sh
export HARDENED_SECRETS_DIR="$PWD/.secrets/hardened-new"
export HARDENED_MODEL_DIR=/absolute/path/to/verified/contract-v2
python3 scripts/provision_hardened.py --directory "$HARDENED_SECRETS_DIR"
python3 scripts/provision_messaging.py --directory "$HARDENED_SECRETS_DIR"
docker compose -f deploy/hardened/compose.yml --profile '*' config --quiet
docker compose -f deploy/hardened/compose.yml build
docker compose -f deploy/hardened/compose.yml run --rm permissions
docker compose -f deploy/hardened/compose.yml up -d postgres elasticsearch
# Wait for PostgreSQL and Elasticsearch to accept authenticated TLS connections.
docker compose -f deploy/hardened/compose.yml run --rm bootstrap
docker compose -f deploy/hardened/compose.yml up -d backend web prometheus grafana
```

Streaming profiles are separate so resource limits and credentials can be checked
before any pipeline starts. Build/start them explicitly, initialize ACLs before
producers, and use only the new checkpoint volume:

```sh
docker compose -f deploy/hardened/compose.yml --profile streaming --profile workers build
docker compose -f deploy/hardened/compose.yml up -d kafka
# Wait for the new broker; then provision its topics and ACLs.
docker compose -f deploy/hardened/compose.yml run --rm kafka-init
docker compose -f deploy/hardened/compose.yml up -d spark-master spark-worker
docker compose -f deploy/hardened/compose.yml up -d spark-driver
docker compose -f deploy/hardened/compose.yml --profile workers up -d
```

No human administrator is created by infrastructure bootstrap. Provision the initial administrator interactively before normal use:
`docker compose -f deploy/hardened/compose.yml exec backend python -m app.auth.provision`.
This uses the backend role and the one-time bootstrap guard. Do not use test fixture accounts in a
real deployment. Do not publish Grafana/Prometheus/Kafka administration ports for
convenience. Grafana is not yet exposed through the HTTPS application proxy.

## Secrets and rotation

The secret directory is mode 0700 on the host; each container mounts only its own
subdirectory. Individual mounted files are readable by the non-root runtime user.
The private CA key remains in the unmounted authority directory. No password is
passed as a command-line argument or committed. Bootstrap mounts administrative
credentials only for provisioning. PostgreSQL/Elasticsearch private files are
copied into dedicated owned volumes to satisfy vendor file-mode requirements.

Leaf certificates expire after 90 days; the local CA expires after 365 days.
Production requires an operational certificate authority and expiry monitoring.
Missing required production configuration fails closed; do not set development
fallbacks to bypass it. Certificate trust and hostname checks remain enabled.

Rotation is an explicit maintenance operation: back up credentials and persistent
state securely; issue replacement leaves with the same service SAN/principal;
prepare new credential files; update server-side PostgreSQL roles, Elasticsearch
users and monitoring credentials through their authenticated admin
interfaces; replace the matching client files; restart only affected hardened
services and re-run verification. For CA rotation distribute both CA certificates
before replacing leaves, then retire the old CA after all peers are verified.
Re-run the permissions initializer only for this deployment's certificate volumes.
The bootstrap script does not rotate existing PostgreSQL role passwords. Do not
regenerate a Kafka cluster ID or delete state as a password-rotation shortcut.
Spark RPC secret changes require a coordinated stop/start of the isolated Spark
cluster, preserving its checkpoint volume; never modify historical checkpoints.

## Health and verification

`GET /api/v1/health` and `/health/live` are public process-liveness probes.
`GET /api/v1/health/ready` checks access to the authorization control row and alert
schema using bounded PostgreSQL connections/statements; it returns 503 on database
failure and never returns connection diagnostics. Search/metrics are optional
for core persisted API reads and appear in authenticated detailed monitoring.

Security tests intentionally create fixture accounts and stop the isolated
PostgreSQL service. They require a NEW deployment provisioned with `--test-only`;
never add that marker to a real deployment. Run:

```sh
HARDENED_INTEGRATION=1 HARDENED_WORKERS_INTEGRATION=1 \
  .venv/bin/python -m pytest tests/hardening/test_configuration.py tests/hardening/test_stack.py -ra
HARDENED_STREAMING_INTEGRATION=1 .venv/bin/python -m pytest tests/hardening/test_streaming.py -ra
AUTH_BROWSER_INTEGRATION=1 HARDENED_INTEGRATION=1 HARDENED_WORKERS_INTEGRATION=1 \
  HARDENED_STREAMING_INTEGRATION=1 HARDENED_RECOVERY_INTEGRATION=1 \
  HARDENED_MONITORING_INTEGRATION=1 .venv/bin/python -m pytest -ra
(cd frontend && npm run lint && npm run build)
.venv/bin/pip-audit -r requirements-test.txt
(cd frontend && npm audit)
trivy image --scanners vuln IMAGE
```

The legacy `PIPELINE_INTEGRATION` and `SPARK_RECOVERY_INTEGRATION` opt-ins
target development endpoints. Leave them disabled when preserving that deployment;
the hardened recovery tests use marked disposable state and verified TLS instead.
Run the actual Spark contract test first and set `SPARK_CONTRACT_OUTPUT` to its
local JSON output to enable the cross-service mapper check.

Keep scan results, certificates, fixtures and runtime evidence outside Git.
The tests permanently guard fail-closed configuration, TLS/ACL rejection, secure
browser authentication/WebSockets, network reachability and readiness recovery.

## Historical Phase 3B vulnerability assessment (17 October 2026)

Trivy findings are package matches, not proof of exploitability. Reachability for
individual findings is unverified unless stated. Do not treat isolation as a fix.
These are the captured **original image** counts; image tags can change.

| Original image | Critical | High |
| --- | ---: | ---: |
| apache/spark:3.5.3 | 6 | 131 |
| confluentinc/cp-kafka:7.6.1 | 3 | 199 |
| confluentinc/cp-zookeeper:7.6.1 | 3 | 199 |
| docker.elastic.co/elasticsearch/elasticsearch:8.15.3 | 9 | 106 |
| grafana/grafana:11.3.0 | 9 | 138 |
| postgres:16-alpine | 1 | 24 |
| prom/prometheus:v2.55.1 | 4 | 106 |
| streaming_analytics_system-alert-engine | 0 | 50 |
| streaming_analytics_system-backend | 0 | 57 |
| streaming_analytics_system-frontend | 2 | 79 |
| streaming_analytics_system-ml-inference | 1 | 114 |
| streaming_analytics_system-storage-sink | 0 | 50 |
| streaming_analytics_system-telemetry-producer | 0 | 50 |
| streaming_analytics_system-threat-intelligence-engine | 0 | 50 |

One historical untagged image reference (`95f80e5ba3b2`) could not be resolved by
the daemon and remains unscanned. No claim of complete historical image coverage.

Python dependency remediation includes python-dotenv 1.0.1 → 1.2.4
(PYSEC-2026-2270), FastAPI 0.115.6 → 0.143.3 with Starlette 1.8.0,
and pytest 8.3.4 → 9.1.1 in the test environment (PYSEC-2026-1845).
Test-only pytest and build packaging tools are excluded from hardened runtime images.

The compatible npm lockfile update reduced findings from 20 to 9 (5 high,
4 moderate). Remaining dependency chains include Tailwind 3 / braces / micromatch /
chokidar / fast-glob and postcss helpers, plus React Router. Tailwind build-tool
findings are absent from the static Nginx runtime filesystem; their build-time
exposure remains. React Router is shipped to browsers. Suggested major upgrades
require separate compatibility work; they have not been force-applied or suppressed.

Relevant remaining npm advisories: [braces stack exhaustion](https://github.com/advisories/GHSA-vfj7-8cjw-p6xm),
[selector parsing complexity](https://github.com/advisories/GHSA-rj75-hqrm-r3gf),
[Router redirect bypass](https://github.com/advisories/GHSA-wrjc-x8rr-h8h6), and
[Router SSR hydration](https://github.com/advisories/GHSA-337j-9hxr-rhxg).
This application builds a client-side SPA, not an SSR server; SSR-specific
reachability has not been demonstrated. Browser redirect handling still needs
review before dismissing the Router findings.

The preserved development Spark 3.5.3 image uses Python 3.8. Its conditional
python-dotenv 1.0.1 pin remains vulnerable (PYSEC-2026-2270); upgrading only the
driver to Python 3.11 would break compatibility with the preserved worker.
The hardened Spark 3.5.7 driver and worker both use Python 3.11 and dotenv 1.2.4.
Python audit results below apply to Python 3.11, not the legacy Python 3.8 image.
Migration of the preserved worker is explicitly deferred and remains a blocker.

### Tested hardened image results

| Image/runtime | Critical | High |
| --- | ---: | ---: |
| confluentinc/cp-kafka:7.6.14 | 3 | 65 |
| docker.elastic.co/elasticsearch/elasticsearch:8.19.22 | 0 | 36 |
| grafana/grafana:11.6.16 | 1 | 71 |
| soc-hardened-spark | 8 | 165 |
| soc-hardened-backend | 0 | 44 |
| soc-hardened-postgres | 0 | 0 |
| soc-hardened-telemetry-producer | 0 | 44 |
| soc-hardened-ml-inference | 0 | 44 |
| soc-hardened-storage-sink | 0 | 44 |
| soc-hardened-alert-engine | 0 | 44 |
| soc-hardened-threat-intelligence-engine | 0 | 44 |
| soc-hardened-bootstrap | 0 | 44 |
| alpine:3.23 | 0 | 0 |
| soc-hardened-web (nginx 1.28.3-r7) | 0 | 0 |
| prom/prometheus:v2.55.1 (unchanged vendor image) | 4 | 106 |

The web image retains one medium finding. PostgreSQL package upgrades
and removal of PostgreSQL’s unused root-only gosu helper removed its high/critical
findings. All seven service requirement audits report zero known vulnerabilities
on Python 3.11. This does not override the container/JAR/Go findings above.

### Unresolved critical examples and release blockers

| Component | Finding | Installed | Scanner-listed fix |
| --- | --- | --- | --- |
| confluentinc/cp-kafka:7.6.14: python3-unbound | CVE-2026-81642 | 1.16.2-5.14.el8_10 | 1.16.2-5.14.el8_10.4 |
| confluentinc/cp-kafka:7.6.14: unbound-libs | CVE-2026-81642 | 1.16.2-5.14.el8_10 | 1.16.2-5.14.el8_10.4 |
| confluentinc/cp-kafka:7.6.14: io.netty:netty-handler | CVE-2026-75595 | 4.1.136.Final | 4.2.17.Final, 4.1.137.Final |
| grafana/grafana:11.6.16: github.com/getkin/kin-openapi | GHSA-r277-6w6q-xmqw | v0.132.0 | 0.144.0 |
| soc-hardened-spark: libsqlite3-0 | CVE-2025-7458 | 3.40.1-2+deb12u2 | No fix listed |
| soc-hardened-spark: zlib1g | CVE-2023-45853 | 1:1.2.13.dfsg-1 | No fix listed |
| soc-hardened-spark: io.netty:netty-handler | CVE-2026-75595 | 4.1.96.Final | 4.2.17.Final, 4.1.137.Final |
| soc-hardened-spark: org.apache.avro:avro | CVE-2024-47561 | 1.7.7 | 1.11.4 |
| soc-hardened-spark: org.apache.derby:derby | CVE-2022-46337 | 10.14.2.0 | 10.14.2.1, 10.16.1.2, 10.15.2.1 |
| soc-hardened-spark: org.apache.zookeeper:zookeeper | CVE-2023-44981 | 3.6.3 | 3.7.2, 3.8.3, 3.9.1 |
| soc-hardened-spark: org.codehaus.jackson:jackson-mapper-asl | CVE-2019-10202 | 1.9.13 | No fix listed |

These require supported vendor upgrades and compatibility/reachability review.
Do not replace individual Spark/Kafka JARs blindly or dismiss findings solely
because the affected image is internal. Spark’s newer runtime still has more
reported high/critical package matches than the original image; it is not claimed
to remediate all Spark vulnerabilities. Prometheus remains on its existing v2
release pending a separately verified major-version upgrade.

Production blockers: unresolved critical/high image and npm findings; legacy
development Spark migration; certificate lifecycle automation; capacity/soak
verification of the entire hardened pipeline running concurrently; and stronger
protection if internal bridge peers are not trusted. No data migration, production
deployment, or promotion to main was performed. Only PostgreSQL, backend and web
have Docker health probes; worker/Spark readiness currently requires operational
metrics and external verification. Tags remain mutable: release engineering must
lock tested image digests and re-scan before any approved deployment.

## Historical Phase 3B verification outcome

CONDITIONAL PASS for the tested controls; production remediation remains incomplete.

- Final combined Python/browser/infrastructure/worker suite: **95 passed, 2 skipped,
  1 deselected**. The excluded tests were executed separately: **2 secured Kafka/Spark
  tests passed**, and the isolated crash/replay/continuation test passed in the earlier
  full run. All **98 collected tests** have successful evidence across these runs.
- After tightening pipeline row policies, the focused hardened
  suite passed **12/12** checks.
- Actual Spark feature-contract suite: **5 passed**, also repeated successfully
  against the rebuilt development-compatible image.
- Earlier full run: 86 passed, 1 failed, 5 skipped; the failed ML test selected IPv6
  localhost against an IPv4 Docker listener. Its explicit IPv4 correction passed
  both the focused rerun and final combined suite.
- Frontend lint: zero errors, two existing hook warnings. TypeScript and production
  build passed; existing large-bundle warning remains.
- Both Compose configurations validate; new credential provisioning was reproduced
  in a separate private directory. Hardened images built successfully.
- Live tests cover unknown-CA rejection, anonymous/unauthorized infrastructure
  access, worker SQL/search/cache scopes, mTLS Kafka delivery/ACL denial, non-root
  authenticated Spark execution and writable isolated checkpoints, blocked direct
  network access, authenticated monitoring, secure-cookie browser/WebSocket workflow,
  and database-outage readiness recovery while liveness stays up.
- Original Spark worker ID `7d1305599c45` and stopped driver ID `322fa4d3af1c` were
  preserved. The historical driver was never started.

## Phase 3C assessment (17 October 2026)

The Phase 3B numbers above are historical. Fresh Trivy 0.76.0 scans reproduced
that hardened baseline before this work. Findings count package occurrences,
including duplicate embedded libraries/binaries; they are not exploit counts.
No finding is suppressed. Runtime acceptance is reported separately below;
scanner reductions do not constitute production approval.

| Component | Baseline critical / high | Updated critical / high | Decision |
| --- | ---: | ---: | --- |
| Spark 3.5.7 / Debian 12 | 8 / 165 | 6 / 145 | Spark 3.5.9, matching Scala 2.12 Kafka connector; Python 3.11 on Debian 13; Java 17 retained |
| Confluent Kafka 7.6.14 | 3 / 65 | 1 / 48 | Same broker with UBI package errata; no independent Java library replacement |
| Prometheus 2.55.1 | 4 / 106 | 0 / 10 | 3.13.4 LTS |
| Grafana 11.6.16 | 1 / 71 | 0 / 5 | 12.4.13 supported maintenance line |

Confluent 7.9.10 was also scanned: **3 critical / 69 high**, with the same
critical Netty/unbound matches. It was rejected because it added compatibility
work without resolving those findings. The chosen UBI update installs vendor
`python3-unbound` and `unbound-libs` **1.16.2-5.14.el8_10.4**, fixing
CVE-2026-81642, and updates other supported OS packages. Broker version remains
7.6.14-ccs. Vendor Java libraries and startup tooling remain intact.
Spark runtime pip/setuptools/wheel are removed after dependency installation;
Python application dependencies remain unchanged and their audit passed.

### Pinned upstream inputs and provenance

All references below are upstream multi-platform manifest-list digests. Runtime
verification in this environment covers **linux/arm64**, not amd64. Dockerfiles
pin upstream inputs; package repositories and Python transitive dependencies
can still change a rebuilt image. Capture the resulting platform image digest,
SBOM and fresh scan for every subsequent build before deployment.

| Input | Pinned digest (SHA-256) |
| --- | --- |
| apache/spark:3.5.9 | `af02a459c8706e031c835c16f9db3c463816776e2543dd0f828af65606bcf392` |
| eclipse-temurin:17-jre | `e04fa72be13b257a59fdec6dc14af78c8f80f035d3c1ed4e03e45552a231d4c8` |
| python:3.11-slim-trixie | `e88e9763f943ec1834f992a4b51e0f24500486803e8bc534e5767af9ea65f6ce` |
| confluentinc/cp-kafka:7.6.14 | `b20da2c030172c556f1a5569ef70110e1b2937dcc03356a90da93ffa4467791f` |
| prom/prometheus:v3.13.4 | `87861b8cf91579109319ebc300f3f1060e6da9c05d6ae8ad15a20c879e84e32e` |
| grafana/grafana:12.4.13 | `41bbc1b5e04416f81891806076db2a2ff60a5d4da5f78d36a88b033a8c199185` |

Sources: [Spark maintenance recommendation](https://spark.apache.org/releases/spark-release-3-5-9.html),
[Confluent 7.6 changelog](https://docs.confluent.io/platform/7.6/release-notes/changelog.html),
[Prometheus LTS downloads](https://prometheus.io/download/),
[Prometheus 3 migration guide](https://prometheus.io/docs/prometheus/latest/migration/),
and [Grafana supported release lines](https://grafana.com/docs/grafana/latest/upgrade-guide/when-to-upgrade/).
BuildKit records build provenance locally; upstream signatures have not been
independently verified. A digest establishes content identity, not publisher trust.

### Residual critical findings and deployment boundaries

| Finding | Runtime evidence and exposure classification | Required action |
| --- | --- | --- |
| CVE-2026-75595, Netty (Spark 4.1.96.Final; Kafka 4.1.136.Final) | Libraries remain present. The exploit requires a Netty SNI-specific client-auth gate with a permissive default SSL context. Kafka uses mandatory listener-wide JSSE mTLS; Spark uses authenticated encrypted RPC, without that SNI routing configuration. Those exploit prerequisites are absent from this deployment. | Retain mandatory authentication and current transport design; obtain a compatible upstream bundle with Netty 4.1.137+; reassess any TLS/SNI configuration change. |
| CVE-2024-47561, Avro 1.7.7 (two occurrences) | Avro schema classes exist inside shaded Hadoop runtime JARs. The pipeline consumes JSON with fixed Spark schemas, not caller-supplied Avro schemas. No direct vulnerable schema-parsing entry point is configured; complete dynamic reachability is unverified. | Block untrusted Avro schemas and arbitrary Spark jobs. Upstream Hadoop/Spark remediation and deeper reachability review remain production gates. |
| CVE-2022-46337, Derby 10.14.2.0 | LDAP-authenticated Derby server is a prerequisite. No Derby network/LDAP service or Hive-enabled session is configured. | Do not enable those services; await an upstream-compatible fix instead of independently replacing Derby. |
| CVE-2023-44981, ZooKeeper 3.6.3 | The library is bundled, but this stack runs Kafka KRaft and standalone Spark without ZooKeeper recovery. No SASL-authenticated ZooKeeper quorum is deployed. | Keep this boundary; use a fixed upstream ZooKeeper before adding any quorum. |
| CVE-2019-10202, Codehaus Jackson 1.9.13 | ObjectMapper classes are present in standalone/shaded JARs. The application uses fixed Spark JSON schemas; an unsafe polymorphic Codehaus deserialization path has not been demonstrated or exhaustively ruled out. No scanner-listed fix. | Unsupported dependency replacement must come through compatible upstream Hadoop/Spark releases; deeper reachability review remains a production gate. |

The Debian 13 base removes the SQLite CVE-2025-7458 and zlib CVE-2023-45853
matches. The latter was already a source-package mismatch in the old runtime:
[Debian states the affected MiniZip code is not built into the Bookworm zlib binary](https://security-tracker.debian.org/tracker/CVE-2023-45853),
and runtime inspection found neither a MiniZip library nor pyminizip. No exclusion
was added to hide it. SQLite exists through Python, but this pipeline does not
accept arbitrary SQLite SQL.

The Spark base change introduces eight high package matches: CVE-2026-76642,
CVE-2026-78408, CVE-2026-78409 and CVE-2026-78410, each reported for
`login` and `liblastlog2-2` from util-linux. No fixed Debian version is listed.
These advisories concern privileged mount/nsenter operations. UID 185, dropped
capabilities, no-new-privileges and the absence of host-namespace access remain
required boundaries; they do not patch the packages. Track vendor errata and
reassess any change to users, capabilities or mounts.

Prometheus resolves the old critical gRPC CVE-2026-33186 and Go TLS
CVE-2025-68121 matches. Its remaining high matches are CVE-2026-78667,
CVE-2026-78669, CVE-2026-84445 and CVE-2026-97031 across Go stdlib, x/net
and gRPC, repeated in prometheus/promtool. The configuration has no xDS gRPC
server, TLS listener or ECH; HTTP Range/HTTP2 reachability requires further
review. Basic authentication and internal networking do not constitute a patch.

Grafana resolves GHSA-r277-6w6q-xmqw. Remaining high findings are
CVE-2026-100352/100353/100354 in kin-openapi and CVE-2026-21728/28377 in
its Tempo dependency. CVE-2026-100353 is a newly introduced package match
relative to the old image and is not omitted from acceptance. There is no Tempo
server or S3 SSE-C configuration in this stack; Grafana's OpenAPI decoder paths
still need detailed reachability review and upstream patches. Anonymous access
must remain disabled; retain file-based administrator credentials and internal-only access.

Full per-package findings (installed/fixed versions, paths, source references,
reachability/actionability notes), rejected-candidate scans and generated evidence
remain under the ignored local `.codex-logs/phase3c/` directory. Generic high
findings not specifically analyzed remain **unverified**, not "non-exploitable".
No comprehensive proof of high-severity exploitability or absence is claimed.

### Final artifact identity

The isolated arm64 runtime uses these platform manifest digests. Export/import
preserved every root filesystem layer and the complete runtime configuration
compared with the freshly scanned source images. The upstream index digests
above select these architecture-specific artifacts for the monitoring images.

| Component | Tested arm64 platform manifest (SHA-256) |
| --- | --- |
| spark | `4b8fdd002a360f5c8241bca3d892dc428069908a326078cb1c498d3c662c3b31` |
| kafka | `4146b2ed5281904544938f61cbf96fdc3095d45ea148e77d06fa88604966244f` |
| prometheus | `765261de302accc2aa2fa1c74aed31ce51c0acd92a0a55406903dbacc18ea39c` |
| grafana | `04ddb75abcf1584ac60149bf75a40b4d0a8dbe40bbdcf0b5c10a1ad6c0562634` |

Four CycloneDX SBOMs, complete scanner JSON, and per-package comparisons are
retained locally and excluded from Git. A removed package match can reflect a
package rename or packaging change; the complete introduced/remaining lists
must be considered alongside the resolved list. No ignore rule was added.

### Upgrade and rollback boundaries

Prometheus 3 changes scrape Content-Type validation and some PromQL boundary
semantics. Retain the existing seven jobs, labels, seven-day retention and basic
API authentication; check exporter health and every repository dashboard query.
Grafana 12 migrates its database on startup. Before any approved deployment,
stop writes and take restorable backups of monitoring volumes and provisioning
configuration. Retain the previous images. A rollback must restore the matching
pre-upgrade data backup; do not point an older binary at an upgraded database.
No migration or downgrade of retained Phase 3B or original development state is
authorized by this validation. The isolated test is not a substitute for backups
and staging validation of the actual deployment's data size and plugins.

### Phase 3C runtime evidence and limitations

Validation uses a separate Colima VM, new test-only credentials, fresh volumes,
unique Kafka topics/groups and isolated Spark checkpoints. The arm64 VM has
4 GiB RAM, two CPUs and temporary 2 GiB swap. This is functional validation,
not sustained-load or production-capacity evidence. Host memory pressure delayed
initialization; pausing an unrelated simulator workload improved responsiveness.

- Spark 3.5.9 / Java 17.0.20.1 / Python 3.11.17: five actual feature-contract
  tests passed. The runtime runs as UID 185 with zero effective capabilities,
  no-new-privileges, and no pip/setuptools/wheel. SQLite is 3.46.1.
- Secured streaming: two tests passed for Kafka mTLS, anonymous/ACL rejection,
  non-root Spark execution, authenticated encrypted RPC, I/O encryption,
  Kafka TLS reads and isolated checkpoint writes.
- Infrastructure: six HTTPS/browser/WebSocket, identity, network, monitoring
  authentication and database-outage/readiness tests passed. All three scoped
  worker SQL/search/cache tests passed after worker initialization completed.
- Monitoring: the seven configured exporters scrape successfully, Grafana can
  query its protected datasource, and anonymous API access is rejected. The
  baseline-to-upgrade migration preserved five repository dashboards, their
  panel counts, datasource UID and a historical backend metric sample. Every
  repository dashboard PromQL expression executed successfully. Seven-day
  retention, labels, credentials and network exposure are unchanged.
- All five running Spark master/worker, Kafka, Prometheus and Grafana services
  matched the scanned image filesystem layers and runtime configuration.
  Spark Python requirements audit found zero known vulnerabilities.

The Kafka CLI initialized topics and part of the ACL inventory. Because repeated
JVM startup was slow under host pressure, a disposable Admin API helper completed
and verified the exact 25 ACL entries from `kafka-init.sh`. No production ACL scope
was broadened. The recovery fixture adds only its unique test topic prefix for
Spark. Startup ordering, an Admin-client lifetime bug, a JSON/JSONB assertion and
a too-short crashed-member wait caused preliminary test failures; these are test
setup/assertion corrections, not weakened security controls or application fixes.

Original state: all 36 original container IDs were retained. The original Spark
worker retained its start time, and the damaged-checkpoint driver remained stopped.
The original development storage-sink independently exited with Kafka
`UNKNOWN_MEMBER_ID` and restarted under its existing policy (exit code 1); its
start time therefore changed. No command stopped/recreated that container or reset
its offsets, and no original checkpoint, retained volume or database migration was
performed. Its automatic restart must not be described as an unchanged runtime.

Production remains blocked by seven critical and 208 high package occurrences,
unverified dynamic reachability for the remaining Avro/Codehaus paths, outstanding
vendor patches, amd64 validation, and capacity/soak testing. The ten legacy
integration cases targeting development endpoints remain disabled to preserve that
environment; isolated secured tests are not a claim that every legacy scenario was
repeated. Existing Phase 3B release blockers still apply. No frontend dependency
changed, so frontend checks were not repeated for this image-only change.

### Final Phase 3C acceptance

**CONDITIONAL PASS for the verified improvements; not approved for production.**
Fresh scans reduce totals from **16 critical / 407 high** to **7 critical / 208 high**.
All relevant final assertions passed without disabling TLS, ACLs, RPC encryption,
non-root execution, isolation, acknowledgement checks or replay-safety assertions.

Across final focused runs, **91 distinct repository tests passed, zero failed,
and 10 legacy integration tests were skipped** out of 101 collected tests:

| Final scope | Passed | Failed | Skipped |
| --- | ---: | ---: | ---: |
| Python non-integration and isolated browser | 77 | 0 | 0 |
| Hardened infrastructure and worker permissions | 9 | 0 | 0 |
| Secured Kafka/Spark | 2 | 0 | 0 |
| Authenticated monitoring | 1 | 0 | 0 |
| Hardened crash/replay and durable processing | 2 | 0 | 0 |
| Legacy development-endpoint integration | 0 | 0 | 10 |

The non-integration final rerun was 76 passed / 1 skipped / 24 deselected; the
skipped browser case then passed with `AUTH_BROWSER_INTEGRATION=1`. A prior
complete non-integration run also passed 77 tests. The separate actual Spark
feature-contract suite passed **5/5** and is not included in the 101 root tests.
Compose validation and Python compilation passed. Frontend checks are not
applicable to the unchanged frontend dependencies.

The final recovery run passed both tests in 157.27 seconds. It verifies a missing
checkpoint commit after a real process exit, replay with the same query identity,
continued windows, all 18 contract-2.0 features and exact event lineage. Six
feature deliveries representing four logical windows yield exactly four durable
predictions and four alerts in both PostgreSQL and Elasticsearch. A separate
consumer process test verifies no acknowledgement on a terminated database
connection, process death after durable SQL but before acknowledgement, successful
redelivery after normal group reassignment, and one row after duplicate replay.
These permanent tests protect the critical compatibility and data-integrity
boundaries affected by runtime upgrades; temporary migration/setup probes and
execution evidence remain untracked.

## Phase 3D residual critical investigation (18 October 2026)

**CONDITIONAL PASS for investigation; production remains blocked.** No runtime,
JAR, image pin, dependency, transport or authorization configuration changed.
The clean `dev` baseline was `d0e6ce038b44b31bbb58ceb4f90de5926362bc42`, tracking
`origin/dev`. Reinspection and benign probes use the exact Phase 3C arm64 images,
not the older `soc-hardened` containers remaining in the development Docker context.
Spark source image/index is `sha256:fe397004c6115f17e583821c622fe36903e45916317d3d292cd1520b045d2f86`;
Kafka is `sha256:490ea76a88fb440cf834b7f8a3a7c00758a19f6427b4f7fc3321ed4cae946951`.
Their transferred platform manifests and upstream input pins remain recorded above.

### Occurrence inventory and classifications

These conclusions apply only to the repository application and hardened Compose
configuration. They exclude arbitrary submitted jobs, administrator-controlled
plugins/configuration and untrusted model/checkpoint replacement. Such access is
already a powerful execution capability, not an event-producer entitlement.
Internal networking alone is not evidence of non-reachability. No scanner findings
are suppressed or reclassified in the scanner database.

| Occurrence | Exact runtime package path and version | Classification | Evidence and invalidating changes |
| --- | --- | --- | --- |
| Netty CVE-2026-75595, Spark | `/opt/spark/jars/netty-handler-4.1.96.Final.jar`; `io.netty:netty-handler` 4.1.96.Final | **Mitigated** under the defined transport | Authenticated encrypted Spark RPC uses `AuthServerBootstrap`, not per-SNI TLS selection. No external SNI call site was found in the image class-reference search. Reassess custom channel handlers, TLS termination, additional services and untrusted job submission. |
| Netty CVE-2026-75595, Kafka | `/usr/share/java/kafka/netty-handler-4.1.136.Final.jar`; `io.netty:netty-handler` 4.1.136.Final | **Mitigated** under the defined transport | Actual broker classes use Kafka `SslChannelBuilder`/`SslTransportLayer` and JSSE, with listener-wide `ssl.client.auth=required`. Bytecode calls `SSLEngine.setNeedClientAuth`. A fresh isolated broker rejects a client without a certificate. Reassess Netty-based proxies/plugins and permissive fallback contexts. |
| Avro CVE-2024-47561, first shaded copy | `/opt/spark/jars/hadoop-client-runtime-3.3.4.jar`; embedded `org.apache.avro:avro` 1.7.7 | **Unverified** | Shaded Specific/Reflect readers exist; Hadoop API classes reference them. No event-to-schema path was demonstrated, but indirect serialization paths remain incompletely traced. |
| Avro CVE-2024-47561, second shaded copy | `/opt/spark/jars/org.apache.hadoop_hadoop-client-runtime-3.3.4.jar`; embedded `org.apache.avro:avro` 1.7.7 | **Unverified** | Byte-identical to the first copy (SHA-256 `9377a68071137ae5f5c8cdc2ed20d6f904a1df4d06df26b47ed8872a1b0d8d47`); same classes and limitations. This is a second scanner occurrence, not a separately proven exploit path. |
| Derby CVE-2022-46337 | `/opt/spark/jars/derby-10.14.2.0.jar`; `org.apache.derby:derby` 10.14.2.0 | **Not applicable** to the defined pipeline | No Derby JDBC connection, LDAP authentication provider or Derby service is configured; Spark uses the in-memory catalog. No Derby classes loaded in the exercised JSON/state/checkpoint flow. Enabling Hive, Derby JDBC or LDAP authentication invalidates this conclusion. |
| ZooKeeper CVE-2023-44981 | `/opt/spark/jars/zookeeper-3.6.3.jar`; `org.apache.zookeeper:zookeeper` 3.6.3 | **Not applicable** to the defined pipeline | KRaft broker/controller and standalone Spark without ZooKeeper recovery; no quorum service or SASL quorum configuration. No ZooKeeper classes loaded in the exercised pipeline. Enabling ZooKeeper recovery, an embedded server or another quorum requires reassessment. |
| Codehaus Jackson CVE-2019-10202 | `/opt/spark/jars/jackson-mapper-asl-1.9.13.jar`; `org.codehaus.jackson:jackson-mapper-asl` 1.9.13 | **Unverified** | Unsafe polymorphic binding has not been demonstrated in the application, but complete dependency-level data flow has not been proved absent. Shaded Codehaus mapper/type-resolver classes also exist in both Hadoop runtime copies, beyond this standalone scanner match. |

### Exploit requirements, source evidence and fix decisions

**Netty:** [upstream advisory GHSA-c4c3-7fpv-j4q5](https://github.com/netty/netty/security/advisories/GHSA-c4c3-7fpv-j4q5)
identifies malformed fragmented ClientHello handling in `SslClientHelloHandler.decode`.
Authentication bypass requires per-SNI client authentication, a permissive default
context and no secondary certificate check. A network peer need not authenticate
if those prerequisites exist. The affected ranges include both installed versions;
fixes are 4.1.137.Final and 4.2.17.Final. Spark's inspected network initialization
and the broker's actual JSSE implementation do not provide that fallback route.
The vendor calls this High while Trivy reports Critical; neither label establishes
reachability. The mitigation is the verified transport design, not a patch.

The [Confluent 7.6 changelog](https://docs.confluent.io/platform/7.6/release-notes/changelog.html)
mentions Netty 4.1.137 updates under Common/ksqlDB. This does not establish that the
broker image contains them: the registry still resolves 7.6.14 to the pinned
`b20da2c0...` index and the tested broker contains 4.1.136.Final. Do not claim the
broker is fixed from component release notes alone. Spark's current recommended
[3.5 maintenance release](https://spark.apache.org/releases/spark-release-3-5-9.html)
is already in use and still bundles 4.1.96.Final. No independently replaced Netty
JAR or untested cross-major migration was adopted. Obtain a vendor bundle and
repeat secure transport/recovery verification before changing these inputs.

**Avro:** the [Apache advisory](https://lists.apache.org/thread/c2v7mhqnmq0jmbwxqq3r5jbj1xg43h5x)
covers Java versions before 1.11.4; 1.11.4/1.12.0 contain the fix. The
[upstream correction](https://github.com/apache/avro/pull/2934) adds trusted-package
checks around schema-directed class resolution. The old
[SpecificDatumReader](https://raw.githubusercontent.com/apache/avro/release-1.7.7/lang/java/avro/src/main/java/org/apache/avro/specific/SpecificDatumReader.java)
resolves `java-class`/`java-key-class` schema properties through `getPropAsClass`.
An attacker must influence a schema used by an affected reader and reach a usable
classpath class/constructor; parsing an ordinary event string is not sufficient.
Do not invoke dangerous classes to test this prerequisite.

The normal unshaded Avro is **1.11.5**, separately loaded from `avro-1.11.5.jar`;
that does not fix relocated `org.apache.hadoop.shaded.org.apache.avro` classes.
The Hadoop API contains references from `AvroSpecificSerialization`,
`AvroReflectSerialization` and MapReduce `EventReader` to those shaded readers.
The live Hadoop `io.serializations` default includes both Avro serializers;
therefore JSON ingestion alone cannot justify a Not applicable classification.
The application uses fixed StructTypes, Kryo, local checkpoints and `file:///`,
with no schema registry, Avro input or MapReduce job-history reader configured.
Neither shaded Avro nor Codehaus loaded before explicit class-origin introspection
in the benign probe. This bounds the observation; it is not an exhaustive call graph.

Restrict schemas, filesystem configuration, jobs, plugins and checkpoint ownership
to trusted administrators. New Avro/file formats, HDFS/MapReduce integrations,
serializer overrides or schema-bearing event handling invalidate this boundary.
The package allowlist added by newer Avro is not assumed to work in shaded 1.7.7.
A supported Spark/Hadoop bundle removing the old shaded implementation, or a
complete bounded data-flow review, remains necessary. Replacing unshaded Avro or
removing one duplicate Hadoop JAR is not a defensible remediation for both copies.

**Derby:** [DERBY-7147](https://issues.apache.org/jira/browse/DERBY-7147)
locates LDAP filter injection in `LDAPAuthenticationSchemeImpl.getDNFromUID`.
An attacker must supply a login identifier to an application using Derby LDAP
authentication. This can include embedded Derby: the earlier Phase 3C wording
about a network server was too narrow. Here no Derby/LDAP authentication service
is activated, and the observed catalog is `in-memory` with Derby authentication
system properties unset. Absence of a network port alone is not the conclusion.

Scanner-listed 10.14.2.1, 10.15.2.1 and 10.16.1.2 POM URLs returned HTTP 404 from
Maven Central; 10.17.1.0 returned 200. [DERBY-7178](https://issues.apache.org/jira/browse/DERBY-7178)
describes the unreleased 10.14 branch fix/version ambiguity. The
[official downloads page](https://db.apache.org/derby/derby_downloads.html)
requires Java 21 for published 10.17.1.0 and confirms Derby retired in October
2025. Java 17 is retained. A scanner fixed-version field is not proof of an
available, maintained, compatible release; no ad hoc Derby build or JAR swap was
made. Upstream bundle retirement/remediation must precede enabling this feature.

**ZooKeeper:** the [Apache advisory](https://zookeeper.apache.org/security/#cve-2023-44981)
affects versions before 3.7.2, 3.8.0–3.8.2 and 3.9.0. A peer capable of SASL
authentication with a principal lacking an instance component can evade quorum
host authorization when `quorum.auth.enableSasl=true` and access the quorum port.
Fixed versions are 3.7.2, 3.8.3 and 3.9.1. Mere client-library use does not create
that server-side quorum path. Spark can activate Curator/ZooKeeper through
ZooKeeper recovery; Hadoop's shaded Curator also references quorum configuration
classes. Neither activation is configured here. Kafka has `process.roles` set
and an empty `zookeeper.connect`. Loading its `QuorumPeerConfig$ConfigException`
for shared configuration validation was observed; this does not start a quorum.
Do not extend this classification to the preserved ZooKeeper-based development
cluster. A future quorum must use a fixed supported
bundle; no standalone ZooKeeper JAR was substituted into Spark.

**Codehaus:** [Red Hat's original report](https://bugzilla.redhat.com/show_bug.cgi?id=CVE-2019-10202)
describes unsafe deserialization in Codehaus 1.9.x and product-specific allowlist
backports. Those EAP/Fuse errata are not a supported replacement for Spark's
unpatched Maven artifact. Trivy lists no fixed Maven version for 1.9.13.
Relevant paths are `ObjectMapper` type resolution and `ClassNameIdResolver` with
polymorphic types/default typing or type annotations, attacker-controlled type IDs
and suitable classpath types. A mapper's presence is not proof those options are on.

Repository source does not configure Codehaus ObjectMapper/default typing or
Java-type annotations. Event JSON is parsed through Spark `from_json` with fixed
StructTypes, not application-selected Java classes. The image class-reference
search found typing references inside mapper libraries, not a separate enabling
application call site; reflection, annotations and indirect initialization mean
this is not proof of absence. Benign `@class` and `java-class` event fields were
ignored by the real parser. The shaded Hadoop copies retain mapper and type-ID
resolver classes; a complete shaded call graph remains outstanding. Keep the
classification Unverified for the standalone occurrence and investigate the
additional shaded surface rather than relying on the scanner's seven-match count.

Model artifacts are Python `joblib`/pickle objects in a separate Python worker;
there is no model-to-JVM Codehaus binding in the inspected application. They must
still be trusted: contract metadata validation and a computed artifact hash do
not authenticate a pickle or make arbitrary model replacement safe. Configuration
is administrator-controlled JSON/properties, not an event-provided mapper policy.
Do not accept untrusted models, jobs, Java serialization, configs or checkpoints
as a workaround. A supported upstream removal/migration and deeper dependency
review remain blockers; no vendor-specific fork was installed.

### Targeted high-severity triage

This is a ranked investigation queue, not a claim of confirmed exploitation or an
assessment of every one of the 208 high occurrences. All listed libraries are
present in runtime images; none was dismissed as build-only merely because a
build tool also uses it.

| Priority | Findings / input boundary | Evidence, current control and next action |
| --- | --- | --- |
| 1 | Grafana kin-openapi v0.147.0: CVE-2026-100352/100353/100354; HTTP request/schema validation | 100353 was introduced by Phase 3C. Review actual registered OpenAPI 3.1 decoder paths and plugin routes first; authentication/internal access reduces callers but does not patch parsing. Fixes listed are 0.148.0/0.149.0. Obtain a supported Grafana bundle. Exact application reachability remains Unverified. |
| 2 | Spark shaded Jackson core 2.12.7/2.13.4: CVE-2025-52999 and other parser/type-binding matches | Event parsing is application-reachable; the old parser implementations are in shaded Hadoop/Parquet dependencies, not necessarily that event parser. Trace the selected parser and all schema/config inputs. Do not equate the reachable `from_json` API with a confirmed old-parser exploit. |
| 3 | Prometheus Go v1.26.8: CVE-2026-78667/78669; HTTP Range/HTTP2 | Administrative HTTP APIs are enabled and protected by Basic authentication; outgoing exporter connections also exist. Inspect actual FileServer/ServeContent handlers and HTTP2 negotiation before declaring either path absent. Go fixes are 1.26.9/1.27.2; x/net fix is 0.60.0. Current application reachability is Unverified. |
| 4 | Spark util-linux: CVE-2026-76642/78408/78409/78410 in `login` and `liblastlog2-2` (eight newly introduced occurrences) | `/usr/bin/mount` and `nsenter` actually exist. UID 185, zero effective capabilities, no-new-privileges, no host namespaces or host-system mounts constrain privilege prerequisites. Debian has no trixie fix listed; do not import unstable packages. These controls mitigate privilege escalation prerequisites, not the package defects. |
| 5 | JLine telnet/grep in Spark/Kafka; Grafana's Tempo CVE-2026-21728/28377; Prometheus xDS/ECH-specific matches | No telnet service, interactive grep API, Tempo server/S3 SSE-C, xDS gRPC server or ECH TLS listener is configured. Lower priority for this topology; reassess service/plugin/transport changes. Binary inclusion alone does not instantiate those services. |

Sources: [kin-openapi 100353 upstream advisory](https://github.com/getkin/kin-openapi/security/advisories/GHSA-vrcc-3ch7-hppw),
[Go Range advisory](https://pkg.go.dev/vuln/GO-2026-6609),
[Debian util-linux status](https://security-tracker.debian.org/tracker/source-package/util-linux),
and [util-linux mount prerequisite analysis](https://github.com/util-linux/util-linux/security/advisories/GHSA-m25x-3hj9-m26f).
The kin-openapi upstream header describes versions through v0.144.0, whereas the
scanner reports v0.147.0 affected with a v0.149.0 fix. That discrepancy is not a
basis for suppression; reconcile the exact decoder implementation and fix commit.

### Verification scope and release implications

Fresh Trivy 0.76.0 scans of the unchanged exact images reproduce Spark **6 critical /
145 high**, Kafka **1 / 48**, Prometheus **0 / 10** and Grafana **0 / 5**: **7 / 208**
in total. No critical occurrence was patched or removed in this investigation.
Generated reports and probes remain ignored under `.codex-logs/phase3d/`.

- Focused repository tests: `tests/hardening/test_configuration.py` and
  `tests/reliability/test_delivery.py`: **12 passed, 0 failed, 0 skipped**.
  Hardened Compose validation with every profile also passed.
- One final benign Spark probe passed real application JSON parsing, fixed-schema
  treatment of type-like fields, local stateful streaming, and resume against its
  own new checkpoint. It used Kryo, UID 185, read-only root, zero capabilities,
  no-new-privileges, authenticated/encrypted RPC configuration and no external
  network. JVM class-load logging was separated from deliberate introspection;
  class loading caused by inspection is not reported as application reachability.
- One final disposable Kafka probe passed authenticated production/consumption,
  unauthorized administrative ACL rejection and missing-client-certificate TLS
  rejection. Its broker loaded JSSE transport classes and retained the Compose
  KRaft/mTLS/default-deny authorization settings. Only fresh credentials and
  container-local temporary storage were used; no host ports were published.
- Preliminary probes needed a missing Kafka-envelope key and client-properties
  mount-path correction. A denied topic describe was intentionally concealed as
  non-existent by Kafka, so the final probe used an explicit denied ACL-list
  operation; this was an assertion correction, not a broadened permission.
- No runtime change required rebuild or another full ML/database/search/browser
  suite. Phase 3C evidence above remains historical; the local Spark probe is not
  a new distributed recovery/crash test or new full-stack compatibility claim.
- All Docker contexts/Colima VMs available here are aarch64. Upstream Spark/Kafka
  indexes advertise amd64 manifests, but there is no native amd64 test environment.
  AMD64 remains **Unverified**; no expensive emulation was attempted. The stopped
  Phase 3C VM was not started. No command targeted historical containers or retained
  data, checkpoints, topics or offsets.

Production remains blocked by incomplete Avro/Codehaus reachability analysis,
residual critical/high packages, retired dependencies, suitable vendor bundles,
AMD64 runtime verification and the previous capacity/operational release gates.
A deployment/configuration change requires renewed assessment of these bounded
classifications. No approval to release, migrate retained data or promote `main`
is implied by this investigation.
