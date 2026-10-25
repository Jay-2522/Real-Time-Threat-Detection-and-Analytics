# Authentication and authorization

## Permission matrix (established before implementation)

| Surface | Public | Viewer | Analyst | Administrator | Service credential |
| --- | --- | --- | --- | --- | --- |
| GET /api/v1/health | Minimal liveness | Yes | Yes | Yes | Not needed |
| POST /api/v1/auth/login | Origin/custom-header protected | Yes | Yes | Yes | No |
| GET /api/v1/auth/me; POST logout | No | Own session | Own session | Own session | No |
| Analytics, events, predictions, risks, search | No | Read | Read | Read | No |
| Dashboard, alerts, incidents, monitoring | No | Read | Read/update alert or incident status | Same | No |
| Threat intel, detections, entities, MITRE | No | Read | Read | Read | No |
| All three /ws/* streams | No | Read | Read | Read | No |
| /docs, /redoc, OpenAPI | No | Read | Read | Read | No |
| /auth/users; /auth/audit | No | No | No | Manage/read | No |
| GET /metrics | No | No | No | No | Metrics-only bearer credential |

There is no configured OIDC provider. Local accounts and revocable server-side
sessions avoid introducing an identity service for this single-organization pilot.
No public registration, default accounts, or development authentication bypass exists.

## Architecture and boundaries

Passwords use `argon2-cffi`'s Argon2id defaults. Login creates a fresh 256-bit random
identifier, never adopts a supplied session, and stores only its SHA-256 digest in
PostgreSQL. The browser receives an HttpOnly, host-only, SameSite=Strict cookie.
Production defaults additionally use Secure and the `__Host-` prefix. Session and
CSRF values never enter localStorage, URLs, audit records, or application logs.
CSRF uses standard HMAC-SHA256 bound to the session; `/auth/me` returns it for the
frontend's memory-only request header. Writes require both the token and an exact
allowed Origin. Login requires an allowed Origin and `X-Requested-With: SOC`.

Sessions expire after eight hours, with a 30-minute idle limit (`SESSION_SECONDS`,
`SESSION_IDLE_SECONDS`). Authenticated HTTP activity refreshes idle time; background
HTTP polling counts as activity. WebSocket sends do not refresh it. Logout revokes
the server record; role/activation changes revoke every session for that user.
Writes recheck authorization inside the transaction, serialized with account
changes. Self role/activation changes are forbidden, and the final active
administrator cannot be removed. Unknown roles/routes/methods are denied.

All backend routes were previously anonymous, including documentation, telemetry,
analytics, predictions, risk/search, dashboard, alerts/incidents, monitoring,
threat intelligence, entity intelligence, detection rules, MITRE, and all three
WebSockets. `app/auth/policy.py` is the explicit REST inventory. The regression test
also enumerates FastAPI routes so a new unclassified endpoint fails verification.
Only minimal health and the protected login exchange are public. Unsupported
OAuth documentation redirects are disabled.

Dashboard, alerts, and threat-intelligence WebSockets authenticate before accept,
require an exact Origin, and recheck expiry, activation, role validity and revocation
before each send (currently every three seconds). Invalid connections are rejected;
active invalid sessions close with code 4401. No URL credential is accepted.
Frontend socket failures revalidate the session before reconnecting. API 401 clears
the authenticated UI; 403 shows a permission error. Existing data helpers rethrow
authentication/authorization failures rather than returning synthetic data.

Guidance: [OWASP session management](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html),
[OWASP CSRF prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html),
and [argon2-cffi defaults](https://argon2-cffi.readthedocs.io/en/stable/howto.html).

## Migration, local setup and initial administrator

Use the existing `dev` checkout and configured local Docker context. Preserve all
existing database volumes and Spark state. This phase does not require starting
Spark or recreating its worker.

1. Run `python3 scripts/setup_local.py`. It preserves an existing `.env` and creates
   an independent random metrics credential in ignored `.secrets/` if absent. The
   directory is owner-only (0700); the file is readable within its specific read-only
   container mount so Prometheus's unprivileged process can read it. Never commit it.
2. Apply the additive, transactional migration **before restarting the backend**:

   ```sh
   docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < migrations/003_authentication.sql
   ```

   It creates only authentication tables/indexes and the singleton bootstrap guard.
   Repeat application preserves users and bootstrap state. Existing telemetry,
   predictions, alerts and incidents are neither changed nor deleted. Backend startup
   fails if the migration is absent. Fresh installations need this migration too.
3. Build and start only the affected services:

   ```sh
   docker compose build backend
   docker compose up -d --no-deps backend frontend prometheus
   ```

   If `DEV_BIND_ROOT` uses a separate source snapshot, synchronize the changed
   `backend/app`, `frontend/src`, and Prometheus configuration there first. Do not
   replace checkpoint mounts, model artifacts, worker containers, or database volumes.
4. An authorized operator provisions the initial account interactively:

   ```sh
   docker compose exec backend python -m app.auth.provision
   ```

   Supply a unique username and a private password of 15–128 characters with at least
   eight different characters. Password entry is hidden and is never accepted as a
   command-line argument. Provisioning is transactional, audited, and locked against
   concurrent initial administrators. It cannot run again after bootstrap, even if
   accounts are later removed outside the application. No real account is created
   automatically or by this implementation's tests.
5. Open `http://localhost:5173`, sign in, and use Sign out when finished. Compose's
   `AUTH_LOCAL_HTTP=true` exception is restricted to development loopback origins
   and loopback request hosts. It uses a separately named HttpOnly cookie; it never
   bypasses authentication. Use the same hostname for frontend and API.

Administrators can use authenticated `GET/POST /api/v1/auth/users`,
`PATCH /api/v1/auth/users/{id}` (role and active fields), and `GET /api/v1/auth/audit`.
User creation takes username, password and role; there is no public registration.
An approved client must retain the session cookie and send the Origin and
`X-CSRF-Token` from `/auth/me` for writes. Do not put passwords/tokens in shell
history or URL parameters. Account management currently has an API, not a GUI.
Viewers see read-only status. Analysts/administrators see status selectors on alert
and incident records; a successful API response is required before showing Saved.

## Monitoring and operational controls

Prometheus uses a dedicated bearer credential at `/run/secrets/backend_metrics_token`
for backend `/metrics`; human sessions cannot scrape it and that credential cannot
read application data. Missing configuration fails closed. Compose mounts the secret
only into backend and Prometheus. To rotate, replace the private file with a fresh
cryptographically random value and recreate those two services with `--no-deps`.

Login admission uses PostgreSQL rows, not just process memory: ten attempts per
normalized account and fifty per client address in a fixed 15-minute window,
including failed attempts, across workers. Unknown users receive the same credential
error and perform dummy Argon2 verification. A process restart cannot reset the
shared counters. Use narrowly trusted proxy addresses; never trust arbitrary
forwarded client headers. The older generic in-memory limiter is only supplemental.

Audit records include timestamp, actor identifier, target identifier, action and
outcome. They cover provisioning, logins, logout, user changes/session revocation,
status changes, and denied modifications. Status/audit writes share one transaction:
audit failure rolls back the status change. No application endpoint can edit/delete
audit records. Database administrators remain trusted; this is not an immutable
external audit archive.

Production must use HTTPS for both frontend and backend on the same site, exact HTTPS
CORS origins, `ENVIRONMENT=production`, and `AUTH_LOCAL_HTTP=false`. Keep Secure
cookie defaults. TLS termination must forward scheme only through an explicitly
trusted proxy. The supplied Compose setup is local development, not a production
release. Do not publish it as one.

## Verification

Install the root test dependencies, run `npm ci` in `frontend`, and install the
isolated browser with `.venv/bin/python -m playwright install chromium`. From the
repository root, with the existing safe Docker context configured:

```sh
AUTH_BROWSER_INTEGRATION=1 PIPELINE_INTEGRATION=1 SPARK_RECOVERY_INTEGRATION=1 \
SPARK_CONTRACT_OUTPUT="$PWD/.codex-logs/spark-contract.json" \
.venv/bin/python -m pytest -ra
(cd frontend && npm run lint && npm run build)
```

Generate the Spark contract evidence first using the existing Phase 1 Spark test
command documented in the repository. Phase 2B integration creates isolated new
checkpoints/topics and never operates on the damaged original checkpoint.

The focused `backend/tests/test_authentication.py` suite verifies authentication,
expiry/idle/revocation/disabled users, fixation, logout, cookies, all anonymous route
denials, RBAC/escalation, CSRF, CORS, all three WebSockets (including active
revocation), shared throttling, audit atomicity and concurrent bootstrap safeguards.
`test_auth_migration.py` runs the SQL migration twice in a disposable PostgreSQL
schema containing copies of existing historical records and checks preservation,
uniqueness, valid-role and foreign-key constraints. `test_auth_browser.py` starts an
isolated API/database and Vite on port 5174, with random test-only passwords, and
uses actual Chromium: login → dashboard → read alert → analyst incident update →
logout → access denied, then viewer denial and active session expiry. No real user
or production data is provisioned or modified by these security tests.

## Remaining limits

This phase does not establish production readiness. OIDC/MFA, password recovery and
password-change UI, external tamper-resistant audit retention, scheduled cleanup of
expired sessions/throttle rows, and edge-level volumetric abuse controls remain
future work. Fixed-window throttling can temporarily deny a legitimate account
under attack. Database security, TLS deployment, network isolation, and protection
of directly exposed infrastructure (including Elasticsearch, Kafka,
Prometheus/Grafana and other services' metric ports) remain separate infrastructure
work. Backend authentication does not secure direct access to those services.
Existing non-authentication synthetic dashboard fallbacks remain outside this phase;
401/403 never use them. No ML contract or Kafka/Spark processing code is changed.
