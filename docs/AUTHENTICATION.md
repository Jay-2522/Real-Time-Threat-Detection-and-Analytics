# Authentication and authorization

The FastAPI backend uses PostgreSQL-backed users, opaque server-side sessions,
Argon2id password hashes, CSRF tokens for writes, and role checks. Roles are
`viewer`, `analyst`, and `administrator`.

| Capability | Viewer | Analyst | Administrator |
| --- | --- | --- | --- |
| Read dashboards, alerts, incidents, and threat data | yes | yes | yes |
| Change alert or incident workflow state | no | yes | yes |
| Create, disable, or change users | no | no | yes |
| Read the authentication audit API | no | no | yes |

Unknown roles, routes, and methods are denied. The initial administrator cannot
remove or demote itself. The current account-management surface is an API, not a UI.

## Local setup

Schema migrations are applied by the one-shot `schema-bootstrap` Compose service.
After a fresh start, provision the first administrator interactively:

```bash
docker compose exec backend python -m app.auth.provision
```

The password prompt is hidden, accepts no command-line password, and enforces the
policy in `backend/app/auth/service.py`. The one-time bootstrap is transactional and
cannot be reopened by deleting accounts. No real account or password is committed.

Local Compose sets `AUTH_LOCAL_HTTP=true` only for loopback development. Use
`http://localhost:5173` and `http://localhost:8000` consistently. Non-loopback and
hardened deployments require secure cookies and HTTPS.

## Sessions and request protection

- Session identifiers are random, stored only as hashes, rotated at login, and sent
  in HttpOnly cookies.
- State-changing requests require the allowed Origin and the session's CSRF token.
- Login admission is stored in PostgreSQL, so a backend restart does not clear it.
- Prometheus uses a separate bearer credential mounted only into backend and
  Prometheus; it cannot read application data.
- Authentication activity is written to the database audit table. This is not an
  external tamper-resistant audit store.

The browser authentication test covers login, dashboard access, analyst changes,
viewer denial, logout, and session expiry. OIDC, MFA, password recovery, a password
change UI, and external audit retention are not implemented.
