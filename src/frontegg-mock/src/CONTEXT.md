# frontegg-mock::src

See [`../CONTEXT.md`](../CONTEXT.md) for crate-level overview.

## Module surface (LOC ≈ 2,711)

| Module | LOC | Purpose |
|---|---|---|
| `handlers/user.rs` | 533 | User CRUD, API token management, role assignment |
| `handlers/sso.rs` | 381 | SSO configuration CRUD + domain/group/role sub-resources |
| `server.rs` | 325 | `FronteggMockServer`, `Context`, axum `Router` wiring, all route constants |
| `models/sso.rs` | 221 | SSO request/response and storage types |
| `models/user.rs` | 155 | User request/response and storage types |
| `handlers/group.rs` | 155 | Group CRUD + role/user membership |
| `main.rs` | 142 | CLI binary — parses args and calls `FronteggMockServer::start` |
| `handlers/auth.rs` | 139 | Login (user + API-token) and token refresh handlers |
| `models/group.rs` | 116 | Group request/response and storage types |
| `utils.rs` | 97 | `encode_jwt`, `decode_jwt`, `RefreshTokenTarget` |
| `models/token.rs` | 93 | API-token and tenant-token types |
| `handlers/scim.rs` | 93 | SCIM 2.0 configuration handlers |
| `models/utils.rs` | 51 | Shared model helpers |
| `models/scim.rs` | 46 | SCIM request/response types |
| `middleware/logging.rs` | 34 | Request logging middleware |
| `middleware/role_update.rs` | 29 | In-band role-update channel middleware |
| `middleware/latency.rs` | 27 | Artificial latency injection |
| `models.rs` | 22 | Module re-exports |
| `handlers.rs` | 20 | Module re-exports |

## Key interfaces

- **`FronteggMockServer::start`** — single public entry point; returns the
  handle with test-control fields (`enable_auth`, `role_updates_tx`,
  `refreshes`, `auth_requests`).
- **`Context`** — shared in-memory state behind `Arc<Mutex>`; mutated by all
  handler paths.

## Cross-references

- `mz-frontegg-auth` — JWT validation logic reused from production crate
- `axum` — HTTP routing substrate
- `jsonwebtoken` — JWT encode/decode
- Generated docs: `doc/developer/generated/frontegg-mock/`
