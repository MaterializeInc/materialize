# frontegg-mock (mz-frontegg-mock)

In-process HTTP server that simulates the
[Frontegg](https://frontegg.com/) identity platform API for use in
Materialize integration tests. Covers auth, user management, groups, SSO,
SCIM 2.0, and tenant API tokens. Also ships a standalone CLI binary.

## Module surface (LOC ≈ 2,711 in `src/`)

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

Tests: ~2,570 LOC in `tests/`.

## Package identity

Crate name: `mz-frontegg-mock`. Binary: `mz-frontegg-mock` (CLI entry point
in `src/main.rs`). Primary consumers: integration test suites in
`mz-pgwire`, `mz-adapter`, and environment-level tests needing a live
Frontegg endpoint.

## Key interfaces (exported)

- **`FronteggMockServer::start`** — async factory; binds a `TcpListener` and
  stands up an axum `Router` covering all Frontegg API paths. Returns a
  handle with `base_url`, `refreshes` counter, `enable_auth` toggle,
  `auth_requests` counter, and `role_updates_tx` channel.
- **`Context`** (in `server`) — in-memory state shared across handlers:
  users, groups, tokens, SSO configs, SCIM config; guarded by `Arc<Mutex>`.
- **`handlers`** — axum handler functions by resource:
  `auth` (login, refresh), `user` (CRUD + API tokens), `group`,
  `scim`, `sso`.
- **`middleware`** — logging, artificial latency injection, in-band role
  updates via channel.
- **`models`** — request/response and storage types for each resource.
- **`utils::{encode_jwt, decode_jwt, RefreshTokenTarget}`** — JWT helpers.

## Dependencies

`axum`, `jsonwebtoken`, `mz-frontegg-auth`, `mz-ore`, `tokio`.

## Bubbled findings for src/CONTEXT.md

- **Test-only artifact in production position**: the library is essentially a
  test double, but it ships as a named binary and is pulled into
  `mz-environmentd` tests. The Adapter Seam between mock and real Frontegg is
  just URL substitution — no trait abstraction — so divergence is a latent
  risk.
- **`enable_auth` toggle and `role_updates_tx` channel** are
  test-control knobs embedded directly in the server struct; clean for
  testing but signal that the public API is test-driven, not
  production-hardened.
- **`src/` is only 2,711 LOC** (the stated 5,281 includes tests); the
  library is small and well-scoped.
