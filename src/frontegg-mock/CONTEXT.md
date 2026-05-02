# frontegg-mock (mz-frontegg-mock)

In-process HTTP server that simulates the
[Frontegg](https://frontegg.com/) identity platform API for use in
Materialize integration tests. Covers auth, user management, groups, SSO,
SCIM 2.0, and tenant API tokens. Also ships a standalone CLI binary.

## Subtree (≈ 5,281 LOC total)

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 2,711 | Library modules |
| `tests/` | ~2,570 | Integration test suite |

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
