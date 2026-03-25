---
source: src/frontegg-mock/src/lib.rs
revision: 8041e666f1
---

# frontegg-mock

`mz-frontegg-mock` is an in-process HTTP server that simulates the Frontegg identity platform API, used in integration tests for Materialize's authentication and authorization logic.

`FronteggMockServer::start` binds a `TcpListener` and stands up an axum `Router` covering auth (user/API-token login and token refresh), user management, group management, SSO configuration, SCIM 2.0 provisioning, and tenant API tokens.

Module structure:

* `server` — `FronteggMockServer` and the shared `Context` (in-memory state + JWT keys).
* `handlers` — axum handler functions organized by resource (`auth`, `user`, `group`, `scim`, `sso`).
* `middleware` — axum middleware for request logging, artificial latency, and in-band role updates.
* `models` — all request/response and storage types (`token`, `user`, `group`, `scim`, `sso`, `utils`).
* `utils` — JWT encode/decode helpers and `RefreshTokenTarget`.
* `main` — CLI binary entry point.

Key dependencies: `axum`, `jsonwebtoken`, `mz-frontegg-auth`, `mz-ore`, `tokio`.
Primary consumers: integration test suites in `mz-pgwire`, `mz-adapter`, and related crates that need a live Frontegg API endpoint.
