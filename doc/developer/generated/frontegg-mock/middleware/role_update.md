---
source: src/frontegg-mock/src/middleware/role_update.rs
revision: 8041e666f1
---

# frontegg-mock::middleware::role_update

Defines `role_update_middleware`, an axum middleware layer that drains the `role_updates_rx` channel before each request, applying any pending `(email, roles)` updates to the in-memory user store.
This allows tests to push role changes via `FronteggMockServer.role_updates_tx` and have them take effect on the next request.
