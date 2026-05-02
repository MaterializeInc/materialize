---
source: src/frontegg-mock/src/handlers/auth.rs
revision: e757b4d11b
---

# frontegg-mock::handlers::auth

Implements the three core authentication handlers: `handle_post_auth_api_token` (exchanges a client_id/secret pair for a JWT and refresh token, supporting both user and tenant API tokens), `handle_post_auth_user` (email/password login), and `handle_post_token_refresh` (consumes a one-time refresh token and re-issues credentials).
All handlers increment the `auth_requests` or `refreshes` counters and respect the `enable_auth` flag, returning `401 Unauthorized` when auth is disabled.
