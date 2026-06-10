---
source: src/frontegg-mock/src/handlers/auth.rs
revision: 33b8db85da
---

# frontegg-mock::handlers::auth

Implements the three core authentication handlers: `handle_post_auth_api_token` (exchanges a client_id/secret pair for a JWT and refresh token, supporting both user and tenant API tokens), `handle_post_auth_user` (email/password login), and `handle_post_token_refresh` (consumes a one-time refresh token and re-issues credentials).
All handlers increment the `auth_requests` or `refreshes` counters and respect the `enable_auth` flag, returning `401 Unauthorized` when auth is disabled.
For user tokens (`UserApiToken` and `UserToken`), the handlers call `get_user_groups` to resolve the authenticated user's group memberships and pass them to `generate_access_token`, embedding them under the `groups` JWT claim. Tenant API tokens always receive an empty groups list.
