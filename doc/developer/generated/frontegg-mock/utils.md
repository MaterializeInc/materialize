---
source: src/frontegg-mock/src/utils.rs
revision: 8041e666f1
---

# frontegg-mock::utils

Provides JWT utility functions shared across handlers: `decode_access_token` (validates a Bearer token with RS256), `generate_access_token` (encodes a `Claims` JWT with roles and derived permissions), `generate_refresh_token` (creates a UUID refresh token stored in `Context.refresh_tokens`), `get_user_roles` (resolves role IDs or names to `UserRole` records), and the `RefreshTokenTarget` enum distinguishing user-password vs. API-token refresh flows.
