---
source: src/frontegg-mock/src/models/token.rs
revision: 8041e666f1
---

# frontegg-mock::models::token

Defines the token-related request and response types for authentication and API token management: `ApiToken`, `AuthUserRequest`, `RefreshTokenRequest`, `UserProfileResponse`, `UserApiTokenRequest/Response`, `CreateTenantApiTokenRequest`, `TenantApiTokenConfig`, and `TenantApiTokenResponse`.
`ApiToken` is the shared credential struct (client_id + secret UUID pair) used for both user-scoped and tenant-scoped API tokens.
