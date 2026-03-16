---
source: src/frontegg-auth/src/auth.rs
revision: 8bba96c9aa
---

# frontegg-auth::auth

Implements `Authenticator`, the central component that validates Materialize app passwords by exchanging them for Frontegg JWTs, verifying the JWT claims (tenant, expiry, user), and maintaining per-password background refresh tasks so that long-running sessions stay authenticated.
`AuthenticatorConfig` holds the token URL, JWK decoding key, tenant ID, LRU cache size, and refresh drop factor.
Exports `Claims`, `ClaimMetadata`, `ClaimTokenType`, and the refresh constants `DEFAULT_REFRESH_DROP_FACTOR` and `DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE`.
