---
source: src/authenticator/src/oidc.rs
revision: 3d4583143e
---

# mz-authenticator::oidc

Implements OIDC (OpenID Connect) authentication for pgwire connections.
Provides `GenericOidcAuthenticator`, which validates JWTs by fetching JWKS from the identity provider's discovery document, caching signing keys in memory, and verifying issuer, audience, expiry, and a configurable authentication claim.
The `OidcError` type covers all failure modes, and `OidcClaims` / `ValidatedClaims` carry the verified JWT payload.
Both types implement `Zeroize` and `ZeroizeOnDrop`, wiping their fields from memory when dropped.
