---
source: src/authenticator/src/oidc.rs
revision: aa7a1afd31
---

# mz-authenticator::oidc

Implements OIDC (OpenID Connect) authentication for pgwire connections.
Provides `GenericOidcAuthenticator`, which validates JWTs by fetching JWKS from the identity provider's discovery document, caching signing keys in memory, and verifying issuer, audience, expiry, and a configurable authentication claim.
The `OidcError` type covers all failure modes, and `OidcClaims` / `ValidatedClaims` carry the verified JWT payload.
