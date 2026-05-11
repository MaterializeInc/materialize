---
source: src/authenticator/src/oidc.rs
revision: 0a20c581ea
---

# mz-authenticator::oidc

Implements OIDC (OpenID Connect) authentication for pgwire connections.
Provides `GenericOidcAuthenticator`, which validates JWTs by fetching JWKS from the identity provider's discovery document, caching signing keys in memory, and verifying issuer, audience, expiry, and a configurable authentication claim.
The `OidcError` type covers all failure modes, and `OidcClaims` / `ValidatedClaims` carry the verified JWT payload.
Both types implement `Zeroize` and `ZeroizeOnDrop`, wiping their fields from memory when dropped.
`OidcClaims` exposes a `groups(claim_name)` method for JWT group-to-role sync: it extracts group names from the named claim, accepting arrays of strings, single strings, or mixed arrays (non-string elements filtered out), and returns normalized (trimmed, lowercased, deduplicated, sorted) group names. Returns `None` if the claim is absent (skip sync) or when the claim value is an unexpected JSON type; returns `Some(vec![])` when the claim is present but yields no usable names.
