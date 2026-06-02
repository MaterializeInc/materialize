---
source: src/authenticator/src/oidc.rs
revision: 9aa4bf7484
---

# mz-authenticator::oidc

Implements OIDC (OpenID Connect) authentication for pgwire connections.
Provides `GenericOidcAuthenticator`, which validates JWTs by fetching JWKS from the identity provider's discovery document, caching signing keys in memory, and verifying issuer, audience, expiry, and a configurable authentication claim.
The `OidcError` type covers all failure modes, and `OidcClaims` / `ValidatedClaims` carry the verified JWT payload.
Both types implement `Zeroize` and `ZeroizeOnDrop`, wiping their fields from memory when dropped.
`OidcClaims` exposes a `groups(claim_path)` method for JWT group-to-role sync: it extracts group names from the claim identified by `claim_path`, accepting arrays of strings, single strings, or mixed arrays (non-string elements filtered out), and returns deduplicated, sorted group names with exact case preserved. `claim_path` may be a bare claim name (e.g. `"groups"`) or a dot-separated path into nested JSON objects (e.g. `"customClaims.groups"`); empty path segments yield `None`. The private `resolve_claim_path` helper walks the dot-separated segments through nested JSON objects to locate the value. Returns `None` if the claim is absent (skip sync) or when the claim value is an unexpected JSON type; returns `Some(vec![])` when the claim is present but yields no usable names.
