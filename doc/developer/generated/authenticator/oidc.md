---
source: src/authenticator/src/oidc.rs
revision: 33b8db85da
---

# mz-authenticator::oidc

Implements OIDC (OpenID Connect) authentication for pgwire connections.
Provides `GenericOidcAuthenticator`, which validates JWTs by fetching JWKS from the identity provider's discovery document, caching signing keys in memory, and verifying issuer, audience, expiry, and a configurable authentication claim.
The `OidcError` type covers all failure modes, and `OidcClaims` / `ValidatedClaims` carry the verified JWT payload.
Both types implement `Zeroize` and `ZeroizeOnDrop`, wiping their fields from memory when dropped.
`ValidatedClaims` includes a `groups` field (`Option<Vec<String>>`) populated from the dyncfg-configured `OIDC_GROUP_CLAIM` claim path after successful token validation.
`OidcClaims` exposes a `groups(claim_path)` method for JWT group-to-role sync: it delegates to `mz_auth::group_claims::extract_groups`, which extracts group names from the claim identified by `claim_path`, accepting arrays of strings, single strings, or mixed arrays (non-string elements filtered out), and returns deduplicated, sorted group names with exact case preserved. `claim_path` may be a bare claim name (e.g. `"groups"`) or a dot-separated path into nested JSON objects (e.g. `"customClaims.groups"`); empty path segments yield `None`. Returns `None` if the claim is absent (skip sync) or when the claim value is an unexpected JSON type; returns `Some(vec![])` when the claim is present but yields no usable names.
