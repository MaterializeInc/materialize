---
source: src/frontegg-auth/src/auth.rs
revision: 26f46d16d4
---

# frontegg-auth::auth

Implements `Authenticator`, the central component that validates Materialize app passwords by exchanging them for Frontegg JWTs, verifying the JWT claims (tenant, expiry, user), and maintaining per-password background refresh tasks so that long-running sessions stay authenticated.
`AuthenticatorConfig` holds the token URL, JWK decoding key, tenant ID, LRU cache size, and refresh drop factor.
Exports `Claims`, `ClaimMetadata`, `ClaimTokenType`, and the refresh constants `DEFAULT_REFRESH_DROP_FACTOR` and `DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE`.

`Claims` carries an `unknown_claims: BTreeMap<String, serde_json::Value>` field (via serde `flatten`) and a `groups(claim_path)` method that delegates to `mz_auth::group_claims::extract_groups` for JWT group-to-role sync.
`ValidatedClaims` includes a `groups: Option<Vec<String>>` field populated from the caller-supplied `group_claim` path; `None` means the claim was absent (skip sync), `Some([])` means present but empty (revoke all sync-granted roles), and `Some([...])` carries the resolved group names.
`Authenticator::authenticate` and `AuthenticatorInner::validate_access_token` accept an optional `group_claim: Option<&str>` parameter used to drive group extraction.
`AuthSessionHandle` exposes a `groups()` method returning the groups from the most recently refreshed JWT, and the background refresh task broadcasts updated group membership through a `groups_tx` watch channel so cached sessions observe bounded-staleness group changes.
