---
source: src/auth/src/group_claims.rs
revision: 33b8db85da
---

# `auth::group_claims`

Shared JWT group-claim extraction used by both the OIDC and Frontegg authenticators. Centralizes dot-separated claim-path resolution and array-vs-string normalization so both authenticators behave identically.

## Public API

### `extract_groups`

```rust
pub fn extract_groups(
    claims: &BTreeMap<String, serde_json::Value>,
    claim_path: &str,
) -> Option<Vec<String>>
```

Extracts group names from a JWT's unknown-claims map.

`claim_path` may be a bare claim name (e.g. `"groups"`) or a dot-separated path into nested JSON objects (e.g. `"customClaims.groups"`). Keys containing a literal `.` are not reachable — a known limitation matching CockroachDB's `group_claim` semantics. Empty path segments (leading/trailing/double dots, or an empty path) yield `None` and emit a `warn!`-level log so misconfiguration is visible.

Return values:
- `None` — claim is absent; sync is skipped and the current role state is preserved
- `Some(vec![])` — claim is present but empty; all sync-granted roles are revoked
- `Some(vec![...])` — deduplicated, sorted group names (exact case preserved; matching against catalog role names is case-sensitive)

Accepts arrays of strings, single strings, or mixed arrays (non-string elements are filtered out). Other JSON types are treated as absent.

## Private helpers

### `resolve_claim_path`

Walks a dot-separated claim path into nested JSON objects. Returns `None` if the path is empty, any segment is empty, an intermediate segment is missing, or an intermediate segment resolves to a non-object value.
