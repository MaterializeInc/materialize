---
source: src/frontegg-mock/src/utils.rs
revision: 33b8db85da
---

# frontegg-mock::utils

Provides JWT utility functions shared across handlers: `decode_access_token` (validates a Bearer token with RS256), `generate_access_token` (encodes a `Claims` JWT with roles and derived permissions, and embeds a `groups` claim via the flattened `unknown_claims` bag — always present, even as an empty array, so that revocation-from-all-groups produces a present-but-empty claim rather than an omitted one), `generate_refresh_token` (creates a UUID refresh token stored in `Context.refresh_tokens`), `get_user_groups` (resolves the groups a user belongs to by scanning `Context.groups` for entries whose user list contains the given user ID, returning sorted and deduplicated group names), `get_user_roles` (resolves role IDs or names to `UserRole` records), and the `RefreshTokenTarget` enum distinguishing user-password vs. API-token refresh flows.
