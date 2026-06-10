---
source: src/frontegg-client/src/client/role.rs
revision: 42fa463871
---

# frontegg-client::client::role

Implements the `list_roles` method on `Client`, paginating through the Frontegg roles API (v2) to collect all available roles.
Defines the `Role` struct, which carries the full Frontegg role representation including id, name, key, permissions, and level.
