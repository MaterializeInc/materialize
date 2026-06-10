---
source: src/frontegg-mock/src/handlers/group.rs
revision: e757b4d11b
---

# frontegg-mock::handlers::group

Implements the Frontegg groups API: list, create, get, update, and delete groups, plus add/remove roles to/from a group.
Groups are stored in the `Context.groups` `BTreeMap<String, Group>`, and role additions validate against the server's known roles, returning `404` for unknown role IDs.
