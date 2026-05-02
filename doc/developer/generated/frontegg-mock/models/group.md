---
source: src/frontegg-mock/src/models/group.rs
revision: 8041e666f1
---

# frontegg-mock::models::group

Defines group-related data types: `Group` (full group record with id, name, roles, users, timestamps), `Role`, `User`, `GroupCreateParams`, `GroupUpdateParams`, `GroupsResponse`, `GroupMapping`, `GroupMappingResponse`, `GroupMappingUpdateRequest`, `DefaultRoles`, and user/role membership parameter types.
These types are shared between the group handler (direct group CRUD) and the SSO handler (which embeds `GroupMapping` and `DefaultRoles` within SSO configurations).
