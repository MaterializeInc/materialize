---
source: src/frontegg-mock/src/models/user.rs
revision: 739b03a8fd
---

# frontegg-mock::models::user

Defines user-related data types: `UserConfig` (the in-memory user record with id, email, password, tenant, roles, and optional API tokens), `UserCreate`, `UserRole`, `UserResponse`, `UserRolesResponse`, `UpdateUserRolesRequest`, `UsersV3Query`, `UsersV3Response`, and group membership parameter types.
`UserConfig::generate` provides a convenience constructor with a randomly generated password and initial API token, and `frontegg_password` formats the Frontegg-style `mzp_<client_id><secret>` password string.
