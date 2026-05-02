---
source: src/frontegg-client/src/client/user.rs
revision: e757b4d11b
---

# frontegg-client::client::user

Implements `list_users`, `create_user`, and `remove_user` methods on `Client`, covering the full user lifecycle against the Frontegg users API.
Defines `User`, `CreatedUser`, `CreateUserRequest`, and `RemoveUserRequest` as the data types for these operations; `list_users` paginates using the shared `Paginated` wrapper.
