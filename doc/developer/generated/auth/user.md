---
source: src/auth/src/user.rs
revision: 96d9e5e984
---

# `auth::user`

Defines `ExternalUserMetadata`, which carries identity and role information for a user authenticated through an external system (such as Frontegg).

## Key types

- **`ExternalUserMetadata`** — Contains a `user_id: Uuid` (the user's identifier in the external system) and an `admin: bool` flag indicating whether the user holds administrative privileges in that system. This struct is used downstream in `mz_sql::session::user` and `mz_adapter` to make access control decisions.
