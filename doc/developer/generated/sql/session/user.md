---
source: src/sql/src/session/user.rs
revision: c61ef02a01
---

# mz-sql::session::user

Defines the `User` struct (name plus optional external/internal metadata), `RoleMetadata` (current/session/authenticated role IDs), and the static built-in user instances: `SYSTEM_USER` (`mz_system`), `SUPPORT_USER` (`mz_support`), `ANALYTICS_USER` (`mz_analytics`), `HTTP_DEFAULT_USER`, and their well-known `RoleId` constants.
Also provides `INTERNAL_USER_NAMES` and `INTERNAL_USER_NAME_TO_DEFAULT_CLUSTER` maps used for routing and privilege checks.
