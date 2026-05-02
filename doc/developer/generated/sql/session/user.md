---
source: src/sql/src/session/user.rs
revision: 886e88a1bb
---

# mz-sql::session::user

Defines the `User` struct (name plus optional external/internal metadata and an optional `AuthenticatorKind` tracking which authenticator authenticated the user), `RoleMetadata` (current/session/authenticated role IDs), and the static built-in user instances: `SYSTEM_USER` (`mz_system`), `SUPPORT_USER` (`mz_support`), `ANALYTICS_USER` (`mz_analytics`), `HTTP_DEFAULT_USER`, and their well-known `RoleId` constants.
Well-known role ID constants: `MZ_SYSTEM_ROLE_ID` (`RoleId::System(1)`), `MZ_SUPPORT_ROLE_ID` (`RoleId::System(2)`), `MZ_ANALYTICS_ROLE_ID` (`RoleId::System(3)`), `MZ_JWT_SYNC_ROLE_ID` (`RoleId::System(4)`, a sentinel for JWT group-sync-managed role memberships), and predefined `MZ_MONITOR_ROLE_ID`/`MZ_MONITOR_REDACTED_ROLE_ID`.
`JWT_SYNC_ROLE_NAME` is the string `"mz_jwt_sync"` — the name of the JWT sync sentinel role.
Also provides `INTERNAL_USER_NAMES` and `INTERNAL_USER_NAME_TO_DEFAULT_CLUSTER` maps used for routing and privilege checks.
