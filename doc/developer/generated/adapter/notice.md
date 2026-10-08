---
source: src/adapter/src/notice.rs
revision: 2085eb6397
---

# adapter::notice

Defines `AdapterNotice`, the enum of all informational or warning-level diagnostic messages the adapter emits to clients during query execution.
Each variant carries context-specific fields (e.g. object names, timestamps, trace IDs) and maps to a PostgreSQL `SqlState` code, a `Severity`, and optional `detail`/`hint` strings via `into_response`.
`DroppedInUseIndex` is a companion struct carrying the index name and the dependent objects preventing its immediate removal.
Two variants cover OIDC group-to-role sync events: `OidcGroupSyncReservedRole { group }` (Warning) for groups that map to reserved role names, and `OidcGroupSyncError { message }` (Warning) for sync failures in fail-open mode. Groups with no matching catalog role are skipped silently and do not produce a notice.
