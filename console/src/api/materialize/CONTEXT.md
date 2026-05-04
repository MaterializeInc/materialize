# console/src/api/materialize

## Role
SQL query-client layer for Materialize's HTTP and WebSocket APIs. All database interaction in the console flows through this module.

## Key modules

| File / dir | Purpose |
|---|---|
| `executeSql.tsx` | Legacy HTTP `/api/sql` fetch wrapper; returns discriminated union `ExecuteSqlOutput` |
| `executeSqlV2.ts` | Newer typed wrapper using Kysely `CompiledQuery`; throws `DatabaseError`/`PermissionError`; adds `query_key` search param; used by React Query migration |
| `db.ts` | Kysely instance with `DummyDriver` (query-builder only, no connection) |
| `useSqlApiRequest.ts` | Core hook: wraps `executeSql` (v1), AbortController, timeout; used by `useSqlMany` |
| `useSqlMany.ts` | Declarative: re-runs query on dep change, aborts stale requests |
| `useSqlTyped.ts` | Single-query typed wrapper over `useSqlMany` |
| `useSqlManyTyped.ts` | Keyed multi-query wrapper; returns object keyed by query name |
| `useSqlLazy.ts` | Imperative variant for form submissions / user actions |
| `MaterializeWebsocket.ts` | WebSocket client implementing `Connectable`; sends `SqlRequest` frames |
| `WebsocketConnectionManager.ts` | Exponential-backoff reconnection manager; subscribes directly to Jotai store for environment health |
| `SubscribeManager.ts` | SUBSCRIBE query fan-out over the WebSocket |
| `cluster/`, `source/`, `sink/`, etc. | Domain query modules: each exports `fetch*` functions using `executeSqlV2` + Kysely builders |

## Data-fetching split (in-flight migration)
Two parallel paths coexist:
- **Legacy path** (`useSqlMany` → `executeSql`): ~15 call sites remain, mainly for imperative / lazy use (`useSqlLazy`, `useSqlTyped`).
- **V2/React Query path** (`executeSqlV2` + `useQuery`/`useSuspenseQuery`): dominant in domain modules; typed end-to-end.

`executeSqlV2.ts` explicitly states: *"Deprecate the old executeSql methods when the migration is finished."* (issue #1176).

## Seams
- `apiClient.mzApiFetch` is the single HTTP seam; all calls go through it; middleware handles auth, version headers, and 401 redirect.
- `apiClient.mzWebsocketUrlScheme` is the WebSocket scheme seam.
- `MaterializeWebsocket` has a TODO noting it should not depend on the `apiClient` singleton (testability).

## Constraints
- Kysely is used as a **query builder only**; `DummyDriver` prevents any direct DB connection.
- `mz_catalog_server` cluster is the default for all catalog queries.
- `types/materialize.d.ts` (generated via `yarn gen:types`) is the schema contract; backwards-compat additions must be annotated.

## Open debt
- `executeSql` (v1) + `useSqlApiRequest` still alive; ~15 non-test call sites.
- Several `TODO (#3400)` comments for subsource→table migration cleanup scattered across query modules.
- `query_key` sent as URL search param rather than a custom header.
