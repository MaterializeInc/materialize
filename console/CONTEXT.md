# console

## Role
Materialize web console — client-only React SPA (TypeScript, Vite, Vercel). Communicates with the Materialize Cloud global API, per-region APIs, and Materialize `environmentd` directly via HTTP and WebSocket.

## Technology stack
- React 18, React Router v6, Chakra UI
- Jotai (global state), React Query (server state)
- Kysely (SQL query builder, no driver)
- Frontegg (cloud auth), OIDC (self-managed auth)
- MSW (test mocking), Vitest + Playwright (tests)
- OpenAPI-generated TypeScript schemas (`yarn gen:api`); Kysely-codegen DB types (`yarn gen:types`)

## Deployment modes
| Mode | Auth | SQL endpoint |
|---|---|---|
| Cloud | Frontegg JWT | per-region `environmentd` HTTP/WS |
| Self-managed | OIDC or password or none | configured `environmentd` address |
| Impersonation | Org-ID header | cloud APIs |

## Source tree summary
See `src/CONTEXT.md` for the full layout. Key layers:
- `src/api/` — auth adapters, OpenAPI REST clients, SQL/WebSocket client layer
- `src/platform/` — feature-route modules (shell, clusters, sources, sinks, billing, …)
- `src/store/` — Jotai atoms (environment health, regions, org)
- `src/components/`, `src/hooks/`, `src/layouts/` — shared UI primitives

## Architecture highlights
- **Single auth seam**: `apiClient` singleton with middleware chain; three implementations share `IApiClientBase`.
- **SQL client split**: Kysely query-builder produces `CompiledQuery`; `executeSqlV2` posts it to `environmentd /api/sql`; React Query hooks in `platform/*/queries.ts` wrap the result. A parallel legacy stack (`executeSql` v1 + `useSqlMany`) is being retired (issue #1176, ~15 non-test call sites remain).
- **WebSocket**: `MaterializeWebsocket` + `WebsocketConnectionManager` (exponential backoff, Jotai env-health subscription). One instance in `ShellWebsocketProvider` (interactive shell), one in `SubscribeManager` (SUBSCRIBE queries).
- **Feature flags**: LaunchDarkly via `useFlags`; several routes/features are flag-gated.
- **Cloud/self-managed divergence**: `AppConfig` at startup + `AppConfigSwitch` component at route level.

## Open debt / review flags
- `executeSql` v1 migration incomplete — two fetch-path implementations for the same endpoint coexist.
- `MaterializeWebsocket` singleton coupling (`apiClient` import) — noted testability issue in source.
- `cloudRegionApi.ts` creates a fresh `openapi-fetch` client per call; no region-client cache.
- Subsource→table migration (#3400): scattered `TODO` guards in query and routing modules.
- `query_key` sent as URL search param rather than request header (noted TODO in `executeSqlV2.ts`).
