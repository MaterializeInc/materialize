# console/src/platform

## Role
Feature-route modules: each subdirectory owns the pages, components, and React Query wrappers for one product area. This is the "view" layer — it consumes `src/api` and `src/store` but owns no persistent state of its own.

## Structure (feature dirs)
| Dir | Area |
|---|---|
| `shell/` | Interactive SQL shell; WebSocket FSM (XState), Jotai atoms for history/prompt, SUBSCRIBE output, plan insights |
| `clusters/` | Cluster list, overview, replica management, freshness |
| `sources/` | Source list, overview, create wizard (kafka/postgres/mysql/sqlserver/webhook) |
| `sinks/` | Sink list, overview |
| `connections/` | Connection list |
| `object-explorer/` | Database object browser |
| `query-history/` | Query history (cloud-only) |
| `roles/` | RBAC roles (feature-flagged) |
| `billing/` | Usage, invoices, payment (cloud-only) |
| `environment-overview/` | Health/freshness overview |
| `auth/` | Login (self-managed) |
| `integrations/`, `secrets/`, `connectors/`, `internal/` | Misc feature areas |

## Routing
- `App.tsx` → `UnauthenticatedRoutes` → `AuthenticatedRoutes` → `EnvironmentRoutes`.
- Region is URL-scoped: `/regions/:regionSlug/*`; region slug is synced to Jotai `currentRegionIdAtom`.
- Several routes are behind feature flags (`useFlags`) or mode guards (`AppConfigSwitch`).
- `ShellWebsocketProvider` wraps all region routes, establishing the WebSocket connection shared by the shell.

## Data-fetching patterns
Dominant pattern in feature dirs: `queries.ts` files exporting `useQuery`/`useSuspenseQuery`/`useMutation` wrappers around `fetch*` functions from `src/api/materialize/`. React Query key namespacing via `buildRegionQueryKey`.

Legacy `useSqlMany`/`useSqlTyped`/`useSqlLazy` hooks: ~16 remaining call sites (shell tutorial widgets, `NewClusterForm`, `NewReplicaModal`, `DeleteObjectModal`, a few `use*.ts` hooks in `src/api/materialize/`).

## Shell subsystem
`shell/` is the most complex area:
- `ShellWebsocketProvider.tsx` manages a `MaterializeWebsocket` + XState FSM (`machines/webSocketFsm.ts`) + Jotai shell state atoms.
- `SubscribeManager` handles SUBSCRIBE fan-out.
- Shell state is stored as Jotai atoms (`store/shell.ts`) debounced to reduce React re-renders.
- `commandCache.ts` persists shell history to `localStorage` (versioned, v1 format).

## Seams
- `AppConfigSwitch` is the cloud/self-managed divergence seam at the route level.
- `useFlags` (LaunchDarkly) is the feature-flag seam.
- `ShellWebsocketProvider` is the WebSocket lifecycle seam — it is the only place `MaterializeWebsocket` is instantiated for interactive use (a second instance lives in `SubscribeManager`).
