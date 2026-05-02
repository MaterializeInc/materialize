# console/src

## Role
Root source tree for the Materialize web console — a client-only React SPA (Vite, TypeScript).

## Directory map

| Dir / file | Purpose |
|---|---|
| `api/` | HTTP + WebSocket clients, OpenAPI-generated schemas, SQL query layer — see `api/CONTEXT.md` |
| `platform/` | Feature route modules (shell, clusters, sources, sinks, etc.) — see `platform/CONTEXT.md` |
| `store/` | Jotai atoms for global state: `environments.ts` (environment health), `cloudRegions.ts`, `organization.ts`, `allClusters.ts` |
| `components/` | Reusable UI components (Chakra-based) |
| `hooks/` | Cross-cutting custom hooks (flags, RBAC, clipboard, etc.) |
| `config/` | `AppConfig` — resolves cloud vs. self-managed mode, scheme, auth mode; singleton read at startup |
| `external-library-wrappers/` | Frontegg and OIDC wrappers (enables mocking in tests) |
| `layouts/` | `BaseLayout`, `ShellLayout`, `JotaiProviderWrapper` |
| `theme/` | Chakra UI theme overrides |
| `test/` | Test utilities, `renderComponent`, MSW server setup |
| `queries/` | Shared React Query keys / query definitions not owned by a single feature |
| `analytics/`, `access/`, `forms/`, `version/` | Analytics, app-password/license access pages, shared form components, version detection |

## Provider stack (App.tsx)
```
ColorModeProvider
  JotaiProviderWrapper      ← Jotai store
    BrowserRouterWrapper    ← React Router (basename varies cloud/self-managed)
      ChakraProviderWrapper
        QueryClientProvider ← React Query
          FronteggProviderWrapper / OidcProviderWrapper
            UnauthenticatedRoutes → AuthenticatedRoutes
```

## State management
- **Jotai**: global singleton state (current environment, regions, org, shell). Kept minimal; store is accessible outside React via `getStore()` (used by `executeSqlV2` and `WebsocketConnectionManager`).
- **React Query**: server state for all data fetching in feature modules.
- **Local component state / URL params**: everything else.

## Key seams (bubbled from subdirs)
- `apiClient` singleton (`api/apiClient.ts`): single fetch/auth seam; three implementations for cloud, impersonation, self-managed.
- `AppConfig` / `AppConfigSwitch`: cloud vs. self-managed divergence at config and UI level.
- `useFlags` (LaunchDarkly): runtime feature-flag seam.
- `ShellWebsocketProvider`: single WebSocket lifecycle owner for interactive shell.
- `executeSql` (v1) → `executeSqlV2` (React Query): in-flight migration; ~15 legacy call sites remain.

## Open debt (bubbled)
- `executeSqlV2.ts` migration not complete; `useSqlApiRequest` / `useSqlMany` / `useSqlLazy` stack still in use.
- `MaterializeWebsocket` imports `apiClient` singleton directly — noted testability issue.
- `cloudRegionApi.ts` creates a new `openapi-fetch` client per call (no singleton).
- Subsource→table migration (#3400) leaves scattered `TODO` guards in query modules.
