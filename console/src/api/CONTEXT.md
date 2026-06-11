# console/src/api

## Role
Top-level API client layer. Owns auth-capable fetch adapters, OpenAPI-generated REST clients for Cloud and region APIs, and the Materialize SQL/WebSocket client sublayer.

## Key modules

| File | Purpose |
|---|---|
| `apiClient.ts` | Singleton factory; produces `CloudApiClient`, `ImpersonationApiClient`, or `SelfManagedApiClient` based on `AppConfig.mode`. Middleware chain (`composeMiddleware`) adds auth headers (Frontegg JWT or OIDC Bearer) and console version header. |
| `cloudGlobalApi.ts` | `openapi-fetch` client for Cloud global API (org, invoices, regions, costs, billing, license keys). Null-guarded: throws if mode is not `"cloud"`. |
| `cloudRegionApi.ts` | Per-region `openapi-fetch` client (create/get region). Client created per-call with dynamic `baseUrl`. |
| `fronteggToken.ts` | Helper to read Frontegg access token outside React. |
| `openApiUtils.ts` | `handleOpenApiResponse` / `handleOpenApiResponseWithBody` — uniform error-throwing wrappers over `openapi-fetch` responses. |
| `buildQueryKeySchema.ts` | React Query key builders (`buildRegionQueryKey`, `buildQueryKeyPart`). |
| `types.ts` | Shared fetch/auth types (`HttpScheme`, `MaterializeAuthConfig`, `TokenAuthConfig`, etc.). |
| `schemas/` | Generated TypeScript from OpenAPI specs: `global-api.ts`, `region-api.ts`, `internal-api.ts`. Regenerated via `yarn gen:api`. |
| `mocks/` | MSW handlers for unit tests (`buildSqlQueryHandlerV2`, etc.). |
| `frontegg/`, `incident-io/` | Frontegg identity API wrappers; Incident.io integration. |
| `materialize/` | SQL/WebSocket sublayer — see `materialize/CONTEXT.md`. |

## Auth architecture
Three client classes share the `IApiClientBase` interface (`mzApiFetch`, `getWsAuthConfig`, schemes):
- `CloudApiClient`: Frontegg JWT via `ContextHolder`; `mzApiFetch` = `cloudApiFetch`.
- `ImpersonationApiClient`: no JWT injection on `mzApiFetch`; org-ID header on `cloudApiFetch`.
- `SelfManagedApiClient`: OIDC Bearer token (if configured), or session-cookie passthrough, or unauthenticated.

The `globalFetch` wrapper (anonymous closure over `window.fetch`) is intentional: allows MSW to intercept in tests without capturing a stale reference.

## Seams
- `apiClient` singleton is the auth seam for all HTTP and WebSocket calls.
- `openapi-fetch` `createClient` is the adapter for REST APIs; typed by generated schemas.
- `cloudRegionApi.ts` creates a new client instance per-call (no shared singleton) because `baseUrl` varies by region — minor inefficiency, low impact.

## Bubbled from materialize/
- In-flight migration from `executeSql` (v1, hook-driven) to `executeSqlV2` (React Query, typed); ~15 legacy call sites remain.
- `MaterializeWebsocket` has a noted testability issue (direct `apiClient` singleton import).
