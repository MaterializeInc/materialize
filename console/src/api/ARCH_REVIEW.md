# ARCH_REVIEW: console/src/api

## 1. Dual SQL execution paths (active migration debt)

**Finding**: Two parallel HTTP-to-`/api/sql` implementations coexist:
- `executeSql.tsx` (v1) + `useSqlApiRequest` → `useSqlMany` / `useSqlTyped` / `useSqlLazy`
- `executeSqlV2.ts` + React Query (`useQuery` / `useSuspenseQuery`)

The v2 docstring explicitly marks v1 for deprecation after issue #1176 completes.

**Risk**: Two error-handling contracts, two abort-signal strategies, two response-parsing paths. New callers can accidentally reach for the wrong one.

**Recommendation**: Enumerate the ~15 remaining v1 call sites (search: `useSqlMany`, `useSqlApiRequest`, `useSqlLazy` outside `useSqlManyTyped.ts` / `useSqlTyped.ts`), convert each to `executeSqlV2` + React Query, then delete `executeSql.tsx`, `useSqlApiRequest.ts`, `useSqlMany.ts`, `useSqlTyped.ts`, and the lazy wrapper.

## 2. MaterializeWebsocket singleton coupling

**Finding**: `MaterializeWebsocket.ts` imports `apiClient` at module level (comment: `"TODO: This class should not depend on the apiClient singleton for better modularity and testability"`). This makes the class hard to instantiate in tests without side-effecting the real singleton.

**Recommendation**: Inject `mzWebsocketUrlScheme` and `getWsAuthConfig` as constructor parameters. The two call sites (`ShellWebsocketProvider`, `SubscribeManager`) already know the `apiClient` and can pass the values.

## 3. Per-call region-API client construction

**Finding**: `cloudRegionApi.ts` calls `createClient<paths>({ baseUrl, fetch })` on every `getRegion` / `createRegion` invocation. `baseUrl` is dynamic per region but `fetch` (`apiClient.cloudApiFetch`) is stable.

**Impact**: Low — the overhead is a closure allocation, not a real connection. But it's an asymmetry with `cloudGlobalApi.ts` (module-level singleton).

**Recommendation**: Cache a map from `baseUrl` → client, or document the intentional per-call pattern.

## 4. query_key as URL search param

**Finding**: `executeSqlV2.ts` appends `query_key` as a URL search param with a TODO to move it to a header. It is also parsed by `buildSqlQueryHandler.ts` in tests.

**Impact**: Search params appear in server logs and proxies; a header would be cleaner. Low urgency but the TODO is tracking it.
