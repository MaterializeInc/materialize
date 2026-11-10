# Cost and structure checks for mz-console-review

The full checks behind SKILL.md sections 5 and 6. Section numbers match, so
cross-references in either file resolve.

## 5. Check what the change costs

Every Console query runs on `mz_catalog_server`, often a single worker, and every
page view, poll, and open tab pays for it. The page itself runs in a browser on
every interaction. Neither cost shows up in a read of the diff.

### 5.1 catalog_server cost

* **A change to a shared builder changes every caller.** `git grep -n` the builder, list which pages and which polls call it, and say which of them pay for a new join or column. A join added for one page ships to the others whether or not that page's flag is on.
* **EXPLAIN the compiled query on `mz_catalog_server`,** following Phase 1 of the `mz-query-perf` skill. Compile the builder (`.compile()`, parameters inlined) from a throwaway vitest probe, then run it with the Console's session against `materialize/materialized:unstable`:

  ```sql
  SET search_path = mz_catalog, mz_internal, mz_catalog_unstable, mz_introspection;
  SET cluster = mz_catalog_server;
  SET transaction_isolation = serializable;
  EXPLAIN OPTIMIZED PLAN AS TEXT FOR <compiled query>;
  ```

  The `search_path` is `SEARCH_PATH` in `src/api/materialize/executeSql.tsx`. Without it and `SET cluster`, the plan is not the one the Console gets. Read the `Used Indexes` footer for every call shape on base and head. A new `*** full scan ***` on `mz_objects`, `mz_schemas`, `mz_databases`, or a history relation is the finding.
* **Label rows from the global subscribes, not from joins.** `useAllObjects` (mounted in `AppInitializer`) carries id, name, schema, database, `objectType`, `clusterId`, and `sourceType`, and `useAllClusters` carries clusters. A query that joins `mz_objects`, `mz_clusters`, or `mz_object_fully_qualified_names` only to label its rows scans them on every load. Pass the ids and attach labels client-side.
* **How ids reach an index.** Observed on v26.45: a literal `object_id IN (...)` on `mz_wallclock_global_lag_recent_history_ind` is a `(lookup)` up to 330 ids and a full scan from 331. `JOIN unnest(ARRAY[...]::text[]) AS ids(id) ON ids.id = object_id` stays a `(differential join)` at any size. `IN (SELECT id FROM mz_objects WHERE ...)` scans `mz_objects`.
* **A two-column index is not an index on its first column.** `mz_hydration_statuses_ind` is keyed `(object_id, replica_id)`, so filtering by `object_id` alone is a full scan, and EXPLAIN prints a "too wide to use for literal equalities" notice.
* **Fast path is a fact to measure, not a goal.** A plain Map/Filter/Project over an index reads the arrangement without building a dataflow (`Explained Query (fast path)`). A join, `GROUP BY`, or `DISTINCT ON` builds one per query. Only argue that the fast path matters when timing shows it: on `mz_hydration_statuses`, a 2000-id literal list on the fast path took 666 ms where an `unnest` join that builds a dataflow took 50 ms.
* **A subscribe costs per viewer for as long as the tab is open.** A `SUBSCRIBE` over a joined query keeps a dataflow per session at roughly 4.5 microcores per operator per viewer. A bare subscribe over an indexed relation is near zero. Prefer the existing global subscribes, or a bare subscribe joined client-side, to a new joined subscribe.
* **Polls multiply by viewers.** A `refetchInterval` is the query's cost times every open tab. Match `staleTime` and `refetchInterval` to when the answer can change, not to the sampling rate, and remember the default `staleTime` of 0 refetches on every window focus. Wallclock lag is sampled once a minute, but at a 24h range a bin is 24 minutes, so a 60s refetch of a 500 ms query costs roughly 8 mCPU per open tab, against about 2.5 mCPU per user for the whole Console on an M.1-nano `mz_catalog_server`.
* **Size a client-side alternative before calling it cheap.** Build the payload in Node (V8, as in Chrome) and time the parse and the work. For 300 objects with 1440 readings each it was 20 MB uncompressed and about 95 ms, so the payload, not the CPU, was the limit.
* **Per-object columns do not belong in per-bucket rows.** Names, types, and hydration repeated on every time bucket multiply the payload by the bucket count. Return them once, or take them from the client lookup.

### 5.2 Render performance on the hot interaction

Find the interaction that runs at pointer or keystroke rate (a drag, a slider,
typing into a filter) and count what it re-renders. Reason about it only after
you have counted.

* **Probe render counts.** In a throwaway test, bump a counter in the component body or the expensive helper, mock the data hooks with a realistic fixture (`vi.hoisted` for any value a `vi.mock` factory reads), and drive the interaction under fake timers. Report before and after counts in the reply. jsdom inflates absolute times, so report the ratio.

  ```tsx
  // In the component or helper under test, for the probe only:
  (globalThis as any).__renders = ((globalThis as any).__renders ?? 0) + 1;

  vi.useFakeTimers({ shouldAdvanceTime: true });
  await renderComponent(<Page />, { initialRouterEntries: ["/?cluster=u1"] });
  const before = (globalThis as any).__renders ?? 0;
  for (let i = 1; i <= 10; i++) {
    act(() => { fireEvent.change(input, { target: { value: String(i) } }); });
    act(() => { vi.advanceTimersByTime(10); });
  }
  expect({ renders: (globalThis as any).__renders - before }).toEqual("MARKER");
  ```

* **An unstable identity breaks every memo below it.** A hook that returns a freshly parsed array or object each render (`parseX(searchParams.get(...))`) invalidates every `useMemo` keyed on it, so the whole derived pipeline reruns per frame. Memoize on the raw primitive.
* **Give the fast value only to what draws it.** When a value changes per frame (a drag handle) and the rest of the page follows a settled copy, pass the fast value only to the component that draws it. Otherwise memoize derived arrays (`rows.filter(...)`) and inline JSX props, and wrap heavy children in `React.memo`. A comment claiming a drag "does not re-render a row per object" is a claim to probe.
* **Bound what mounts.** A table with a row per object needs pagination, which `useUniversalTable` provides. A chart with a line per object needs a cap or an aggregate (see `LINE_MAX_COUNT` in `platform/clusters/queries.ts`).
* **Suspense on a changing key blanks the section.** `useSuspenseQuery` throws on every new key (window, cluster, or an id list in the key), so everything under the boundary swaps to its fallback. `useQuery` with `placeholderData: keepPreviousData` keeps the old data up while the new key loads.
* **Check input behavior in a real browser.** jsdom does not model number-input sanitization or layout. Bundle a minimal repro with the Console's `esbuild` and drive it with Playwright. If the bundled browsers do not match the installed Playwright, launch with `channel: "chrome"`.

* **Probe URL-state and debounce hooks with `renderHook`.** Wrap them in a `MemoryRouter`, read `useLocation().search`, use `useNavigate()` for a pasted link, and advance fake timers past the settle window:

  ```tsx
  const wrapper = ({ children }) => (
    <MemoryRouter initialEntries={["/?threshold=2000"]}>{children}</MemoryRouter>
  );
  const { result } = renderHook(() => {
    const params = useFreshnessParams();
    const settled = useSettledThreshold(params.threshold, params.setThreshold);
    return { params, settled, search: useLocation().search };
  }, { wrapper });
  act(() => result.current.settled.onChange(5000));
  act(() => { vi.advanceTimersByTime(50); });
  act(() => result.current.params.setPredicate("current")); // URL change inside the window
  act(() => { vi.advanceTimersByTime(500); });
  expect(result.current.search).toEqual("MARKER");
  ```

* **Probe request order with MSW, not `vi.mock`.** A `vi.mock` of an API module did not reach imports inside `platform/clusters/queries.ts`. Record start times in a handler instead:

  ```tsx
  server.use(
    http.post("*/api/sql", async ({ request }) => {
      const body = await request.clone().json();
      const text = body.queries.map((q: any) => q.query).join(" ");
      starts[text.includes("mz_hydration_statuses") ? "hydration" : "lag"] = performance.now();
      await delay(200);
      return HttpResponse.json({
        results: body.queries.map(() => mapKyselyToTabular({ rows: [], columns })),
      });
    }),
  );
  ```

## 6. Audit hooks and component structure

### 6.1 Hooks

List every `useEffect`, `useMemo`, and `useCallback` in the diff and trace who
consumes each value: a memoized child's prop, another hook's dependency list, a
debounce or subscription that must keep one identity, or nothing. Nothing means
the hook can go.

* **A `useMemo` inside a `React.memo` component, keyed only on that component's props or state, never hits.** A render there already means one of them changed.
* **React Query hashes keys by value.** An array memoized only to build a query key buys nothing.
* **A `useCallback` earns its place only when the consumer is memoized or lists it as a dependency.** Inline arrows into unmemoized controls (`SearchableSelect`, `TimePeriodSelect`, native inputs) are fine.
* **`useEffect` is for external sync and cleanup.** After the audit, every effect left should name the external system it syncs with.
* **Check a dependency's identity in its source.** react-router 7's `setSearchParams` depends on `[navigate, searchParams]`, so every URL change gives it, and every `useCallback` built on it, a new identity.
* **A debounce built in `useMemo` from a callback rebuilds when the callback changes, and its cleanup cancels the pending call.** Hold the callback in `useLatestRef` and build the debounce once. The setters feeding it then need no `useCallback` at all.
* **Two `useSuspenseQuery` calls in one component run in series,** because the first throws before the second is reached. Batch the statements into one `executeSqlV2` request, or use `useSuspenseQueries`.
* **Prove both directions.** Before calling a hook unnecessary, remove it in a probe and show render counts and tests unchanged (5.2). Before calling it necessary, remove it and show what breaks. Dropping the setters' `useCallback`s without the `useLatestRef` fix meant a drag never committed.

### 6.2 Components

* **Split loading, deriving, and rendering.** A component that runs data hooks, a chain of memos, and the layout is hard to probe and easy to break. Move a derivation chain whose identity is a performance contract into one hook, with a `renderHook` test that its output keeps its identity across the hot interaction.
* **Keep fast state low.** A value that changes per frame belongs in the lowest component that draws it. When two distant pieces need it (a header input and a graph), move large static blocks such as the control bar into their own component, so they stop re-rendering on every move.
* **Do not pass JSX into a memoized child.** It forces a `useMemo` around an element. Render the alternate state (empty, error) in the parent, and pass the child data.
* **Key the boundary, not the child.** An error boundary that has caught renders its fallback, so re-keying a child it no longer renders does nothing. Put the reset key on `AppErrorBoundary`.
* **Use the layout the neighboring pages use.** The Objects panel's sections are Chakra `Card`s (`ObjectFreshness.tsx`), and the Freshness page's are an `Accordion` (`FreshnessPage.tsx`). A new section layout should match the page it sits beside. Two tables of the same rows with independent sort state is a design question to raise, not a nit.
