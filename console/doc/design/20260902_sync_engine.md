# The sync engine

- Associated: MaterializeInc/materialize#38231, MaterializeInc/materialize#38631

## Overview

The console keeps a live, client-side replica of parts of the Materialize
catalog. The sync engine is the stack that maintains that replica: a
Materialize `SUBSCRIBE` feeds a reduction layer, plain session objects manage
lifecycle, and the results land in one of two stores that pages read from. The
newer of the two stores is TanStack DB, which adds incrementally maintained
live queries and a per-tenant instant-load cache on top of the same feed.

Pages never see the plumbing. They read a jotai atom or run a `useLiveQuery`
against a collection, and the data is simply there, current, across
reconnects and region switches.

```
   Materialize
   SUBSCRIBE ... WITH (PROGRESS) ENVELOPE UPSERT        (one per data set)
        |
        v  WebSocket diff stream
   SubscribeManager           folds diffs into the current row set,
        |                     emits one snapshot per closed timestamp
        v
   subscribe session          plain object: lifecycle, region reset,
        |                     cache hydration, sink writes
        v
   +----+---------------------------+
   |                                |
   v                                v
 jotai atom                  TanStack DB collection <---> localStorage cache
 (SubscribeState)            (rows diffed in)             (scoped, instant load)
   |                                |
   v                                v
 useAtomValue                 useLiveQuery
 consumers                    consumers (incremental)
```

## Why two stores

The jotai atoms (`allObjects`, `allSchemas`, `allClusters`, `allRoles`) hold
each data set as one array plus status. They are simple and fine for consumers
that render the whole set or run a cheap filter.

TanStack DB collections hold the same rows keyed, with a live-query engine on
top. A collection is the right store when a surface needs any of:

- **Incremental recomputation.** A live query (filters, joins, lookups)
  recomputes from row deltas instead of re-scanning the full array on every
  catalog tick. With atoms, every tick replaces the array and every consumer
  re-filters and re-renders.
- **Fine-grained re-renders.** A component watching one key re-renders only
  when that row changes, not whenever anything in the set changes.
- **Instant load.** Collections persist complete snapshots to a per-tenant
  localStorage cache and seed from it on the next visit, so the surface
  renders before the live snapshot arrives.

Both stores are sinks of the same engine. Adopting a collection for a data set
does not change what the server sees: within a tab, the upstream SUBSCRIBE
count stays the same, and a collection can even be fed from an existing atom
(see the atom-fed session below) at zero additional upstream cost.

## The layers

| Layer | File | Owns |
| --- | --- | --- |
| `MaterializeWebsocket` | `api/materialize/MaterializeWebsocket.ts` | The wire: one WebSocket to the SQL HTTP endpoint, one subscribe per connection. |
| `SubscribeManager` | `api/materialize/SubscribeManager.ts` | The SUBSCRIBE protocol: buffers rows per timestamp, folds the upsert stream into the current row set, exposes snapshots via `onChange`/`getSnapshot`. Holds its last snapshot through a resubscribe so the UI does not flash empty. |
| `WebsocketConnectionManager` | `api/materialize/WebsocketConnectionManager.ts` | Connection lifecycle: subscribes to environment health and the current region on the jotai store, connects when healthy, reconnects with backoff, reconnects when the region's address changes, and tears down a socket left pointing at a departed region. |
| Subscribe sessions | `api/materialize/subscribeSession.ts` | Composition: one constructor wires the manager, the connection manager, the sink, the region reset, and cache hydration, in one defined order. |
| Hooks | `api/materialize/useSubscribe.ts` | Lifecycle adapters: a single effect creates the session on mount and destroys it on unmount. |
| Collection bridge | `api/materialize/subscribeCollection.ts` | `createSubscribeCollection`: turns manager snapshots into incremental insert/update/delete writes on a TanStack DB collection, owns the scoped localStorage cache, and mirrors status (error, snapshotComplete) into a companion atom. |
| Cache scope | `store/syncEngineCache.ts` | Derives the cache scope `organizationId\|regionId` as an atom, so hydration can be driven from the store. |

The design rule behind the split: reactive plumbing between module-level
singletons belongs on the jotai store (`store.sub`), not in React effects.
Effects impose ordering between independently registered callbacks, re-run
under Suspense, and force every cross-cutting concern to be threaded into each
hook variant separately. A session wires everything once, synchronously, and
is testable without a DOM.

## Subscribe sessions

A session is a plain object created by a hook's single effect and destroyed on
unmount. Three kinds exist:

- `createAtomSubscribeSession`: owns a socket, reduces into a jotai atom.
  Holds existing atom data through a fresh connection's empty pre-snapshot,
  and resets the manager and the atom to the loading state when the region
  changes, so no page serves another region's catalog.
- `createCollectionSubscribeSession`: owns a socket, feeds a collection.
  Additionally holds a keep-alive subscriber so the collection is not garbage
  collected while no component queries it, clears the collection synchronously
  on a region change, and hydrates it from its scoped cache via the scope atom.
- `createAtomFedCollectionSession`: no socket. Bridges an already running
  subscribe atom into a collection, adding no upstream load, plus the same
  region clear and scoped hydration. (The source atom's own reset reaches the
  bridge as an empty pre-snapshot, which applySnapshot ignores, so the
  collection must clear itself.)

```
                     +--------------------------------------+
  hook               |   subscribe session (plain object)   |
  one effect:        |                                      |      +-----------+
  create / destroy --+-> SubscribeManager <---- diff stream +----- | WebSocket |
                     |   WebsocketConnectionManager --------+----> +-----------+
                     |                     reconnect at addr|
                     |   sink: atom | collection -----------+----> atoms / collections
                     |                                      |            |
                     |   store.sub(regionId)    -> reset    |            v
                     |   store.sub(cacheScope)  -> hydrate  |     localStorage cache
                     +--------------------------------------+     (scoped persist)
```

## The collection bridge

`createSubscribeCollection` is the seam between the feed and TanStack DB. The
manager already reduces the diff stream to the current row set, so the bridge
diffs each snapshot against what the collection holds and writes only the
delta as insert/update/delete operations. Live queries downstream therefore
recompute incrementally.

The bridge also owns the instant-load cache:

- **Persist.** With a `persistName`, complete snapshots (never partial state)
  are written to localStorage on a trailing throttle, under the key
  `mz-console:sync-engine:<name>|<organizationId>|<regionId>|v<version>`.
- **Hydrate.** Seeding is deferred until the scope is known, then the cached
  rows load into the collection and the loading gate opens immediately. A
  cached snapshot counts as complete for gating; the live snapshot replaces it
  when it arrives. Other scopes' cache entries are pruned on hydrate.
- **Fail closed.** A scope resolution that errors suspends persistence
  entirely rather than leaving writes aimed at the previous scope's key.
- **Scope changes.** Re-hydrating under a new scope drops the in-memory rows
  and any pending persist first, so one tenant's rows are never shown as, or
  written under, another tenant's cache.
- **Status.** Error and snapshotComplete mirror into a companion jotai atom so
  consumers keep the same loading/error semantics as atom-backed data.

An empty pre-snapshot from a fresh connection carries no data: the bridge
ignores it rather than clearing cache-seeded state or counting it as live
data, except that it clears a stale error so the UI falls back to loading
during reconnect backoff.

## Data flow

Steady state: the socket delivers the diff stream, the manager folds it and
emits a snapshot per closed timestamp, the sink writes it into the atom or
diffs it into the collection, and `useLiveQuery` consumers recompute
incrementally.

On a region switch:

1. The region atom is written by the region selector.
2. The connection manager sees the new address and reconnects the socket
   there. Pausing on an unhealthy target tears the departed region's socket
   down, so a later return to a healthy region always resumes. A health blip
   within the current region leaves a working socket alone.
3. Each session's region subscription fires synchronously: the manager drops
   its held rows and atoms go back to loading.
4. The cache scope atom recomputes. Collection sessions hydrate the new
   scope's cache, dropping in-memory rows and any pending persist.
5. The new region's snapshot arrives and flows through the sinks as usual.

On a reconnect within one region, the manager keeps its last snapshot and
re-emits it when the socket reopens, so the UI holds data through the
resubscribe instead of flashing empty.

## Boundaries

What the engine is not:

- **Not a server-load lever by itself.** It is a client store. Within one tab
  the upstream SUBSCRIBE count is unchanged by adopting it. Its sessions and
  sinks are transport-agnostic, which is the seam a shared upstream
  (cross-tab or cross-client dedup) can plug into without touching surfaces,
  but the engine alone does not provide that.
- **Not for polled telemetry.** Expensive introspection queries
  (utilization history, arrangement sizes) are react-query territory, with
  polling intervals and its persistence tooling. The engine serves
  catalog-shaped data that a SUBSCRIBE can maintain.
- **Not for page-scoped, request-driven subscribes.** Those use
  `useSubscribe`/`useSubscribeManager`, which tie a manager to a component
  and expose state via `useSyncExternalStore`.

## Adopting a collection for a surface

A surface that moves from an atom read to a collection gets, without further
code: incremental live queries, per-key re-render granularity, instant load
from the scoped cache, keep-alive for the app session, and region-correct
resets. The object explorer is the reference adoption: its tree runs live
queries over the objects collection (fed from the app-wide `allObjects` atom)
and the namespaces collection (its own subscribe), and renders from cache on
revisit before the live snapshot lands.

Declare configuration once at module scope. The hooks require a referentially
stable options object, and a new object identity restarts the session, which
makes accidental instability visible instead of masked.

Atom-backed data set:

```ts
export const allWidgets = atom<SubscribeState<Widget>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

const ALL_WIDGETS_SUBSCRIBE_OPTIONS = {
  atom: allWidgets,
  subscribe: buildSubscribeQuery(buildWidgetsQuery(), { upsertKey: "id" }),
  select: (row: SubscribeRow<Widget>) => row.data,
  upsertKey: (row: SubscribeRow<Widget>) => row.data.id,
};

export function useSubscribeToAllWidgets() {
  useGlobalUpsertSubscribe(ALL_WIDGETS_SUBSCRIBE_OPTIONS);
}
```

Collection with its own subscribe and the instant-load cache:

```ts
export const widgetsCollection = createSubscribeCollection<Widget>({
  id: "widgets",
  getKey: (widget) => widget.id,
  persistName: "widgets",
});

const WIDGETS_SUBSCRIBE_OPTIONS = {
  target: widgetsCollection,
  scopeAtom: syncEngineCacheScopeLoadableAtom,
  subscribe: buildSubscribeQuery(buildWidgetsQuery(), { upsertKey: "id" }),
  select: (row: SubscribeRow<Widget>) => row.data,
  upsertKey: (row: SubscribeRow<Widget>) => row.data.id,
};

export function useSubscribeToWidgetsCollection() {
  useGlobalSubscribeCollection(WIDGETS_SUBSCRIBE_OPTIONS);
}
```

Collection fed from an existing atom, no second socket:

```ts
export const widgetsCollection = createSubscribeCollection<Widget>({
  id: "widgets",
  getKey: (widget) => widget.id,
  persistName: "widgets",
});

export function useSubscribeToWidgetsCollection() {
  const store = useStore();
  React.useEffect(() => {
    const session = createAtomFedCollectionSession({
      store,
      sourceAtom: allWidgets,
      target: widgetsCollection,
      scopeAtom: syncEngineCacheScopeLoadableAtom,
    });
    return session.destroy;
  }, [store]);
}
```

Consumers read collections through `useLiveQuery`, with status from the
companion atom:

```ts
const { data } = useLiveQuery((q) => q.from({ widgets: widgetsCollection.collection }));
const status = useAtomValue(widgetsCollection.statusAtom);
```

Mount the activator hook once: in `AppInitializer` for data the whole app
reads, or in a route component for data one surface reads. Note that a route
component keyed by a path pattern survives param-only navigation, including
region switches, which is why region handling lives in the session rather
than in mount/unmount.

## Testing

Sessions and the collection bridge are plain objects, so their behavior is
asserted headlessly and synchronously: create them against `getStore()`,
write the region or scope atoms or call `applySnapshot`, and assert on the
sink in the same tick. No rendering, no Suspense, no `waitFor`. See
`useSubscribe.test.tsx` and `subscribeCollection.test.ts`. jsdom-rendered
tests of this stack are unreliable (sockets fail and produce error states,
Suspense tears sessions down mid-switch), so prefer session-level tests plus
component tests that mock the activator hooks.
