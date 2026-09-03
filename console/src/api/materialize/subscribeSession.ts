// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Atom, createStore, PrimitiveAtom } from "jotai";

import { reconnectionStateAtom } from "~/hooks/useAutomaticallyConnectSocket";
import { currentRegionIdSyncAtom } from "~/store/environments";

import { SubscribeCollection } from "./subscribeCollection";
import {
  SelectFunction,
  SubscribeManager,
  SubscribeRow,
  SubscribeState,
  UpsertKeyFunction,
} from "./SubscribeManager";
import { SessionVariables, SqlRequest } from "./types";
import { WebsocketConnectionManager } from "./WebsocketConnectionManager";

type JotaiStore = ReturnType<typeof createStore>;

/** The shape jotai's `loadable()` wraps an async atom into. */
export type ScopeLoadableAtom = Atom<
  | { state: "loading" }
  | { state: "hasError"; error: unknown }
  | { state: "hasData"; data: string | undefined }
>;

/** Hydrates `target` from its scoped cache whenever the scope settles or
 * changes; hydrate itself handles both seeding and the scope-change reset. A
 * failed scope resolution fails closed: persistence is suspended rather than
 * left aimed at a previous scope's key. */
function subscribeToScope(
  store: JotaiStore,
  scopeAtom: ScopeLoadableAtom,
  target: {
    hydrate: (scope: string) => void;
    suspendPersistence: () => void;
  },
) {
  const applyScope = () => {
    const value = store.get(scopeAtom);
    if (value.state === "hasData" && value.data) target.hydrate(value.data);
    else if (value.state === "hasError") target.suspendPersistence();
  };
  applyScope();
  return store.sub(scopeAtom, applyScope);
}

/** Invokes `onRegionChange` when the current region changes. */
function subscribeToRegionChange(
  store: JotaiStore,
  onRegionChange: () => void,
) {
  let regionId = store.get(currentRegionIdSyncAtom);
  return store.sub(currentRegionIdSyncAtom, () => {
    const next = store.get(currentRegionIdSyncAtom);
    if (next === regionId) return;
    regionId = next;
    onRegionChange();
  });
}

export interface SubscribeSessionOptions<T extends object, R> {
  store: JotaiStore;
  request: SqlRequest;
  upsertKey: UpsertKeyFunction<T>;
  select?: SelectFunction<T, R>;
  sessionVariables?: SessionVariables;
  closeSocketOnComplete?: boolean;
}

export interface SubscribeSession<T extends object, R> {
  manager: SubscribeManager<T, R>;
  destroy: () => void;
}

/**
 * One app-session subscribe, wired outside React: a SubscribeManager kept
 * connected by a WebsocketConnectionManager, snapshots pushed into `sink`, and
 * a region-change reset subscribed directly on the store. Hooks only own the
 * session's lifecycle; the composition is testable headlessly.
 */
function createSubscribeSession<T extends object, R>(
  options: SubscribeSessionOptions<T, R>,
  sink: {
    apply: (snapshot: SubscribeState<R>) => void;
    onRegionChange?: () => void;
  },
): SubscribeSession<T, R> {
  const { store, request, upsertKey, select } = options;
  const manager = new SubscribeManager<T, R>({
    request,
    // The connection manager supplies the address on every attempt.
    httpAddress: "",
    upsert: { key: upsertKey },
    select,
    sessionVariables: options.sessionVariables,
    closeSocketOnComplete: options.closeSocketOnComplete,
  });
  const unsubscribeChange = manager.onChange(() =>
    sink.apply(manager.getSnapshot()),
  );
  const connection = new WebsocketConnectionManager(
    manager,
    store,
    reconnectionStateAtom,
  );

  // The held rows belong to the previous region's catalog: reset rather than
  // serve or replay them while the socket re-subscribes at the new address.
  const unsubscribeRegion = subscribeToRegionChange(store, () => {
    manager.reset();
    sink.onRegionChange?.();
  });

  return {
    manager,
    destroy() {
      unsubscribeRegion();
      unsubscribeChange();
      connection.destroy();
    },
  };
}

/** A subscribe session that reduces into a jotai atom (allObjects et al.). */
export function createAtomSubscribeSession<
  T extends object,
  R = SubscribeRow<T>,
>(
  options: SubscribeSessionOptions<T, R> & {
    atom: PrimitiveAtom<SubscribeState<R>>;
  },
): SubscribeSession<T, R> {
  const { store, atom } = options;
  return createSubscribeSession(options, {
    apply: (snapshot) => {
      const current = store.get(atom);
      if (current === snapshot) return;
      // Hold cached atom data through a fresh manager's empty pre-snapshot state.
      const snapshotIsEmptyPreload =
        !snapshot.snapshotComplete && !snapshot.data.length && !snapshot.error;
      if (snapshotIsEmptyPreload && current.data.length) return;
      store.set(atom, snapshot);
    },
    onRegionChange: () =>
      store.set(atom, { data: [], snapshotComplete: false, error: undefined }),
  });
}

/** A subscribe session that feeds a TanStack DB collection (namespaces et al.).
 * The region change clears the collection synchronously; hydrate is
 * responsible only for cache seeding. */
export function createCollectionSubscribeSession<
  T extends object,
  R extends object = SubscribeRow<T>,
>(
  options: SubscribeSessionOptions<T, R> & {
    target: SubscribeCollection<R>;
    scopeAtom?: ScopeLoadableAtom;
  },
): SubscribeSession<T, R> {
  const { store, target, scopeAtom } = options;
  // Hold a subscriber for the session so the collection doesn't pause/GC
  // while no component is querying it.
  const keepAlive = target.collection.subscribeChanges(() => {});
  const unsubscribeScope = scopeAtom
    ? subscribeToScope(store, scopeAtom, target)
    : undefined;
  const session = createSubscribeSession(options, {
    apply: (snapshot) => target.applySnapshot(snapshot),
    onRegionChange: () => target.reset(),
  });
  target.applySnapshot(session.manager.getSnapshot());
  return {
    manager: session.manager,
    destroy() {
      unsubscribeScope?.();
      keepAlive.unsubscribe();
      session.destroy();
    },
  };
}

/**
 * Feeds a collection from an already-running subscribe atom rather than its
 * own socket, adding no upstream load. The source atom's own session resets
 * the atom on a region change, but that reaches this bridge as an empty
 * pre-snapshot, which applySnapshot ignores, so the collection clears itself
 * on the region change too.
 */
export function createAtomFedCollectionSession<R extends object>(options: {
  store: JotaiStore;
  sourceAtom: Atom<SubscribeState<R>>;
  target: SubscribeCollection<R>;
  scopeAtom?: ScopeLoadableAtom;
}): { destroy: () => void } {
  const { store, sourceAtom, target, scopeAtom } = options;
  const unsubscribeScope = scopeAtom
    ? subscribeToScope(store, scopeAtom, target)
    : undefined;
  const unsubscribeRegion = subscribeToRegionChange(store, () =>
    target.reset(),
  );
  const apply = () => target.applySnapshot(store.get(sourceAtom));
  apply();
  const unsubscribeSource = store.sub(sourceAtom, apply);
  return {
    destroy() {
      unsubscribeScope?.();
      unsubscribeRegion();
      unsubscribeSource();
    },
  };
}
