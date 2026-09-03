// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Collection, createCollection } from "@tanstack/db";
import { atom, PrimitiveAtom } from "jotai";

import { getStore } from "~/jotai";
import storageAvailable from "~/utils/storageAvailable";

import { SubscribeError, SubscribeState } from "./SubscribeManager";

/** Trailing-throttle window for persisting the collection to localStorage. */
const PERSIST_THROTTLE_MS = 1000;

/** localStorage key prefix shared by all sync-engine collection caches. */
const PERSIST_PREFIX = "mz-console:sync-engine:";
/** Bump when a collection's cached row shape changes, to invalidate old caches. */
const PERSIST_VERSION = 1;

/**
 * Cache key for one collection under one auth/region scope, e.g.
 * `mz-console:sync-engine:all-objects|<organizationId>|<regionId>|v1`. Scoping by
 * `scope` keeps one tenant's catalog from seeding another tenant's session on a
 * shared origin.
 */
export function syncEngineCacheKey(name: string, scope: string): string {
  return `${PERSIST_PREFIX}${name}|${scope}|v${PERSIST_VERSION}`;
}

function readPersistedRows<T>(key: string): T[] {
  if (!storageAvailable("localStorage")) return [];
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T[]) : [];
  } catch {
    return [];
  }
}

function writePersistedRows<T>(key: string, rows: T[]) {
  if (!storageAvailable("localStorage")) return;
  try {
    localStorage.setItem(key, JSON.stringify(rows));
  } catch {
    // Ignore quota / serialization errors; the cache is best-effort.
  }
}

/** The non-data part of a SubscribeState, mirrored into a jotai atom so consumers
 * keep the existing error/loading semantics while data lives in the collection. */
export interface SubscribeCollectionStatus {
  error: SubscribeError | undefined;
  snapshotComplete: boolean;
}

export interface SubscribeCollection<T extends object> {
  collection: Collection<T, string>;
  statusAtom: PrimitiveAtom<SubscribeCollectionStatus>;
  /** Feed the latest upsert-reduced state from a SubscribeManager snapshot. The
   * full current row set is diffed against what's already synced and only the
   * delta is written into the collection. Safe to call before sync starts. */
  applySnapshot: (state: SubscribeState<T>) => void;
  /** Seed the collection from the localStorage cache scoped to `scope`
   * (`organizationId|regionId`) and route future writes to that scoped key.
   * Deferred and scoped on purpose: a constant module-level key would seed one
   * tenant's catalog into another's session after an org or region switch. Only
   * the first call per scope seeds; later calls for the same scope no-op. */
  hydrate: (scope: string) => void;
  /** Drop all rows and pending persistence and return to the loading state,
   * for when the held rows belong to another region's catalog. A later
   * hydrate may still seed the new scope from its cache. */
  reset: () => void;
  /** Stop persisting until a scope resolves again. Scope resolution failing
   * must fail closed: writes aimed at a previous scope's key would store one
   * tenant's rows under another's cache. */
  suspendPersistence: () => void;
}

/**
 * Bridges a Materialize SUBSCRIBE (via SubscribeManager) into a TanStack DB
 * collection. SubscribeManager already reduces the diff stream to the current
 * set of rows at the latest closed timestamp; we diff that set against the
 * collection's synced contents to produce incremental insert/update/delete
 * writes. Live queries on top stay incremental.
 */
export function createSubscribeCollection<T extends object>(options: {
  id: string;
  getKey: (row: T) => string;
  /** Collection-name segment of the localStorage cache key, e.g. "all-objects".
   * The full key is completed with the auth/region scope in `hydrate`; without a
   * `persistName` the collection is not persisted. */
  persistName?: string;
}): SubscribeCollection<T> {
  const { id, getKey, persistName } = options;

  // Latest desired full set, keyed. Seeded lazily from the scoped cache in
  // `hydrate`, then kept current by `applySnapshot`.
  let desired = new Map<string, T>();

  const statusAtom = atom<SubscribeCollectionStatus>({
    error: undefined,
    snapshotComplete: false,
  });

  // Captured when the collection starts syncing; cleared when it pauses.
  type WriteApi = Parameters<
    Parameters<typeof createCollection<T, string>>[0]["sync"]["sync"]
  >[0];
  let writeApi: WriteApi | undefined;
  // Signature of each row currently written, for cheap change detection.
  const synced = new Map<string, string>();
  let lastSnapshotComplete = false;
  let ready = false;
  // Full scoped cache key, set on first hydrate; persistence routes here.
  let persistKey: string | undefined;
  // Scope already hydrated, so repeat hydrations for the same scope no-op.
  let hydratedScope: string | undefined;
  // A live SUBSCRIBE snapshot has arrived, so the cache must not overwrite it.
  let liveSnapshotApplied = false;

  const sig = (row: T) => JSON.stringify(row);

  const materialize = () => {
    if (!writeApi) return;

    const inserts: T[] = [];
    const updates: T[] = [];
    const deletes: string[] = [];
    for (const key of synced.keys()) {
      if (!desired.has(key)) deletes.push(key);
    }
    for (const [key, row] of desired) {
      const prev = synced.get(key);
      if (prev === undefined) inserts.push(row);
      else if (prev !== sig(row)) updates.push(row);
    }

    if (inserts.length || updates.length || deletes.length) {
      writeApi.begin();
      for (const key of deletes) {
        writeApi.write({ type: "delete", key });
        synced.delete(key);
      }
      for (const row of inserts) {
        writeApi.write({ type: "insert", value: row });
        synced.set(getKey(row), sig(row));
      }
      for (const row of updates) {
        writeApi.write({ type: "update", value: row });
        synced.set(getKey(row), sig(row));
      }
      writeApi.commit();
    }

    if (lastSnapshotComplete && !ready) {
      writeApi.markReady();
      ready = true;
    }
  };

  const collection = createCollection<T, string>({
    id,
    getKey,
    // Start syncing on creation so the collection can accept writes before any
    // component queries it; the tree's live query keeps it active thereafter.
    startSync: true,
    sync: {
      // Each snapshot carries the entire row, so updates are full rows.
      rowUpdateMode: "full",
      sync: (params) => {
        writeApi = params;
        ready = false;
        // Full reset on (re)start: truncate any rows retained through a pause,
        // then write the desired set (cache-seeded or latest) fresh.
        synced.clear();
        params.begin();
        params.truncate();
        for (const [key, row] of desired) {
          params.write({ type: "insert", value: row });
          synced.set(key, sig(row));
        }
        params.commit();
        if (lastSnapshotComplete) {
          params.markReady();
          ready = true;
        }
        return () => {
          writeApi = undefined;
          synced.clear();
          ready = false;
        };
      },
    },
  });

  let persistTimer: ReturnType<typeof setTimeout> | undefined;
  const schedulePersist = () => {
    if (!persistKey || persistTimer) return;
    const key = persistKey;
    persistTimer = setTimeout(() => {
      persistTimer = undefined;
      writePersistedRows(key, Array.from(desired.values()));
    }, PERSIST_THROTTLE_MS);
  };

  const writeStatus = (next: SubscribeCollectionStatus) => {
    const store = getStore();
    const current = store.get(statusAtom);
    if (
      current.error !== next.error ||
      current.snapshotComplete !== next.snapshotComplete
    ) {
      store.set(statusAtom, next);
    }
  };

  const applySnapshot = (state: SubscribeState<T>) => {
    // An empty pre-snapshot carries no data: don't clear cache-seeded state or
    // count it as live (that would block the later, async-scoped hydrate). It
    // does clear a stale error so a reconnect falls back to loading.
    const isEmptyPreload =
      !state.snapshotComplete && state.data.length === 0 && !state.error;
    if (isEmptyPreload) {
      writeStatus({ error: undefined, snapshotComplete: lastSnapshotComplete });
      return;
    }

    liveSnapshotApplied = true;
    desired = new Map(state.data.map((row) => [getKey(row), row]));
    lastSnapshotComplete = state.snapshotComplete;
    materialize();
    // Only cache a complete snapshot, so we never seed from partial state.
    if (state.snapshotComplete) schedulePersist();
    writeStatus({
      error: state.error,
      snapshotComplete: state.snapshotComplete,
    });
  };

  // Drop this collection's cache entries for every scope but the current one, so
  // a previous tenant's catalog does not linger at rest after a switch.
  const pruneOtherScopes = (currentKey: string) => {
    if (!persistName || !storageAvailable("localStorage")) return;
    const prefix = `${PERSIST_PREFIX}${persistName}|`;
    try {
      const stale: string[] = [];
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key && key.startsWith(prefix) && key !== currentKey)
          stale.push(key);
      }
      stale.forEach((key) => localStorage.removeItem(key));
    } catch {
      // Best-effort cleanup.
    }
  };

  const hydrate = (scope: string) => {
    if (!persistName || hydratedScope === scope) return;
    const scopeChanged = hydratedScope !== undefined;
    hydratedScope = scope;
    persistKey = syncEngineCacheKey(persistName, scope);
    pruneOtherScopes(persistKey);
    if (scopeChanged) {
      // The rows in memory belong to the previous scope: drop them and the
      // pending persist (its timer reads `desired` at fire time).
      if (persistTimer) {
        clearTimeout(persistTimer);
        persistTimer = undefined;
      }
      desired = new Map();
      liveSnapshotApplied = false;
      lastSnapshotComplete = false;
    }
    if (liveSnapshotApplied) {
      // Live data already arrived; don't overwrite it with the cache, but do
      // persist it now that the scoped key is known.
      if (lastSnapshotComplete) schedulePersist();
      return;
    }
    const cached = readPersistedRows<T>(persistKey);
    if (cached.length) {
      desired = new Map(cached.map((row) => [getKey(row), row]));
      // A cached full snapshot counts as complete for the loading gate; the
      // live snapshot refreshes it once it arrives.
      lastSnapshotComplete = true;
    } else if (!scopeChanged) {
      return;
    }
    // On a scope change this also flushes the emptied set, so the collection
    // stops serving the previous scope's rows while the new snapshot loads.
    materialize();
    writeStatus({ error: undefined, snapshotComplete: lastSnapshotComplete });
  };

  const clearPendingPersist = () => {
    if (persistTimer) {
      clearTimeout(persistTimer);
      persistTimer = undefined;
    }
  };

  const reset = () => {
    clearPendingPersist();
    desired = new Map();
    lastSnapshotComplete = false;
    liveSnapshotApplied = false;
    materialize();
    writeStatus({ error: undefined, snapshotComplete: false });
  };

  const suspendPersistence = () => {
    clearPendingPersist();
    persistKey = undefined;
    // Allow a later successful resolution to hydrate again, even for the
    // scope whose resolution just failed.
    hydratedScope = undefined;
  };

  return {
    collection,
    statusAtom,
    applySnapshot,
    hydrate,
    reset,
    suspendPersistence,
  };
}
