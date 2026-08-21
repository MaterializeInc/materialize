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
  /** When set, the row set is cached in localStorage under this key and used to
   * seed the collection synchronously on creation, so consumers render the last
   * known state instantly while the live SUBSCRIBE reconciles in the background. */
  persistKey?: string;
}): SubscribeCollection<T> {
  const { id, getKey, persistKey } = options;

  // Latest desired full set, keyed. Seeded from the persisted cache so the
  // collection has data before the first SUBSCRIBE snapshot arrives.
  let desired = new Map<string, T>();
  if (persistKey) {
    const cached = readPersistedRows<T>(persistKey);
    if (cached.length) {
      desired = new Map(cached.map((row) => [getKey(row), row]));
    }
  }
  const seededFromCache = desired.size > 0;

  const statusAtom = atom<SubscribeCollectionStatus>({
    error: undefined,
    // A cached full snapshot counts as complete for loading-gate purposes; the
    // live snapshot will refresh it once it arrives.
    snapshotComplete: seededFromCache,
  });

  // Captured when the collection starts syncing; cleared when it pauses.
  type WriteApi = Parameters<
    Parameters<typeof createCollection<T, string>>[0]["sync"]["sync"]
  >[0];
  let writeApi: WriteApi | undefined;
  // Signature of each row currently written, for cheap change detection.
  const synced = new Map<string, string>();
  let lastSnapshotComplete = seededFromCache;
  let ready = false;

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
    // Start syncing on creation; the activator hook holds a subscription to keep
    // it alive for the app session (matching the global SUBSCRIBE atoms).
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
    persistTimer = setTimeout(() => {
      persistTimer = undefined;
      writePersistedRows(persistKey, Array.from(desired.values()));
    }, PERSIST_THROTTLE_MS);
  };

  const applySnapshot = (state: SubscribeState<T>) => {
    // Hold the current (possibly cache-seeded) state through a fresh manager's
    // empty pre-snapshot instead of clearing it back to a loading state. Keyed
    // on our own `desired` map, which is reliably seeded before sync starts —
    // not on collection.size, which lags sync materialization.
    const isEmptyPreload =
      !state.snapshotComplete && state.data.length === 0 && !state.error;
    if (isEmptyPreload && desired.size > 0) return;

    desired = new Map(state.data.map((row) => [getKey(row), row]));
    lastSnapshotComplete = state.snapshotComplete;
    materialize();
    // Only cache a complete snapshot, so we never seed from partial state.
    if (state.snapshotComplete) schedulePersist();
    const store = getStore();
    const current = store.get(statusAtom);
    if (
      current.error !== state.error ||
      current.snapshotComplete !== state.snapshotComplete
    ) {
      store.set(statusAtom, {
        error: state.error,
        snapshotComplete: state.snapshotComplete,
      });
    }
  };

  return { collection, statusAtom, applySnapshot };
}
