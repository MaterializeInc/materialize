// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getStore } from "~/jotai";

import {
  createSubscribeCollection,
  syncEngineCacheKey,
} from "./subscribeCollection";
import { SubscribeState } from "./SubscribeManager";

type Row = { id: string; name: string };

const state = (
  data: Row[],
  snapshotComplete = true,
  error: SubscribeState<Row>["error"] = undefined,
): SubscribeState<Row> => ({ data, snapshotComplete, error });

/** Let the collection's synchronous sync writes settle before asserting. */
const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

let counter = 0;
const freshCollection = (persistName?: string) =>
  createSubscribeCollection<Row>({
    id: `test-collection-${counter++}`,
    getKey: (row) => row.id,
    persistName,
  });

describe("createSubscribeCollection", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("applies inserts, updates, and deletes as deltas", async () => {
    const { collection, applySnapshot } = freshCollection();

    applySnapshot(
      state([
        { id: "1", name: "a" },
        { id: "2", name: "b" },
      ]),
    );
    await flush();
    expect(collection.size).toBe(2);

    applySnapshot(
      state([
        { id: "1", name: "a2" },
        { id: "3", name: "c" },
      ]),
    );
    await flush();
    expect(collection.size).toBe(2);
    expect(collection.get("1")?.name).toBe("a2"); // updated
    expect(collection.has("2")).toBe(false); // deleted
    expect(collection.get("3")?.name).toBe("c"); // inserted
  });

  it("hydrates from the scoped cache before any snapshot arrives", async () => {
    const name = "seed";
    const scope = "org1|region1";
    localStorage.setItem(
      syncEngineCacheKey(name, scope),
      JSON.stringify([
        { id: "1", name: "a" },
        { id: "2", name: "b" },
      ]),
    );

    const { collection, statusAtom, hydrate } = freshCollection(name);
    hydrate(scope);
    await flush();

    expect(collection.size).toBe(2);
    expect(collection.get("1")?.name).toBe("a");
    // A cached full snapshot counts as complete for loading-gate purposes.
    expect(getStore().get(statusAtom).snapshotComplete).toBe(true);
  });

  it("scopes the cache per org/region and prunes other scopes on hydrate", async () => {
    const name = "scoped";
    const keyA = syncEngineCacheKey(name, "orgA|region");
    const keyB = syncEngineCacheKey(name, "orgB|region");
    localStorage.setItem(keyA, JSON.stringify([{ id: "1", name: "from-a" }]));
    localStorage.setItem(keyB, JSON.stringify([{ id: "9", name: "from-b" }]));

    const { collection, hydrate } = freshCollection(name);
    hydrate("orgA|region");
    await flush();

    // Only org A's cache seeds the collection; org B's rows never appear.
    expect(collection.has("1")).toBe(true);
    expect(collection.has("9")).toBe(false);
    // Org B's cache is pruned from disk; org A's remains.
    expect(localStorage.getItem(keyB)).toBeNull();
    expect(localStorage.getItem(keyA)).not.toBeNull();
  });

  it("hydrates from cache after an empty pre-snapshot was applied", async () => {
    // The empty initial state is pushed before the async scope resolves, so
    // hydrate always runs after it; it must not count as live data.
    const name = "late-hydrate";
    const scope = "org1|region1";
    localStorage.setItem(
      syncEngineCacheKey(name, scope),
      JSON.stringify([{ id: "1", name: "a" }]),
    );

    const { collection, statusAtom, applySnapshot, hydrate } =
      freshCollection(name);
    applySnapshot(state([], false));
    hydrate(scope);
    await flush();

    expect(collection.size).toBe(1);
    expect(getStore().get(statusAtom).snapshotComplete).toBe(true);

    // The live snapshot still replaces the cached rows once it arrives.
    applySnapshot(state([{ id: "2", name: "b" }]));
    await flush();
    expect(collection.has("1")).toBe(false);
    expect(collection.get("2")?.name).toBe("b");
  });

  it("holds the current rows through an empty pre-snapshot", async () => {
    const { collection, applySnapshot } = freshCollection();
    applySnapshot(state([{ id: "1", name: "a" }]));
    await flush();
    expect(collection.size).toBe(1);

    // A fresh manager's empty, not-yet-complete snapshot must not clear state.
    applySnapshot(state([], false));
    await flush();
    expect(collection.size).toBe(1);
  });

  it("does not carry the previous scope's rows into a new scope", async () => {
    const name = "scope-switch";
    const keyB = syncEngineCacheKey(name, "org|regionB");
    const { collection, statusAtom, applySnapshot, hydrate } =
      freshCollection(name);

    hydrate("org|regionA");
    applySnapshot(state([{ id: "1", name: "region-a" }]));
    await flush();
    expect(collection.size).toBe(1);

    vi.useFakeTimers();
    try {
      // A region switch re-hydrates with a new scope while region A's rows are
      // still in memory. They must not be persisted under region B's key.
      hydrate("org|regionB");
      vi.advanceTimersByTime(1100);
      expect(localStorage.getItem(keyB)).toBeNull();
    } finally {
      vi.useRealTimers();
    }

    // Nor served as region B data: the collection empties and the loading
    // gate closes until region B's own snapshot arrives.
    await flush();
    expect(collection.size).toBe(0);
    expect(getStore().get(statusAtom).snapshotComplete).toBe(false);
  });

  it("seeds the new scope from its own cache after a scope switch", async () => {
    const name = "scope-switch-seed";
    const keyB = syncEngineCacheKey(name, "org|regionB");
    const { collection, applySnapshot, hydrate } = freshCollection(name);

    hydrate("org|regionA");
    applySnapshot(state([{ id: "1", name: "region-a" }]));
    await flush();

    // Written after region A's hydrate pruned other scopes, as another tab
    // already on region B would.
    localStorage.setItem(keyB, JSON.stringify([{ id: "9", name: "region-b" }]));
    hydrate("org|regionB");
    await flush();

    expect(collection.has("1")).toBe(false);
    expect(collection.get("9")?.name).toBe("region-b");
  });

  it("reset drops rows, pending persistence, and the loading gate", async () => {
    vi.useFakeTimers();
    try {
      const name = "reset";
      const scope = "org|regionA";
      const key = syncEngineCacheKey(name, scope);
      const { collection, statusAtom, applySnapshot, hydrate, reset } =
        freshCollection(name);
      hydrate(scope);
      applySnapshot(state([{ id: "1", name: "a" }]));

      // Reset before the persist throttle fires: nothing may be written.
      reset();
      vi.advanceTimersByTime(1100);
      expect(localStorage.getItem(key)).toBeNull();
      expect(getStore().get(statusAtom).snapshotComplete).toBe(false);

      vi.useRealTimers();
      await flush();
      expect(collection.size).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("suspendPersistence stops writes until a scope resolves again", () => {
    vi.useFakeTimers();
    try {
      const name = "suspend";
      const scope = "org|regionA";
      const key = syncEngineCacheKey(name, scope);
      const { applySnapshot, hydrate, suspendPersistence } =
        freshCollection(name);
      hydrate(scope);
      applySnapshot(state([{ id: "1", name: "a" }]));

      // Suspend cancels the pending write and later snapshots stay unpersisted.
      suspendPersistence();
      applySnapshot(state([{ id: "2", name: "b" }]));
      vi.advanceTimersByTime(1100);
      expect(localStorage.getItem(key)).toBeNull();

      // A scope resolving again re-arms persistence, even the same scope.
      hydrate(scope);
      applySnapshot(state([{ id: "3", name: "c" }]));
      vi.advanceTimersByTime(1100);
      expect(JSON.parse(localStorage.getItem(key) ?? "[]")).toEqual([
        { id: "3", name: "c" },
      ]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("clears a stale error when an empty pre-snapshot follows a reconnect", () => {
    const { applySnapshot, statusAtom } = freshCollection();
    const error = { code: "boom", message: "it failed" };
    applySnapshot(state([], false, error));
    expect(getStore().get(statusAtom).error).toEqual(error);

    // SubscribeManager re-emits an empty pre-snapshot when a reconnect opens;
    // the status must fall back to loading, not hold the error.
    applySnapshot(state([], false));
    expect(getStore().get(statusAtom).error).toBeUndefined();
    expect(getStore().get(statusAtom).snapshotComplete).toBe(false);
  });

  it("surfaces error and snapshotComplete on the status atom", () => {
    const { applySnapshot, statusAtom } = freshCollection();

    applySnapshot(state([{ id: "1", name: "a" }]));
    expect(getStore().get(statusAtom)).toMatchObject({
      snapshotComplete: true,
      error: undefined,
    });

    const error = { code: "boom", message: "it failed" };
    applySnapshot(state([{ id: "1", name: "a" }], true, error));
    expect(getStore().get(statusAtom).error).toEqual(error);
  });

  it("persists only completed snapshots, to the scoped key", () => {
    vi.useFakeTimers();
    try {
      const name = "persist";
      const scope = "org1|region1";
      const key = syncEngineCacheKey(name, scope);
      const { applySnapshot, hydrate } = freshCollection(name);
      hydrate(scope);

      // Incomplete snapshot: never cached, so we never seed from partial state.
      applySnapshot(state([{ id: "1", name: "a" }], false));
      vi.advanceTimersByTime(1100);
      expect(localStorage.getItem(key)).toBeNull();

      // Complete snapshot: cached under the scoped key after the throttle window.
      applySnapshot(
        state([
          { id: "1", name: "a" },
          { id: "2", name: "b" },
        ]),
      );
      vi.advanceTimersByTime(1100);
      expect(JSON.parse(localStorage.getItem(key) ?? "[]")).toEqual([
        { id: "1", name: "a" },
        { id: "2", name: "b" },
      ]);
    } finally {
      vi.useRealTimers();
    }
  });
});
