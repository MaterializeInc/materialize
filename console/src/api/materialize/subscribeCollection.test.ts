// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getStore } from "~/jotai";

import { createSubscribeCollection } from "./subscribeCollection";
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
const freshCollection = (persistKey?: string) =>
  createSubscribeCollection<Row>({
    id: `test-collection-${counter++}`,
    getKey: (row) => row.id,
    persistKey,
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

  it("seeds from the persisted cache before any snapshot arrives", async () => {
    const persistKey = "test:seed";
    localStorage.setItem(
      persistKey,
      JSON.stringify([
        { id: "1", name: "a" },
        { id: "2", name: "b" },
      ]),
    );

    const { collection, statusAtom } = freshCollection(persistKey);
    await flush();

    expect(collection.size).toBe(2);
    expect(collection.get("1")?.name).toBe("a");
    // A cached full snapshot counts as complete for loading-gate purposes.
    expect(getStore().get(statusAtom).snapshotComplete).toBe(true);
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

  it("persists only completed snapshots to the cache", () => {
    vi.useFakeTimers();
    try {
      const persistKey = "test:persist";
      const { applySnapshot } = freshCollection(persistKey);

      // Incomplete snapshot: never cached, so we never seed from partial state.
      applySnapshot(state([{ id: "1", name: "a" }], false));
      vi.advanceTimersByTime(1100);
      expect(localStorage.getItem(persistKey)).toBeNull();

      // Complete snapshot: cached after the trailing-throttle window.
      applySnapshot(
        state([
          { id: "1", name: "a" },
          { id: "2", name: "b" },
        ]),
      );
      vi.advanceTimersByTime(1100);
      expect(JSON.parse(localStorage.getItem(persistKey) ?? "[]")).toEqual([
        { id: "1", name: "a" },
        { id: "2", name: "b" },
      ]);
    } finally {
      vi.useRealTimers();
    }
  });
});
