// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom } from "jotai";
import { ws } from "msw";

import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import {
  currentEnvironmentState,
  currentRegionIdAtom,
  EnvironmentsWithHealth,
  environmentsWithHealth,
} from "~/store/environments";
import { defaultRegionId, healthyEnvironment } from "~/test/utils";

import {
  createSubscribeCollection,
  syncEngineCacheKey,
} from "./subscribeCollection";
import { SubscribeState } from "./SubscribeManager";
import {
  createAtomFedCollectionSession,
  createAtomSubscribeSession,
  createCollectionSubscribeSession,
} from "./subscribeSession";

// Accept subscribe sockets and stay silent, so connections neither error nor
// deliver data: the only thing that may then change state is the session.
const subscribeSocket = ws.link("ws://*");

type Row = { id: string };

const request = { queries: [{ query: "SELECT 1", params: [] }] };
const upsertKey = (row: { data: Row }) => row.data.id;

/** Let the collection's async sync writes settle before asserting. */
const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const loadingState: SubscribeState<Row> = {
  data: [],
  snapshotComplete: false,
  error: undefined,
};

function setTwoRegions() {
  const store = getStore();
  store.set(
    environmentsWithHealth,
    new Map<string, unknown>([
      [defaultRegionId, { ...healthyEnvironment, httpAddress: "addr-a:6876" }],
      ["aws/eu-west-1", { ...healthyEnvironment, httpAddress: "addr-b:6876" }],
    ]) as unknown as EnvironmentsWithHealth,
  );
  return store;
}

describe("subscribeSession", () => {
  beforeEach(() => {
    server.use(subscribeSocket.addEventListener("connection", () => {}));
  });

  afterEach(async () => {
    const store = getStore();
    store.set(currentRegionIdAtom, defaultRegionId);
    await store.get(currentEnvironmentState);
  });

  it("resets the atom to loading only when the region actually changes", () => {
    const store = setTwoRegions();
    const testAtom = atom<SubscribeState<Row>>(loadingState);
    const session = createAtomSubscribeSession<Row, Row>({
      store,
      request,
      upsertKey,
      select: (row) => row.data,
      atom: testAtom,
    });
    try {
      // A completed snapshot from the previous region is in the atom.
      store.set(testAtom, {
        data: [{ id: "1" }],
        snapshotComplete: true,
        error: undefined,
      });

      // Re-setting the same region is not a change.
      store.set(currentRegionIdAtom, defaultRegionId);
      expect(store.get(testAtom).data).toEqual([{ id: "1" }]);

      // The previous region's rows must not be served as the new region's data.
      store.set(currentRegionIdAtom, "aws/eu-west-1");
      expect(store.get(testAtom).data).toEqual([]);
      expect(store.get(testAtom).snapshotComplete).toBe(false);
    } finally {
      session.destroy();
    }
  });

  it("stops reacting to region changes after destroy", () => {
    const store = setTwoRegions();
    const testAtom = atom<SubscribeState<Row>>(loadingState);
    const session = createAtomSubscribeSession<Row, Row>({
      store,
      request,
      upsertKey,
      select: (row) => row.data,
      atom: testAtom,
    });
    session.destroy();

    store.set(testAtom, {
      data: [{ id: "1" }],
      snapshotComplete: true,
      error: undefined,
    });
    store.set(currentRegionIdAtom, "aws/eu-west-1");
    expect(store.get(testAtom).data).toEqual([{ id: "1" }]);
  });

  it("clears the collection when the region switches", async () => {
    const store = setTwoRegions();
    const target = createSubscribeCollection<Row>({
      id: "session-test-collection",
      getKey: (row) => row.id,
    });
    const session = createCollectionSubscribeSession<Row, Row>({
      store,
      request,
      upsertKey,
      select: (row) => row.data,
      target,
    });
    try {
      // A completed snapshot from the previous region is in the collection.
      target.applySnapshot({
        data: [{ id: "1" }],
        snapshotComplete: true,
        error: undefined,
      });
      await flush();
      expect(target.collection.size).toBe(1);

      // The rows must be gone and the loading gate closed, not merely a
      // manager reset invoked: that reset reaches applySnapshot as an empty
      // pre-snapshot, which it ignores by design.
      store.set(currentRegionIdAtom, "aws/eu-west-1");
      await flush();
      expect(target.collection.size).toBe(0);
      expect(store.get(target.statusAtom).snapshotComplete).toBe(false);
    } finally {
      session.destroy();
    }
  });

  it("clears an atom-fed collection when the region switches", async () => {
    const store = setTwoRegions();
    const sourceAtom = atom<SubscribeState<Row>>(loadingState);
    const target = createSubscribeCollection<Row>({
      id: "session-test-atom-fed",
      getKey: (row) => row.id,
    });
    const session = createAtomFedCollectionSession({
      store,
      sourceAtom,
      target,
    });
    try {
      store.set(sourceAtom, {
        data: [{ id: "1" }],
        snapshotComplete: true,
        error: undefined,
      });
      await flush();
      expect(target.collection.size).toBe(1);

      store.set(currentRegionIdAtom, "aws/eu-west-1");
      await flush();
      expect(target.collection.size).toBe(0);
      expect(store.get(target.statusAtom).snapshotComplete).toBe(false);
    } finally {
      session.destroy();
    }
  });

  it("suspends persistence when the scope fails to resolve", () => {
    vi.useFakeTimers();
    const store = setTwoRegions();
    const scopeAtom = atom<
      | { state: "loading" }
      | { state: "hasError"; error: unknown }
      | { state: "hasData"; data: string | undefined }
    >({ state: "loading" });
    const sourceAtom = atom<SubscribeState<Row>>(loadingState);
    const target = createSubscribeCollection<Row>({
      id: "session-test-scope-error",
      getKey: (row) => row.id,
      persistName: "session-scope-error",
    });
    const session = createAtomFedCollectionSession({
      store,
      sourceAtom,
      target,
      scopeAtom,
    });
    const key = syncEngineCacheKey("session-scope-error", "org|regionA");
    try {
      store.set(scopeAtom, { state: "hasData", data: "org|regionA" });
      store.set(sourceAtom, {
        data: [{ id: "1" }],
        snapshotComplete: true,
        error: undefined,
      });

      // Scope resolution fails before the persist throttle fires: nothing may
      // be written under the last known scope's key.
      store.set(scopeAtom, { state: "hasError", error: new Error("boom") });
      store.set(sourceAtom, {
        data: [{ id: "2" }],
        snapshotComplete: true,
        error: undefined,
      });
      vi.advanceTimersByTime(1100);
      expect(localStorage.getItem(key)).toBeNull();
    } finally {
      vi.useRealTimers();
      session.destroy();
      localStorage.clear();
    }
  });
});
