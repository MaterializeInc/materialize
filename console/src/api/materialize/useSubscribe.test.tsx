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

import { createSubscribeCollection } from "./subscribeCollection";
import { SubscribeState } from "./SubscribeManager";
import {
  createAtomSubscribeSession,
  createCollectionSubscribeSession,
} from "./subscribeSession";

// Accept subscribe sockets and stay silent, so connections neither error nor
// deliver data: the only thing that may then change state is the session.
const subscribeSocket = ws.link("ws://*");

type Row = { id: string };

const request = { queries: [{ query: "SELECT 1", params: [] }] };
const upsertKey = (row: { data: Row }) => row.data.id;

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

  it("resets the collection session's manager when the region switches", () => {
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
      const reset = vi.spyOn(session.manager, "reset");
      store.set(currentRegionIdAtom, "aws/eu-west-1");
      expect(reset).toHaveBeenCalledTimes(1);
    } finally {
      session.destroy();
    }
  });
});
