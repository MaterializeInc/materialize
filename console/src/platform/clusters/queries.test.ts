// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook, waitFor } from "@testing-library/react";
import { millisecondsInMinute } from "date-fns/constants";
import { createStore } from "jotai";
import parse from "postgres-interval";

import { DatabaseObject } from "~/api/materialize/objects";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { roleQueryKeys } from "~/platform/roles/queries";
import { getQueryClient } from "~/queryClient";
import { allObjects } from "~/store/allObjects";
import { createProviderWrapper } from "~/test/utils";

import {
  buildFreshnessData,
  clusterQueryKeys,
  useClusterLineageIds,
  useFreshnessObjects,
  useOwners,
  useReplicaOfflineEvents,
  useRollingWindowStartMs,
} from "./queries";

const ownersColumns = buildColumns([
  "id",
  "name",
  { name: "isOwner", type_oid: MzDataType.bool },
]);

const OWNED_ROLE_ID = "u1";
const UNOWNED_ROLE_ID = "u2";
const UNKNOWN_ROLE_ID = "u404";

function buildOwnersHandler(waitTimeMs?: number) {
  return buildSqlQueryHandlerV2(
    {
      queryKey: roleQueryKeys.owners(),
      results: mapKyselyToTabular({
        columns: ownersColumns,
        rows: [
          { id: OWNED_ROLE_ID, name: "my_role", isOwner: true },
          { id: UNOWNED_ROLE_ID, name: "someone_elses_role", isOwner: false },
        ],
      }),
    },
    { waitTimeMs },
  );
}

const errorOwnersHandler = buildSqlQueryHandlerV2({
  queryKey: roleQueryKeys.owners(),
  results: {
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
    notices: [],
  },
});

async function renderUseOwners() {
  const ProviderWrapper = await createProviderWrapper();
  return renderHook(() => useOwners(), { wrapper: ProviderWrapper });
}

describe("useOwners", () => {
  it("returns true for a role the user can act as", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
  });

  it("returns false for a role the user cannot act as", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    // Settle on a known owner first, so this asserts the resolved answer rather
    // than the in-flight default.
    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    expect(result.current.isOwner(UNOWNED_ROLE_ID)).toBe(false);
  });

  it("returns false for an owner id missing from the result set", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    expect(result.current.isOwner(UNKNOWN_ROLE_ID)).toBe(false);
  });

  it("returns false until the query resolves", async () => {
    server.use(buildOwnersHandler(50));
    const { result } = await renderUseOwners();

    // An owner must not read as an owner before the query settles, otherwise
    // owner-only controls would appear and then disappear.
    expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(false);

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
  });

  it("returns false when the query fails", async () => {
    server.use(errorOwnersHandler);
    const { result } = await renderUseOwners();

    // A failed query leaves no ownership data, so isOwner has no flip to wait
    // on. Wait on the query reaching its error state instead.
    await waitFor(() =>
      expect(
        getQueryClient().getQueryState(roleQueryKeys.owners())?.status,
      ).toEqual("error"),
    );
    expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(false);
  });

  it("keeps a stable isOwner reference across re-renders", async () => {
    server.use(buildOwnersHandler());
    const { result, rerender } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    // Consumers pass isOwner to useMemo dependency arrays, so a new reference on
    // every render would defeat their memoization.
    const firstReference = result.current.isOwner;
    rerender();
    expect(result.current.isOwner).toBe(firstReference);
  });
});

const lineageColumns = buildColumns([
  "clusterId",
  "currentDeploymentClusterId",
  "clusterName",
]);

function buildLineageHandler(
  clusterIdsKey: string,
  rows: {
    clusterId: string;
    currentDeploymentClusterId: string;
    clusterName: string;
  }[],
) {
  return buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.deploymentLineage({ clusterIdsKey }),
    results: mapKyselyToTabular({ columns: lineageColumns, rows }),
  });
}

describe("useClusterLineageIds", () => {
  it("resolves a cluster to itself and its past blue-green deployments", async () => {
    server.use(
      buildLineageHandler("u3", [
        { clusterId: "u3", currentDeploymentClusterId: "u3", clusterName: "a" },
        { clusterId: "u2", currentDeploymentClusterId: "u3", clusterName: "a" },
      ]),
    );
    const ProviderWrapper = await createProviderWrapper();
    const { result } = renderHook(() => useClusterLineageIds(["u3"], true), {
      wrapper: ProviderWrapper,
    });

    await waitFor(() => expect(result.current.data).toEqual(["u2", "u3"]));
  });

  it("resolves a system cluster, which has no lineage rows, to itself", async () => {
    server.use(buildLineageHandler("s2", []));
    const ProviderWrapper = await createProviderWrapper();
    const { result } = renderHook(() => useClusterLineageIds(["s2"], true), {
      wrapper: ProviderWrapper,
    });

    await waitFor(() => expect(result.current.data).toEqual(["s2"]));
  });
});

const offlineEventColumns = buildColumns([
  "replicaId",
  "occurredAt",
  "status",
  "reason",
]);

const TIME_PERIOD_MINUTES = 60;

const offlineEvent = (replicaId: string) => ({
  replicaId,
  occurredAt: "2030-01-01 00:00:30+00",
  status: "offline",
  reason: "oom-killed",
});

function buildOfflineEventsHandler(
  replicaIdsKey: string,
  rows: ReturnType<typeof offlineEvent>[],
  waitTimeMs?: number,
) {
  return buildSqlQueryHandlerV2(
    {
      queryKey: clusterQueryKeys.replicaOfflineEvents({
        replicaIdsKey,
        timePeriodMinutes: TIME_PERIOD_MINUTES,
      }),
      results: mapKyselyToTabular({ columns: offlineEventColumns, rows }),
    },
    { waitTimeMs },
  );
}

async function renderUseReplicaOfflineEvents(
  initialSamples: { replicaId: string }[],
) {
  const ProviderWrapper = await createProviderWrapper();
  return renderHook(
    ({ samples }) =>
      useReplicaOfflineEvents(samples, TIME_PERIOD_MINUTES, true),
    { wrapper: ProviderWrapper, initialProps: { samples: initialSamples } },
  );
}

describe("useReplicaOfflineEvents", () => {
  it("fetches events for the sorted set of sampled replicas", async () => {
    // The handler only answers the sorted, deduplicated key.
    server.use(buildOfflineEventsHandler("u1,u2", [offlineEvent("u2")]));
    const { result } = await renderUseReplicaOfflineEvents([
      { replicaId: "u2" },
      { replicaId: "u1" },
      { replicaId: "u2" },
    ]);

    await waitFor(() => expect(result.current).toEqual([offlineEvent("u2")]));
  });

  it("stays idle while there are no samples", async () => {
    const { result } = await renderUseReplicaOfflineEvents([]);

    expect(
      getQueryClient().getQueryState(
        clusterQueryKeys.replicaOfflineEvents({
          replicaIdsKey: "",
          timePeriodMinutes: TIME_PERIOD_MINUTES,
        }),
      )?.fetchStatus,
    ).toEqual("idle");
    expect(result.current).toBeUndefined();
  });

  it("keeps the previous events while a changed replica set loads", async () => {
    server.use(
      buildOfflineEventsHandler("u1", [offlineEvent("u1")]),
      buildOfflineEventsHandler(
        "u1,u2",
        [offlineEvent("u1"), offlineEvent("u2")],
        50,
      ),
    );
    const { result, rerender } = await renderUseReplicaOfflineEvents([
      { replicaId: "u1" },
    ]);
    await waitFor(() => expect(result.current).toEqual([offlineEvent("u1")]));

    rerender({ samples: [{ replicaId: "u1" }, { replicaId: "u2" }] });
    expect(result.current).toEqual([offlineEvent("u1")]);

    await waitFor(() =>
      expect(result.current).toEqual([offlineEvent("u1"), offlineEvent("u2")]),
    );
  });
});

const buildObject = (
  overrides: Partial<DatabaseObject> & { id: string },
): DatabaseObject =>
  ({
    name: "orders_mv",
    objectType: "materialized-view",
    schemaId: "u1",
    schemaName: "public",
    databaseId: "u1",
    databaseName: "materialize",
    sourceType: null,
    isWebhookTable: "false",
    clusterId: "u1",
    clusterName: "quickstart",
    ...overrides,
  }) as DatabaseObject;

async function renderFreshnessObjects(initial: DatabaseObject[]) {
  const store = createStore();
  store.set(allObjects, {
    data: initial,
    error: undefined,
    snapshotComplete: true,
  });
  const ProviderWrapper = await createProviderWrapper({ store });
  // Counted so a test can wait for the re-render the subscribe causes. Without
  // that wait an identity assertion passes before anything has happened.
  const renders = { count: 0 };
  const view = renderHook(
    () => {
      renders.count += 1;
      return useFreshnessObjects("u1");
    },
    { wrapper: ProviderWrapper },
  );
  return { ...view, store, renders };
}

describe("useFreshnessObjects", () => {
  it("keeps its identity when an object on another cluster changes", async () => {
    // `useAllObjects` emits a new array for any change in the environment. A
    // new array here rebuilds the stats and rows chain behind it, so a cluster
    // nobody is looking at would re-render the page.
    const mine = buildObject({ id: "u10" });
    const { result, store, renders } = await renderFreshnessObjects([
      mine,
      buildObject({ id: "u20", clusterId: "u2", name: "elsewhere" }),
    ]);

    const first = result.current;
    expect(first).toHaveLength(1);
    const before = renders.count;

    store.set(allObjects, {
      data: [
        mine,
        buildObject({ id: "u20", clusterId: "u2", name: "renamed" }),
      ],
      error: undefined,
      snapshotComplete: true,
    });

    // The subscribe emitted, so the hook ran again. What it returns must be
    // the array it returned last time.
    await waitFor(() => expect(renders.count).toBeGreaterThan(before));
    expect(result.current).toBe(first);
  });

  it("changes identity when one of its own objects is renamed", async () => {
    const { result, store } = await renderFreshnessObjects([
      buildObject({ id: "u10", name: "before" }),
    ]);

    const first = result.current;
    store.set(allObjects, {
      data: [buildObject({ id: "u10", name: "after" })],
      error: undefined,
      snapshotComplete: true,
    });

    await waitFor(() => expect(result.current[0].objectName).toBe("after"));
    expect(result.current).not.toBe(first);
  });

  it("keeps a name that contains control characters intact", async () => {
    const { result } = await renderFreshnessObjects([
      buildObject({ id: "u10", name: "orders\u0001u2" }),
      buildObject({ id: "u11", name: "\u0002" }),
    ]);

    expect(result.current.map((o) => [o.objectId, o.objectName])).toEqual([
      ["u10", "orders\u0001u2"],
      ["u11", "\u0002"],
    ]);
  });
});

describe("buildFreshnessData", () => {
  // Newest first, so a builder that trusts row order fails. Three bins a
  // minute apart, with the newest reading the healthiest.
  const readings = [
    { bucketStart: new Date(180_000), objectId: "u1", lag: parse("00:00:01") },
    { bucketStart: new Date(120_000), objectId: "u1", lag: parse("00:00:05") },
    { bucketStart: new Date(60_000), objectId: "u1", lag: parse("00:10:00") },
  ];
  const objects = [
    {
      objectId: "u1",
      objectName: "orders_mv",
      schemaName: "public",
      databaseName: "materialize",
      objectType: "materialized-view",
    },
  ];

  it("runs time forwards whatever order the rows arrive in", () => {
    const data = buildFreshnessData(readings, objects);

    expect(data.startTime).toBe(60_000);
    expect(data.endTime).toBe(180_000);
    expect(data.historicalData.map((d) => d.timestamp)).toEqual([
      60_000, 120_000, 180_000,
    ]);
  });

  it("reports each object's newest bin as its current lag", () => {
    const data = buildFreshnessData(readings, objects);

    expect(data.currentData[0].lag).toEqual(parse("00:00:01"));
  });
});

describe("useRollingWindowStartMs", () => {
  const NOW = Date.parse("2026-10-16T12:00:00Z");
  const HOUR = 60 * millisecondsInMinute;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts one window back and moves forward once per window", () => {
    const { result } = renderHook(() => useRollingWindowStartMs(180));
    expect(result.current).toEqual(NOW - 3 * HOUR);

    act(() => vi.advanceTimersByTime(3 * HOUR - 1));
    expect(result.current).toEqual(NOW - 3 * HOUR);

    act(() => vi.advanceTimersByTime(1));
    expect(result.current).toEqual(NOW);
  });

  it("starts the new window back when the window changes, never the old start", () => {
    const renderedStarts: Array<number | undefined> = [];
    const { rerender } = renderHook(
      (windowMinutes: number | undefined) => {
        const startMs = useRollingWindowStartMs(windowMinutes);
        renderedStarts.push(startMs);
        return startMs;
      },
      { initialProps: 180 },
    );
    act(() => vi.advanceTimersByTime(HOUR));
    renderedStarts.length = 0;

    rerender(1440);

    const dayStart = NOW + HOUR - 24 * HOUR;
    expect(renderedStarts.at(-1)).toEqual(dayStart);
    expect(renderedStarts.filter((startMs) => startMs !== undefined)).toEqual(
      renderedStarts.filter((startMs) => startMs === dayStart),
    );
  });

  it("has no start without a window", () => {
    const { result, rerender } = renderHook(
      (windowMinutes: number | undefined) =>
        useRollingWindowStartMs(windowMinutes),
      { initialProps: 180 },
    );

    rerender(undefined);

    expect(result.current).toBeUndefined();
  });
});
