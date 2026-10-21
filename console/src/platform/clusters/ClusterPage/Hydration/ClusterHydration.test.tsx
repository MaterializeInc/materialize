// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { DatabaseObject } from "~/api/materialize/objects";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { buildClusterServerResponse } from "~/platform/clusters/clustersTestUtils";
import {
  clusterQueryKeys,
  replicaStatusesUpdatedAt,
} from "~/platform/clusters/queries";
import { allClusters } from "~/store/allClusters";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { syncEngineCacheScopeAtom } from "~/store/syncEngineCache";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { mockSubscribeSocket } from "~/test/mockSubscribeSocket";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { assert } from "~/util";
import { parseDbVersion } from "~/version/api";

import { ClusterHydration } from "./ClusterHydration";
import { HYDRATION_HELP } from "./hydrationView";
import {
  hydrationStatesByCluster,
  liveObjectHydrationsByReplica,
} from "./useLiveHydration";

vi.mock("@visx/responsive", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@visx/responsive")>()),
  useParentSize: () => ({
    parentRef: () => {},
    width: 900,
    height: 0,
    resize: () => {},
  }),
}));

const ENVIRONMENT = {
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion("v26.46.0 (0000000000)"),
  },
};
const ENVIRONMENT_WITHOUT_HISTORY = {
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion("v26.42.0 (0000000000)"),
  },
};

const MINUTE_MS = 60_000;
const DAY_MS = 24 * 60 * MINUTE_MS;
const NOW_MS = Date.now();
const TIME_PERIOD_MINUTES = 7 * 24 * 60;
const GIB = 2 ** 30;

const CLUSTER = buildClusterServerResponse(
  { id: "u9", name: "analytics" },
  ["u1", "u2"].map((id, index) => ({
    id,
    name: `r${index + 1}`,
    size: "100cc",
    disk: false,
    statuses: [],
  })),
);
const NO_REPLICAS_CLUSTER = buildClusterServerResponse(
  { id: "u9", name: "analytics" },
  [],
);
const STATUSES_UPDATED_AT = replicaStatusesUpdatedAt(CLUSTER.replicas);
const BOOTED_AT_MS = NOW_MS - 3 * DAY_MS;

const catalogObject = (
  id: string,
  name: string,
  objectType: string,
): DatabaseObject => ({
  databaseName: "materialize",
  databaseId: "u1",
  name,
  schemaName: "public",
  schemaId: "u2",
  id,
  objectType,
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u9",
  clusterName: "analytics",
});

const timestamptz = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.timestamptz });
const float8 = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.float8 });

const episode = (
  replicaId: string,
  startedAtMs: number,
  durationMs: number,
  objectCount: number,
) => ({
  replicaId,
  startedAt: String(startedAtMs),
  finishedAt: String(startedAtMs + durationMs),
  objectCount: String(objectCount),
  peakMemoryBytes: String(6 * GIB),
  peakDiskBytes: String(GIB / 4),
  peakHeapBytes: String(6.4 * GIB),
});

// r2's full hydration, the latest one.
const LATEST_FULL = { startedAtMs: BOOTED_AT_MS + 21_000, durationMs: 280_000 };
const EPISODES = [
  episode("u1", BOOTED_AT_MS + 20_000, 271_000, 8),
  episode("u2", LATEST_FULL.startedAtMs, LATEST_FULL.durationMs, 8),
  episode("u1", NOW_MS - DAY_MS, 26_000, 1),
];

const online = (replicaId: string, atMs: number) => ({
  replicaId,
  processId: "0",
  occurredAt: String(atMs),
  status: "online",
  reason: null,
});
const outOfMemory = (replicaId: string, atMs: number) => ({
  replicaId,
  processId: "0",
  occurredAt: String(atMs),
  status: "offline",
  reason: "oom-killed",
});

interface Scenario {
  environment?: typeof ENVIRONMENT;
  statusRows?: object[];
  /** Hydrated and total objects per replica. */
  progress?: Record<string, [number, number]>;
  /** Live progress rows, in place of the ones `progress` makes up. */
  progressRows?: unknown[][];
  /** Objects created on the cluster in the window. */
  creationRows?: object[];
  hasReplicas?: boolean;
  liveObjects?: unknown[][];
  /** The live progress subscribe never delivers a snapshot. */
  isProgressStuck?: boolean;
  isEpisodesError?: boolean;
  search?: string;
}

const renderHydrationTab = async ({
  environment = ENVIRONMENT,
  statusRows = [online("u1", BOOTED_AT_MS), online("u2", BOOTED_AT_MS)],
  progress = { u1: [9, 9], u2: [9, 9] },
  progressRows,
  creationRows = [
    {
      id: "u11",
      objectType: "materialized-view",
      occurredAt: String(NOW_MS - 2 * DAY_MS),
    },
  ],
  hasReplicas = true,
  liveObjects = [],
  isProgressStuck = false,
  isEpisodesError = false,
  search = "",
}: Scenario = {}) => {
  // Query keys embed the environment version, so set it before the handlers.
  await setFakeEnvironment(getStore().set, "aws/us-east-1", environment);
  const replicaIds = hasReplicas ? ["u1", "u2"] : [];
  getStore().set(
    allClusters,
    mockSubscribeState({ data: [hasReplicas ? CLUSTER : NO_REPLICAS_CLUSTER] }),
  );
  const latestStart = new Date(LATEST_FULL.startedAtMs).toISOString();
  const latestEnd = new Date(
    LATEST_FULL.startedAtMs + LATEST_FULL.durationMs,
  ).toISOString();
  server.use(
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.clusterReplicaHistory({
        clusterId: "u9",
        timePeriodMinutes: TIME_PERIOD_MINUTES,
        currentReplicaIdsKey: replicaIds.join(","),
      }),
      results: mapKyselyToTabular({
        rows: replicaIds.map((replicaId, index) => ({
          replicaId,
          name: `r${index + 1}`,
          size: "100cc",
          createdAt: String(BOOTED_AT_MS),
          droppedAt: null,
        })),
        columns: [timestamptz("createdAt"), timestamptz("droppedAt")],
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaStatusHistory({
        replicaIds: ["u1", "u2"],
        statusesUpdatedAt: STATUSES_UPDATED_AT,
      }),
      results: mapKyselyToTabular({
        rows: statusRows,
        columns: [timestamptz("occurredAt")],
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.clusterHydrationEpisodes({
        clusterId: "u9",
        timePeriodMinutes: TIME_PERIOD_MINUTES,
      }),
      results: isEpisodesError
        ? {
            notices: [],
            error: {
              message: "Something went wrong",
              code: ErrorCode.INTERNAL_ERROR,
            },
          }
        : mapKyselyToTabular({
            rows: EPISODES,
            columns: [
              timestamptz("startedAt"),
              timestamptz("finishedAt"),
              float8("objectCount"),
              float8("peakMemoryBytes"),
              float8("peakDiskBytes"),
              float8("peakHeapBytes"),
            ],
          }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaHeapLimits({
        replicaIds: ["u1", "u2"],
      }),
      results: mapKyselyToTabular({
        rows: ["u1", "u2"].map((replicaId) => ({
          replicaId,
          processId: "0",
          heapLimit: String(10 * GIB),
          memoryBytes: String(GIB),
        })),
        columns: buildColumns(["replicaId", "processId", "heapLimit"]),
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaSizeAllocations({ sizes: ["100cc"] }),
      results: mapKyselyToTabular({
        rows: [
          {
            size: "100cc",
            processes: "1",
            workers: "1",
            cpuNanoCores: "1000000000",
            memoryBytes: String(8 * GIB),
            diskBytes: String(2 * GIB),
          },
        ],
        columns: [
          float8("processes"),
          float8("workers"),
          float8("cpuNanoCores"),
          float8("memoryBytes"),
          float8("diskBytes"),
        ],
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.objectCreationTimes({
        objectIds: ["u10", "u11"],
        timePeriodMinutes: TIME_PERIOD_MINUTES,
      }),
      results: mapKyselyToTabular({
        rows: creationRows,
        columns: [timestamptz("occurredAt")],
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.episodeObjectHydrations({
        replicaId: "u2",
        startDate: latestStart,
        endDate: latestEnd,
      }),
      results: mapKyselyToTabular({
        rows: [
          {
            globalId: "u10",
            objectId: "u10",
            installedAt: String(LATEST_FULL.startedAtMs),
            startedAt: String(LATEST_FULL.startedAtMs),
            hydratedAt: String(LATEST_FULL.startedAtMs + 38_000),
          },
          {
            globalId: "u11",
            objectId: "u11",
            installedAt: String(LATEST_FULL.startedAtMs),
            startedAt: String(LATEST_FULL.startedAtMs + 10_000),
            hydratedAt: String(
              LATEST_FULL.startedAtMs + LATEST_FULL.durationMs,
            ),
          },
        ],
        columns: [
          timestamptz("installedAt"),
          timestamptz("startedAt"),
          timestamptz("hydratedAt"),
        ],
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaCpuPeak({
        replicaId: "u2",
        startDate: latestStart,
        endDate: latestEnd,
      }),
      results: mapKyselyToTabular({
        rows: [{ peakCpuNanoCores: "900000000" }],
        columns: [float8("peakCpuNanoCores")],
      }),
    }),
    mockSubscribeSocket((sql) => {
      if (sql.includes("mz_compute_hydration_times_per_worker")) {
        return {
          columns: [
            buildColumn({ name: "globalId" }),
            buildColumn({ name: "objectId" }),
            timestamptz("installedAt"),
            timestamptz("startedAt"),
            timestamptz("hydratedAt"),
          ],
          rows: liveObjects,
        };
      }
      if (sql.includes("mz_compute_hydration_times")) {
        if (isProgressStuck) return undefined;
        return {
          columns: [
            buildColumn({ name: "replicaId" }),
            buildColumn({ name: "objectId" }),
            buildColumn({ name: "isHydrated", type_oid: MzDataType.bool }),
          ],
          rows:
            progressRows ??
            Object.entries(progress).flatMap(([replicaId, [hydrated, total]]) =>
              Array.from({ length: total }, (_, objectIndex) => [
                replicaId,
                `u${100 + objectIndex}`,
                objectIndex < hydrated,
              ]),
            ),
        };
      }
      return undefined;
    }),
  );
  return renderComponent(
    <Routes>
      <Route path=":clusterId/:clusterName" element={<ClusterHydration />} />
    </Routes>,
    {
      initialRouterEntries: [`/u9/analytics${search}`],
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", environment),
    },
  );
};

describe("ClusterHydration", () => {
  beforeEach(async () => {
    localStorage.clear();
    // The collections outlive a test, which would serve its rows to the next.
    const scope = await getStore().get(syncEngineCacheScopeAtom);
    assert(scope);
    hydrationStatesByCluster.get(scope, "u9").reset();
    liveObjectHydrationsByReplica.get(scope, "u2").reset();
    allObjectsCollection.applySnapshot({
      data: [
        catalogObject("u10", "orders_idx", "index"),
        catalogObject("u11", "customer_ltv", "materialized-view"),
      ],
      snapshotComplete: true,
      error: undefined,
    });
  });

  it("names the latest full hydration and opens a hydration's objects from the table", async () => {
    await renderHydrationTab();

    expect(await screen.findByText("Every replica is hydrated.")).toBeVisible();
    expect(screen.getByText(/and took 4m 40s on r2\.$/)).toBeVisible();
    expect(
      await screen.findByRole("heading", { name: "Full Hydration on r2" }),
    ).toBeVisible();
    expect(
      await screen.findByText(
        /worked longest, for 4m 30s, until the hydration finished/,
      ),
    ).toBeVisible();
    expect(screen.getByText("90%")).toBeVisible();
    // The object created after the latest full hydration.
    const createdSince = screen.getByRole("group", {
      name: "Created since the last full hydration",
    });
    expect(within(createdSince).getByText("customer_ltv")).toBeVisible();

    const table = screen.getByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    expect(
      rows.map((row) => within(row).getAllByRole("cell")[2].textContent),
    ).toEqual(["New objects", "Full hydration", "Full hydration"]);
    await userEvent.click(within(rows[0]).getByRole("button"));
    expect(
      await screen.findByRole("heading", { name: "New Objects on r1" }),
    ).toBeVisible();
  });

  it("calls out a replica that keeps restarting before it finishes", async () => {
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        outOfMemory("u1", NOW_MS - 50 * MINUTE_MS),
        online("u1", NOW_MS - 49 * MINUTE_MS),
        outOfMemory("u1", NOW_MS - 30 * MINUTE_MS),
        online("u1", NOW_MS - 29 * MINUTE_MS),
        outOfMemory("u1", NOW_MS - 10 * MINUTE_MS),
        online("u1", NOW_MS - 9 * MINUTE_MS),
      ],
      progress: { u1: [3, 9], u2: [9, 9] },
    });

    expect(
      await screen.findByText(/^r1 hasn't finished hydrating since /),
    ).toBeVisible();
    expect(
      screen.getByText(
        "It restarted 2 times, each after running out of memory.",
      ),
    ).toBeVisible();
    expect(
      screen.getAllByRole("link", {
        name: "Troubleshoot hydration failures",
      })[0],
    ).toBeVisible();
    expect(
      screen.getByText("Restarted 2 times without finishing"),
    ).toBeVisible();
    const table = await screen.findByRole("table");
    expect(
      within(table).getAllByText("Restarted before finishing"),
    ).toHaveLength(2);
  });

  it("shows what a hydrating replica is working on", async () => {
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", NOW_MS - 2 * MINUTE_MS),
      ],
      progress: { u1: [9, 9], u2: [1, 2] },
      liveObjects: [
        [
          "u10",
          "u10",
          String(NOW_MS - 2 * MINUTE_MS),
          String(NOW_MS - MINUTE_MS),
          null,
        ],
        ["u11", "u11", String(NOW_MS - 2 * MINUTE_MS), null, null],
      ],
    });

    expect(
      await screen.findByText(/^r2 is hydrating: 1 of 2 objects, 2m/),
    ).toBeVisible();
    const working = await screen.findByRole("list", {
      name: "Objects r2 is hydrating",
    });
    expect(within(working).getByText(/^orders_idx · working 1m/)).toBeVisible();
    expect(
      within(working).getByText("1 waiting for a hydration slot"),
    ).toBeVisible();
  });

  it("dates a new object's hydration from its creation, not the replica's boot", async () => {
    await renderHydrationTab({
      // Both replicas booted days ago, and an index created a minute and a
      // half ago is hydrating on r1.
      progressRows: [
        ["u1", "u10", true],
        ["u1", "u11", false],
        ["u2", "u10", true],
        ["u2", "u11", true],
      ],
      creationRows: [
        {
          id: "u11",
          objectType: "materialized-view",
          occurredAt: String(NOW_MS - 90_000),
        },
      ],
    });

    expect(
      await screen.findByText(
        /^r1 is hydrating: 1 of 2 objects, 1m 3\ds so far\.$/,
      ),
    ).toBeVisible();
    expect(
      screen.getByRole("img", { name: /^r1 is hydrating, 1m 3\ds so far$/ }),
    ).toBeInTheDocument();
  });

  it("says a cluster with no replicas has nothing hydrated", async () => {
    await renderHydrationTab({ hasReplicas: false });

    expect(
      await screen.findByText(
        "This cluster has no replicas, so nothing on it is hydrated.",
      ),
    ).toBeVisible();
    expect(screen.getByText("This cluster has no replicas.")).toBeVisible();
  });

  it("explains each kind of hydration from a tip that opens on keyboard focus", async () => {
    await renderHydrationTab();

    const tip = await screen.findByRole("img", {
      name: "About Restarted before finishing",
    });
    act(() => tip.focus());

    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      HYDRATION_HELP.unfinished,
    );
  });

  it("shows the history without waiting for live progress", async () => {
    await renderHydrationTab({ isProgressStuck: true });

    expect(await screen.findByRole("table")).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Full Hydration on r2" }),
    ).toBeVisible();
    // Where each replica stands comes from the live progress.
    expect(
      screen.queryByText("Every replica is hydrated."),
    ).not.toBeInTheDocument();
  });

  it("narrows to the replica chosen on the cluster page", async () => {
    await renderHydrationTab({ search: "?replica=r1" });

    const table = await screen.findByRole("table");
    expect(
      within(table)
        .getAllByRole("row")
        .slice(1)
        .map((row) => within(row).getAllByRole("cell")[1].textContent),
    ).toEqual(["r1", "r1"]);
    expect(
      screen.getByRole("progressbar", { name: "r1 objects hydrated" }),
    ).toBeVisible();
    expect(
      screen.queryByRole("progressbar", { name: "r2 objects hydrated" }),
    ).not.toBeInTheDocument();
  });

  it("keeps live progress when the history fails to load", async () => {
    await renderHydrationTab({ isEpisodesError: true });

    expect(
      await screen.findByText("Hydration history couldn't be loaded."),
    ).toBeVisible();
    expect(
      await screen.findByRole("progressbar", { name: "r1 objects hydrated" }),
    ).toBeVisible();
    expect(
      screen.queryByText("Every replica is hydrated."),
    ).not.toBeInTheDocument();
  });

  it("waits for the catalog before classifying hydrations and counting new objects", async () => {
    allObjectsCollection.reset();
    await renderHydrationTab();

    expect(
      await screen.findByRole("progressbar", { name: "r1 objects hydrated" }),
    ).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.queryByText("No objects")).not.toBeInTheDocument();

    act(() =>
      allObjectsCollection.applySnapshot({
        data: [
          catalogObject("u10", "orders_idx", "index"),
          catalogObject("u11", "customer_ltv", "materialized-view"),
        ],
        snapshotComplete: true,
        error: undefined,
      }),
    );

    expect(await screen.findByRole("table")).toBeVisible();
    const createdSince = screen.getByRole("group", {
      name: "Created since the last full hydration",
    });
    expect(within(createdSince).getByText("1 object")).toBeVisible();
  });

  it("shows the history by replica starts alone when the catalog fails to load", async () => {
    allObjectsCollection.reset();
    await renderHydrationTab();
    act(() =>
      allObjectsCollection.applySnapshot({
        data: [],
        snapshotComplete: false,
        error: {
          message: "Something went wrong",
          code: ErrorCode.INTERNAL_ERROR,
        },
      }),
    );

    expect(await screen.findByRole("table")).toBeVisible();
    expect(screen.getByText("Creation times couldn't be loaded")).toBeVisible();
  });

  it("keeps live progress on environments without hydration history", async () => {
    await renderHydrationTab({ environment: ENVIRONMENT_WITHOUT_HISTORY });

    expect(
      await screen.findByText(
        "Hydration history needs Materialize v26.43 or later.",
      ),
    ).toBeVisible();
    expect(
      await screen.findByRole("progressbar", { name: "r1 objects hydrated" }),
    ).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
});
