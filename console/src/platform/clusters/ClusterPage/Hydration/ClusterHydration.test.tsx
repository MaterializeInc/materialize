// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
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
import { AppConfig, SelfManagedAppConfig } from "~/config/AppConfig";
import { appConfigAtom } from "~/config/store";
import { getStore } from "~/jotai";
import { buildClusterServerResponse } from "~/platform/clusters/clustersTestUtils";
import {
  clusterQueryKeys,
  replicaStatusesUpdatedAt,
} from "~/platform/clusters/queries";
import { getQueryClient } from "~/queryClient";
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
// A self-managed install through the operator's Helm chart, unlike the
// emulator.
const HELM_INSTALL_ENVIRONMENT = {
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion("v26.46.0 (0000000000, helm chart: 26.0.0)"),
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

const buildCluster = (offlineReplicaIds: string[] = []) =>
  buildClusterServerResponse(
    { id: "u9", name: "analytics" },
    ["u1", "u2"].map((id, index) => ({
      id,
      name: `r${index + 1}`,
      size: "100cc",
      disk: false,
      statuses: offlineReplicaIds.includes(id)
        ? [
            {
              replica_id: id,
              process_id: "0",
              reason: null,
              status: "offline",
              updated_at: new Date(NOW_MS).toISOString(),
            },
          ]
        : [],
    })),
  );
const NO_REPLICAS_CLUSTER = buildClusterServerResponse(
  { id: "u9", name: "analytics" },
  [],
);
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
// r1's additive hydration, the last to finish.
const LAST_FINISHED = { startedAtMs: NOW_MS - DAY_MS, durationMs: 26_000 };
const EPISODES = [
  episode("u1", BOOTED_AT_MS + 20_000, 271_000, 8),
  episode("u2", LATEST_FULL.startedAtMs, LATEST_FULL.durationMs, 8),
  episode("u1", LAST_FINISHED.startedAtMs, LAST_FINISHED.durationMs, 1),
];

const online = (replicaId: string, atMs: number) => ({
  replicaId,
  processId: "0",
  occurredAt: String(atMs),
  status: "online",
  reason: null,
});
const offline = (replicaId: string, atMs: number) => ({
  replicaId,
  processId: "0",
  occurredAt: String(atMs),
  status: "offline",
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
  appConfig?: AppConfig;
  statusRows?: object[];
  /** Hydrated and total objects per replica. */
  progress?: Record<string, [number, number]>;
  /** Live progress rows, in place of the ones `progress` makes up. */
  progressRows?: unknown[][];
  /** When the cluster's objects were created. */
  creationRows?: object[];
  /** Indexes and materialized views created in the window and dropped since. */
  droppedObjectRows?: object[];
  hasReplicas?: boolean;
  /** Replicas whose process reports offline. */
  offlineReplicaIds?: string[];
  /** Replicas dropped in the window. */
  droppedReplicas?: Array<{
    replicaId: string;
    name: string;
    createdAtMs: number;
    droppedAtMs: number;
  }>;
  /** The CPU peak of the last finished hydration fails to load. */
  isCpuPeakError?: boolean;
  /** Peaks sampled during any window other than a finished hydration's. */
  attemptPeakUsage?: Record<string, string | null>;
  /** Objects in the last finished hydration, beyond its first. */
  extraObjectCount?: number;
  /** Hydration episodes beyond the fixtures' own. */
  extraEpisodes?: object[];
  /** Object hydrations read from a replica, or a failure to read them. */
  replicaObjectRows?: object[] | "error";
  liveObjects?: unknown[][];
  /** The live progress subscribe never delivers a snapshot. */
  isProgressStuck?: boolean;
  isEpisodesError?: boolean;
  /** The current replicas' status history fails to load. */
  isStatusError?: boolean;
  search?: string;
}

/**
 * Waits for the reads of replicas' own hydration times to settle, and for the
 * tab to show what they found.
 */
const settleReplicaReads = async () => {
  await waitFor(() => {
    const reads = getQueryClient()
      .getQueryCache()
      .findAll({
        predicate: ({ queryKey }) =>
          (queryKey[1] as { scope?: string } | undefined)?.scope ===
          "replicaObjectHydrations",
      });
    expect(reads.length).toBeGreaterThan(0);
    expect(reads.every(({ state }) => state.status !== "pending")).toBe(true);
  });
  // The tab hears of a settled read a tick later.
  await act(() => new Promise((resolve) => setTimeout(resolve, 0)));
};

const renderHydrationTab = async ({
  environment = ENVIRONMENT,
  appConfig,
  statusRows = [online("u1", BOOTED_AT_MS), online("u2", BOOTED_AT_MS)],
  progress = { u1: [9, 9], u2: [9, 9] },
  progressRows,
  creationRows = [
    {
      id: "u11",
      objectType: "materialized-view",
      occurredAt: String(BOOTED_AT_MS - DAY_MS),
    },
  ],
  droppedObjectRows = [],
  hasReplicas = true,
  offlineReplicaIds = [],
  droppedReplicas = [],
  isCpuPeakError = false,
  attemptPeakUsage = {
    peakCpuNanoCores: "500000000",
    peakMemoryBytes: String(7.5 * GIB),
    peakHeapBytes: String(9.6 * GIB),
  },
  extraObjectCount = 0,
  extraEpisodes = [],
  replicaObjectRows = [],
  liveObjects = [],
  isProgressStuck = false,
  isEpisodesError = false,
  isStatusError = false,
  search = "",
}: Scenario = {}) => {
  // Query keys embed the environment version, so set it before the handlers.
  await setFakeEnvironment(getStore().set, "aws/us-east-1", environment);
  const replicaIds = hasReplicas ? ["u1", "u2"] : [];
  const cluster = hasReplicas
    ? buildCluster(offlineReplicaIds)
    : NO_REPLICAS_CLUSTER;
  getStore().set(allClusters, mockSubscribeState({ data: [cluster] }));
  const lastFinishedStart = new Date(LAST_FINISHED.startedAtMs).toISOString();
  const lastFinishedEnd = new Date(
    LAST_FINISHED.startedAtMs + LAST_FINISHED.durationMs,
  ).toISOString();
  const latestStart = new Date(LATEST_FULL.startedAtMs).toISOString();
  const latestEnd = new Date(
    LATEST_FULL.startedAtMs + LATEST_FULL.durationMs,
  ).toISOString();
  server.use(
    // Keyed on the restarts that read as finished, so matched by scope.
    http.post("*/api/sql", ({ request }) => {
      const queryKey = JSON.parse(
        new URL(request.url).searchParams.get("query_key") ?? "[]",
      );
      if (queryKey[1]?.scope !== "droppedObjectLifetimes") return undefined;
      return HttpResponse.json({
        results: [
          mapKyselyToTabular({
            rows: droppedObjectRows,
            columns: [timestamptz("createdAt"), timestamptz("droppedAt")],
          }),
        ],
      });
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.clusterReplicaHistory({
        clusterId: "u9",
        timePeriodMinutes: TIME_PERIOD_MINUTES,
        currentReplicaIdsKey: replicaIds.join(","),
      }),
      results: mapKyselyToTabular({
        rows: [
          ...replicaIds.map((replicaId, index) => ({
            replicaId,
            name: `r${index + 1}`,
            size: "100cc",
            createdAt: String(BOOTED_AT_MS),
            droppedAt: null,
          })),
          ...droppedReplicas.map(
            ({ replicaId, name, createdAtMs, droppedAtMs }) => ({
              replicaId,
              name,
              size: "100cc",
              createdAt: String(createdAtMs),
              droppedAt: String(droppedAtMs),
            }),
          ),
        ],
        columns: [timestamptz("createdAt"), timestamptz("droppedAt")],
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaStatusHistory({
        replicaIds: droppedReplicas.map(({ replicaId }) => replicaId),
        statusesUpdatedAt: undefined,
      }),
      results: mapKyselyToTabular({
        rows: [],
        columns: [timestamptz("occurredAt")],
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaStatusHistory({
        replicaIds: ["u1", "u2"],
        statusesUpdatedAt: replicaStatusesUpdatedAt(cluster.replicas),
      }),
      results: isStatusError
        ? {
            notices: [],
            error: {
              message: "Something went wrong",
              code: ErrorCode.INTERNAL_ERROR,
            },
          }
        : mapKyselyToTabular({
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
            rows: [...EPISODES, ...extraEpisodes],
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
      queryKey: clusterQueryKeys.episodeObjectHydrations({
        replicaId: "u1",
        startDate: lastFinishedStart,
        endDate: lastFinishedEnd,
      }),
      results: mapKyselyToTabular({
        rows: [
          {
            globalId: "u11",
            objectId: "u11",
            installedAt: String(LAST_FINISHED.startedAtMs),
            startedAt: String(LAST_FINISHED.startedAtMs),
            hydratedAt: String(
              LAST_FINISHED.startedAtMs + LAST_FINISHED.durationMs,
            ),
          },
          // Dropped objects' dataflows, named by their ids, starting in turn.
          ...Array.from({ length: extraObjectCount }, (_, index) => ({
            globalId: `u${200 + index}`,
            objectId: null,
            installedAt: String(LAST_FINISHED.startedAtMs + index + 1),
            startedAt: String(LAST_FINISHED.startedAtMs + index + 1),
            hydratedAt: String(LAST_FINISHED.startedAtMs + 1_000),
          })),
        ],
        columns: [
          timestamptz("installedAt"),
          timestamptz("startedAt"),
          timestamptz("hydratedAt"),
        ],
      }),
    }),
    // Keyed on when the replica last needed a hydration, so matched by scope.
    http.post("*/api/sql", ({ request }) => {
      const [, params] = JSON.parse(
        new URL(request.url).searchParams.get("query_key") ?? "[]",
      );
      if (params?.scope !== "replicaObjectHydrations") return undefined;
      return HttpResponse.json({
        results: [
          replicaObjectRows === "error"
            ? {
                notices: [],
                error: {
                  message: "Something went wrong",
                  code: ErrorCode.INTERNAL_ERROR,
                },
              }
            : mapKyselyToTabular({
                rows: replicaObjectRows,
                columns: [
                  timestamptz("installedAt"),
                  timestamptz("startedAt"),
                  timestamptz("hydratedAt"),
                ],
              }),
        ],
      });
    }),
    // Keyed on each window, including restarts' windows, so matched by scope.
    http.post("*/api/sql", ({ request }) => {
      const [, params] = JSON.parse(
        new URL(request.url).searchParams.get("query_key") ?? "[]",
      );
      if (params?.scope !== "replicaPeakUsage") return undefined;
      const isLastFinished =
        params.replicaId === "u1" &&
        params.startDate === lastFinishedStart &&
        params.endDate === lastFinishedEnd;
      const isLatestFull =
        params.replicaId === "u2" &&
        params.startDate === latestStart &&
        params.endDate === latestEnd;
      if (isLastFinished && isCpuPeakError) {
        return HttpResponse.json({
          results: [
            {
              notices: [],
              error: {
                message: "Something went wrong",
                code: ErrorCode.INTERNAL_ERROR,
              },
            },
          ],
        });
      }
      const finishedPeaks = { peakMemoryBytes: null, peakHeapBytes: null };
      return HttpResponse.json({
        results: [
          mapKyselyToTabular({
            rows: [
              isLastFinished
                ? { ...finishedPeaks, peakCpuNanoCores: "300000000" }
                : isLatestFull
                  ? { ...finishedPeaks, peakCpuNanoCores: "900000000" }
                  : attemptPeakUsage,
            ],
            columns: [
              float8("peakCpuNanoCores"),
              float8("peakMemoryBytes"),
              float8("peakHeapBytes"),
            ],
          }),
        ],
      });
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
      initializeState: ({ set }) => {
        if (appConfig) set(appConfigAtom, appConfig);
        return setFakeEnvironment(set, "aws/us-east-1", environment);
      },
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
        // Every cluster has introspection indexes, which no hydration counts.
        catalogObject(
          "si144115188075856385",
          "mz_active_peeks_per_worker_u9_primary_idx",
          "index",
        ),
      ],
      snapshotComplete: true,
      error: undefined,
    });
  });

  it("opens the last hydration, and any other from the table", async () => {
    await renderHydrationTab();

    expect(
      await screen.findByRole("heading", { name: "Last Hydration" }),
    ).toBeVisible();
    expect(screen.getByText(/^Additive hydration on r1, /)).toBeVisible();
    // Every replica is hydrated, so there's nothing to call out.
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(screen.queryByRole("note")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Hydration in Progress" }),
    ).not.toBeInTheDocument();
    const table = screen.getByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    expect(
      rows.map((row) => within(row).getAllByRole("cell")[2].textContent),
    ).toEqual(["Additive", "Full", "Full"]);
    await userEvent.click(within(rows[1]).getByRole("button"));
    expect(
      await screen.findByRole("heading", { name: "Selected Hydration" }),
    ).toBeVisible();
    expect(screen.getByText(/^Full hydration on r2, /)).toBeVisible();
    const peakHeap = screen.getByRole("group", { name: "Peak heap" });
    expect(within(peakHeap).getByText("64%")).toBeVisible();
    expect(within(peakHeap).getByText("6.4 GB")).toBeVisible();
    expect(screen.getByText("90%")).toBeVisible();
  });

  it("opens the hydration that ended last, not the one that began last", async () => {
    // r1 began a long hydration before r2's last restart and finished after it.
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        outOfMemory("u2", NOW_MS - 40 * MINUTE_MS),
        online("u2", NOW_MS - 39 * MINUTE_MS),
        outOfMemory("u2", NOW_MS - 20 * MINUTE_MS),
        online("u2", NOW_MS - 19 * MINUTE_MS),
      ],
      progress: { u1: [9, 9], u2: [3, 9] },
      extraEpisodes: [
        episode("u1", NOW_MS - 30 * MINUTE_MS, 20 * MINUTE_MS, 2),
      ],
    });

    expect(
      await screen.findByRole("heading", { name: "Last Hydration" }),
    ).toBeVisible();
    expect(screen.getByText(/^Full hydration on r1, /)).toBeVisible();
    const table = screen.getByRole("table");
    const firstRow = within(table).getAllByRole("row")[1];
    expect(within(firstRow).getAllByRole("cell")[2]).toHaveTextContent(
      "Restarted before finishing",
    );
  });

  it("orders a hydration's objects slowest first, or by when they started", async () => {
    await renderHydrationTab();
    const table = await screen.findByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    await userEvent.click(within(rows[1]).getByRole("button"));
    const waterfall = await screen.findByRole("img", {
      name: "Object hydration times",
    });
    const names = () =>
      within(waterfall)
        .getAllByText(/^(orders_idx|customer_ltv)$/, { selector: "title" })
        .map(({ textContent }) => textContent);
    expect(
      screen.getByRole("button", { name: "Slowest first", pressed: true }),
    ).toBeVisible();
    expect(names()).toEqual(["customer_ltv", "orders_idx"]);
    // Working times alone: one bar per object, and no waiting in the legend.
    expect(screen.queryByText("Waiting")).not.toBeInTheDocument();
    expect(waterfall.querySelectorAll("rect")).toHaveLength(2);

    await userEvent.click(screen.getByRole("button", { name: "By start" }));

    expect(names()).toEqual(["orders_idx", "customer_ltv"]);
    expect(screen.getByText("Waiting")).toBeVisible();
  });

  it("pages through a hydration's objects", async () => {
    await renderHydrationTab({ extraObjectCount: 30 });

    expect(await screen.findByText("Showing 1-30 of 31 objects")).toBeVisible();
    const waterfall = screen.getByRole("img", {
      name: "Object hydration times",
    });
    expect(
      within(waterfall).queryByText("u229", { selector: "title" }),
    ).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Next page" }));

    expect(screen.getByText("Showing 31-31 of 31 objects")).toBeVisible();
    expect(
      within(waterfall).getByText("u229", { selector: "title" }),
    ).toBeInTheDocument();
  });

  it("keeps a hydration's object timings when its CPU sample fails to load", async () => {
    await renderHydrationTab({ isCpuPeakError: true });

    expect(
      await screen.findByRole("img", { name: "Object hydration times" }),
    ).toBeVisible();
    expect(screen.getByText("Couldn't be loaded")).toBeVisible();
  });

  it("keeps the history up while a new replica's or object's data loads", async () => {
    await renderHydrationTab();
    expect(await screen.findByRole("table")).toBeVisible();

    // A third replica and a new index each change the keys the history reads.
    act(() =>
      getStore().set(
        allClusters,
        mockSubscribeState({
          data: [
            buildClusterServerResponse(
              { id: "u9", name: "analytics" },
              ["u1", "u2", "u3"].map((id, index) => ({
                id,
                name: `r${index + 1}`,
                size: "100cc",
                disk: false,
                statuses: [],
              })),
            ),
          ],
        }),
      ),
    );
    act(() =>
      allObjectsCollection.applySnapshot({
        data: [
          catalogObject("u10", "orders_idx", "index"),
          catalogObject("u11", "customer_ltv", "materialized-view"),
          catalogObject("u12", "orders_by_day_idx", "index"),
        ],
        snapshotComplete: true,
        error: undefined,
      }),
    );

    expect(screen.getByRole("table")).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Last Hydration" }),
    ).toBeVisible();
  });

  it("moves a hydrating replica's elapsed time on", async () => {
    // Relative to this test's start, as the fixtures' `NOW_MS` is not.
    const bootedAtMs = Date.now() - 2 * MINUTE_MS;
    await renderHydrationTab({
      statusRows: [online("u1", BOOTED_AT_MS), online("u2", bootedAtMs)],
      progress: { u1: [9, 9], u2: [1, 2] },
    });
    // Followed by how long its last full hydration took.
    expect(await screen.findByText(/^2m \d+s so far · /)).toBeVisible();

    expect(
      await screen.findByText(/^2m [2-9]s so far · /, {}, { timeout: 4_000 }),
    ).toBeVisible();
  });

  it("says restarts that went offline went offline", async () => {
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        offline("u1", NOW_MS - 50 * MINUTE_MS),
        online("u1", NOW_MS - 49 * MINUTE_MS),
        offline("u1", NOW_MS - 30 * MINUTE_MS),
        online("u1", NOW_MS - 29 * MINUTE_MS),
        offline("u1", NOW_MS - 10 * MINUTE_MS),
        online("u1", NOW_MS - 9 * MINUTE_MS),
      ],
      progress: { u1: [3, 9], u2: [9, 9] },
    });

    const ended = await screen.findByRole("group", { name: "Ended" });
    expect(within(ended).getByText("Went offline")).toBeVisible();
    expect(
      within(screen.getByRole("table")).getAllByText(
        "Restarted before finishing",
      ),
    ).toHaveLength(2);
  });

  it("counts a restart an index dropped since cut short", async () => {
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        offline("u1", NOW_MS - 50 * MINUTE_MS),
        online("u1", NOW_MS - 49 * MINUTE_MS),
        offline("u1", NOW_MS - 30 * MINUTE_MS),
        online("u1", NOW_MS - 29 * MINUTE_MS),
        offline("u1", NOW_MS - 10 * MINUTE_MS),
        online("u1", NOW_MS - 9 * MINUTE_MS),
      ],
      progress: { u1: [3, 9], u2: [9, 9] },
      // Created after r1's last finished hydration, before its first restart,
      // and dropped after its last.
      droppedObjectRows: [
        {
          id: "u12",
          objectType: "index",
          createdAt: String(NOW_MS - 55 * MINUTE_MS),
          droppedAt: String(NOW_MS - 5 * MINUTE_MS),
        },
      ],
    });

    const table = await screen.findByRole("table");
    // The third comes from the dropped index, which loads after the others.
    await waitFor(() =>
      expect(
        within(table).getAllByText("Restarted before finishing"),
      ).toHaveLength(3),
    );
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
      await screen.findByText(/^r1 has no finished hydration recorded since /),
    ).toBeVisible();
    expect(
      screen.getByText(
        /^It restarted 2 times, each after running out of memory\. See our/,
      ),
    ).toBeVisible();
    expect(
      within(screen.getByRole("status")).getByRole("link", {
        name: "docs on troubleshooting hydration failures",
      }),
    ).toBeVisible();
    expect(screen.getByText("Hydrated so far: 3 of 9 objects")).toBeVisible();
    // The alert already counts the restarts.
    expect(screen.queryByText(/without finishing/)).not.toBeInTheDocument();
    const table = await screen.findByRole("table");
    expect(
      within(table).getAllByText("Restarted before finishing"),
    ).toHaveLength(2);
    // The last restart, with how it ended and what it used before it did.
    expect(
      screen.getByRole("heading", { name: "Last Hydration" }),
    ).toBeVisible();
    const ended = screen.getByRole("group", { name: "Ended" });
    expect(within(ended).getByText("Out of memory")).toBeVisible();
    expect(
      within(screen.getByRole("group", { name: "Ran for" })).getByText(/^19m/),
    ).toBeVisible();
    const peakHeap = screen.getByRole("group", { name: "Peak heap" });
    expect(await within(peakHeap).findByText("96%")).toBeVisible();
    expect(within(peakHeap).getByText("9.6 GB")).toBeVisible();
    expect(
      screen.queryByRole("img", { name: "Object hydration times" }),
    ).not.toBeInTheDocument();
  });

  it("shows what a hydrating replica is working on and has hydrated so far", async () => {
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", NOW_MS - 2 * MINUTE_MS),
      ],
      progress: { u1: [9, 9], u2: [1, 3] },
      liveObjects: [
        [
          "u10",
          "u10",
          String(NOW_MS - 2 * MINUTE_MS),
          String(NOW_MS - MINUTE_MS),
          null,
        ],
        [
          "u11",
          "u11",
          String(NOW_MS - 2 * MINUTE_MS),
          String(NOW_MS - 2 * MINUTE_MS),
          String(NOW_MS - 90_000),
        ],
        ["u12", "u12", String(NOW_MS - 2 * MINUTE_MS), null, null],
      ],
    });

    expect(
      await screen.findByText("Hydrated so far: 1 of 3 objects"),
    ).toBeVisible();
    // The progress card says it, so there's no alert and no Hydrating label.
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(screen.queryByText("Hydrating")).not.toBeInTheDocument();
    const working = await screen.findByRole("list", {
      name: "Objects r2 is hydrating",
    });
    expect(
      within(working).getByText(
        (_, element) =>
          element?.tagName === "LI" &&
          /^Working on orders_idx for 1m \d+s$/.test(element.textContent ?? ""),
      ),
    ).toBeVisible();
    expect(within(working).queryByText(/waiting/)).not.toBeInTheDocument();
    const hydrated = screen.getByRole("group", {
      name: "Objects r2 has hydrated so far",
    });
    expect(
      within(hydrated)
        .getAllByText(/^(orders_idx|customer_ltv|u12)$/, { selector: "title" })
        .map(({ textContent }) => textContent),
    ).toEqual(["customer_ltv"]);
    expect(within(hydrated).getByText("30s")).toBeVisible();
  });

  it("dates a new object's hydration from its creation, not the replica's boot", async () => {
    // Relative to this test's start: the fixtures' `NOW_MS` is taken when the
    // file loads, and the tests before this one move the clock on.
    const createdAtMs = Date.now() - 90_000;
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
          occurredAt: String(createdAtMs),
        },
      ],
    });

    expect(await screen.findByText(/^1m 3\ds so far/)).toBeVisible();
  });

  it("lists past hydrations with each one's peak memory", async () => {
    await renderHydrationTab();

    expect(
      await screen.findByRole("heading", { name: "Past Hydrations" }),
    ).toBeVisible();
    // Every kind shows, with no filter over them.
    expect(
      screen.queryByRole("list", { name: "Show hydrations" }),
    ).not.toBeInTheDocument();
    const table = screen.getByRole("table");
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map(({ textContent }) => textContent),
    ).toEqual(
      expect.arrayContaining(["Peak memory", "Peak disk", "Peak heap"]),
    );
    // 6 GiB of the replica's 8 GiB.
    expect(within(table).getAllByText("75.0%")).toHaveLength(3);
  });

  it("shows an empty progress bar for a replica that hasn't reported yet", async () => {
    // Relative to this test's start, as the fixtures' `NOW_MS` is not.
    const bootedAtMs = Date.now() - 30_000;
    await renderHydrationTab({
      statusRows: [online("u1", BOOTED_AT_MS), online("u2", bootedAtMs)],
      progressRows: [
        ["u1", "u10", true],
        ["u1", "u11", true],
      ],
    });

    const bar = await screen.findByRole("progressbar", {
      name: "r2 objects hydrated",
    });
    expect(bar).toHaveAttribute("aria-valuenow", "0");
    expect(bar).toHaveAttribute("aria-valuemax", "2");
    expect(screen.getByText("Hydrated so far: 0 of 2 objects")).toBeVisible();
    expect(screen.getByText("Starting")).toBeVisible();
    expect(await screen.findByText(/^3\d(\.\d+)?s so far/)).toBeVisible();
    // The progress card says it's starting, so there's no alert.
    expect(await screen.findByRole("table")).toBeVisible();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("reads a hydration Materialize hasn't recorded yet from its replica", async () => {
    // r2 ran out of memory, came back and hydrated, and the history doesn't
    // have that hydration yet.
    const bootedAtMs = NOW_MS - 19 * MINUTE_MS;
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        outOfMemory("u2", NOW_MS - 20 * MINUTE_MS),
        online("u2", bootedAtMs),
      ],
      replicaObjectRows: [
        {
          globalId: "u10",
          objectId: "u10",
          installedAt: String(bootedAtMs),
          startedAt: String(bootedAtMs),
          hydratedAt: String(bootedAtMs + 38_000),
        },
        {
          globalId: "u11",
          objectId: "u11",
          installedAt: String(bootedAtMs),
          startedAt: String(bootedAtMs + 10_000),
          hydratedAt: String(bootedAtMs + 280_000),
        },
      ],
    });

    expect(
      await screen.findByText(/^Read from r2, since Materialize hasn't/),
    ).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Last Hydration" }),
    ).toBeVisible();
    expect(screen.getByText(/^Full hydration on r2, /)).toBeVisible();
    const duration = screen.getByRole("group", { name: "Duration" });
    expect(within(duration).getByText("4m 40s")).toBeVisible();
    expect(within(duration).getByText("2 objects")).toBeVisible();
    const peakHeap = screen.getByRole("group", { name: "Peak heap" });
    expect(await within(peakHeap).findByText("96%")).toBeVisible();
    const waterfall = screen.getByRole("img", {
      name: "Object hydration times",
    });
    expect(
      within(waterfall).getByText("customer_ltv", { selector: "title" }),
    ).toBeInTheDocument();

    // A recorded hydration opens below it, as it isn't the last.
    const table = screen.getByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    await userEvent.click(within(rows[0]).getByRole("button"));
    expect(
      await screen.findByRole("heading", { name: "Selected Hydration" }),
    ).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Last Hydration" }),
    ).toBeVisible();
  });

  it("keeps the recorded last hydration when the replica can't be read", async () => {
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        outOfMemory("u2", NOW_MS - 20 * MINUTE_MS),
        online("u2", NOW_MS - 19 * MINUTE_MS),
      ],
      replicaObjectRows: "error",
    });

    expect(
      await screen.findByText(/^Additive hydration on r1, /),
    ).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Last Hydration" }),
    ).toBeVisible();
    await settleReplicaReads();
    expect(screen.queryByText(/^Read from r2/)).not.toBeInTheDocument();
  });

  it("keeps the recorded last hydration when a replica only blipped offline", async () => {
    // r2's status went offline and back, but its process kept running, so
    // its latest episode is the one recorded at boot.
    await renderHydrationTab({
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        offline("u2", NOW_MS - 20 * MINUTE_MS),
        online("u2", NOW_MS - 19 * MINUTE_MS),
      ],
      replicaObjectRows: [
        {
          globalId: "u10",
          objectId: "u10",
          installedAt: String(LATEST_FULL.startedAtMs),
          startedAt: String(LATEST_FULL.startedAtMs),
          hydratedAt: String(LATEST_FULL.startedAtMs + LATEST_FULL.durationMs),
        },
      ],
    });

    expect(
      await screen.findByText(/^Additive hydration on r1, /),
    ).toBeVisible();
    await settleReplicaReads();
    expect(screen.queryByText(/^Read from r2/)).not.toBeInTheDocument();
  });

  it("keeps a replica offline between restarts off the clock", async () => {
    await renderHydrationTab({
      offlineReplicaIds: ["u1"],
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        outOfMemory("u1", NOW_MS - 30 * MINUTE_MS),
        online("u1", NOW_MS - 29 * MINUTE_MS),
        outOfMemory("u1", NOW_MS - 10 * MINUTE_MS),
      ],
      progress: { u1: [3, 9], u2: [9, 9] },
    });

    expect(
      await screen.findByText(/^r1 has no finished hydration recorded since /),
    ).toBeVisible();
    expect(screen.getByText("Restarting")).toBeVisible();
    // Its last progress is from before it went offline.
    expect(
      screen.queryByRole("progressbar", { name: "r1 objects hydrated" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText(/^\d.* so far/)).not.toBeInTheDocument();
  });

  it("infers no restarts on environments without hydration history", async () => {
    await renderHydrationTab({
      environment: ENVIRONMENT_WITHOUT_HISTORY,
      statusRows: [
        online("u1", BOOTED_AT_MS),
        online("u2", BOOTED_AT_MS),
        outOfMemory("u1", NOW_MS - 36 * 60 * MINUTE_MS),
        online("u1", NOW_MS - 36 * 60 * MINUTE_MS + MINUTE_MS),
      ],
      progress: { u1: [8, 9], u2: [9, 9] },
    });

    expect(
      await screen.findByRole("progressbar", { name: "r1 objects hydrated" }),
    ).toBeVisible();
    expect(screen.queryByText(/no finished hydration/)).not.toBeInTheDocument();
    expect(screen.queryByText("Restarting")).not.toBeInTheDocument();
  });

  it("says the history failed when the replicas' status fails to load", async () => {
    await renderHydrationTab({ isStatusError: true });

    expect(
      await screen.findByText("Hydration history couldn't be loaded."),
    ).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("leaves out a new object's hydration time until its creation time loads", async () => {
    // Relative to this test's start, as the fixtures' `NOW_MS` is not.
    const createdAtMs = Date.now() - 30_000;
    await renderHydrationTab({
      progressRows: [
        ["u1", "u10", true],
        ["u1", "u11", true],
        ["u1", "u12", false],
        ["u2", "u10", true],
        ["u2", "u11", true],
        ["u2", "u12", true],
      ],
    });
    expect(await screen.findByRole("table")).toBeVisible();
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: clusterQueryKeys.objectCreationTimes({
            objectIds: ["u10", "u11", "u12"],
          }),
          results: mapKyselyToTabular({
            rows: [
              {
                id: "u12",
                objectType: "index",
                occurredAt: String(createdAtMs),
              },
            ],
            columns: [timestamptz("occurredAt")],
          }),
        },
        { waitTimeMs: 200 },
      ),
    );

    // The catalog has the new index before its creation time loads.
    act(() =>
      allObjectsCollection.applySnapshot({
        data: [
          catalogObject("u10", "orders_idx", "index"),
          catalogObject("u11", "customer_ltv", "materialized-view"),
          catalogObject("u12", "orders_by_day_idx", "index"),
        ],
        snapshotComplete: true,
        error: undefined,
      }),
    );

    expect(
      screen.getByRole("progressbar", { name: "r1 objects hydrated" }),
    ).toBeVisible();
    expect(screen.queryByText(/^\d.* so far/)).not.toBeInTheDocument();
    expect(await screen.findByText(/^3\d(\.\d+)?s so far/)).toBeVisible();
  });

  it("shows an offline replica rather than its last progress", async () => {
    await renderHydrationTab({ offlineReplicaIds: ["u2"] });

    expect(
      await screen.findByRole("heading", { name: "Hydration in Progress" }),
    ).toBeVisible();
    expect(screen.getByText("Offline")).toBeVisible();
    expect(
      screen.queryByRole("progressbar", { name: "r2 objects hydrated" }),
    ).not.toBeInTheDocument();
    // The progress card says it's offline, so there's no alert.
    expect(await screen.findByRole("table")).toBeVisible();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("says a cluster with no replicas has nothing hydrated", async () => {
    await renderHydrationTab({ hasReplicas: false });

    expect(
      await screen.findByText(
        "This cluster has no replicas, so nothing on it is hydrated.",
      ),
    ).toBeVisible();
    expect(
      screen.queryByRole("heading", { name: "Hydration in Progress" }),
    ).not.toBeInTheDocument();
  });

  it("says a cluster without indexes or materialized views has nothing to hydrate", async () => {
    allObjectsCollection.applySnapshot({
      data: [],
      snapshotComplete: true,
      error: undefined,
    });
    await renderHydrationTab({ progress: { u1: [0, 0], u2: [0, 0] } });

    expect(
      await screen.findByText(
        "This cluster has no indexes or materialized views, so it has nothing to hydrate.",
      ),
    ).toBeVisible();
    expect(await screen.findByRole("table")).toBeVisible();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Hydration in Progress" }),
    ).not.toBeInTheDocument();
  });

  it("says when the Console runs against the emulator", async () => {
    await renderHydrationTab({ appConfig: new SelfManagedAppConfig() });

    expect(await screen.findByRole("note")).toHaveTextContent(
      /^This is the Materialize Emulator\./,
    );
  });

  it("says nothing of the emulator on a self-managed install", async () => {
    await renderHydrationTab({
      appConfig: new SelfManagedAppConfig(),
      environment: HELM_INSTALL_ENVIRONMENT,
    });

    expect(await screen.findByRole("table")).toBeVisible();
    expect(screen.queryByRole("note")).not.toBeInTheDocument();
  });

  it("explains each kind of hydration from a tip that opens on keyboard focus", async () => {
    await renderHydrationTab();

    const tip = await screen.findByRole("img", {
      name: "About Additive hydration",
    });
    act(() => tip.focus());

    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      HYDRATION_HELP.additive,
    );
  });

  it("shows the history without waiting for live progress", async () => {
    await renderHydrationTab({ isProgressStuck: true });

    expect(await screen.findByRole("table")).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Last Hydration" }),
    ).toBeVisible();
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
    expect(screen.getByLabelText("Replica")).toHaveValue("r1");
  });

  it("offers a retired replica while its history is in the window", async () => {
    await renderHydrationTab({
      droppedReplicas: [
        {
          replicaId: "u0",
          name: "r1-burst",
          createdAtMs: NOW_MS - 3 * DAY_MS,
          droppedAtMs: NOW_MS - 2 * DAY_MS,
        },
      ],
      search: "?replica=r1-burst",
    });

    expect(
      await screen.findByRole("option", { name: "r1-burst · 100cc · retired" }),
    ).toBeInTheDocument();
    expect(screen.getByLabelText("Replica")).toHaveValue("r1-burst");
    expect(
      await screen.findByText("No hydrations in this period."),
    ).toBeVisible();
    // Its view is its history, with no alert about the current replicas.
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("keeps live progress when the history fails to load", async () => {
    await renderHydrationTab({
      isEpisodesError: true,
      progress: { u1: [3, 9], u2: [9, 9] },
    });

    expect(
      await screen.findByText("Hydration history couldn't be loaded."),
    ).toBeVisible();
    expect(
      await screen.findByRole("progressbar", { name: "r1 objects hydrated" }),
    ).toBeVisible();
  });

  it("waits for the catalog before classifying hydrations and counting new objects", async () => {
    allObjectsCollection.reset();
    await renderHydrationTab({ progress: { u1: [3, 9], u2: [9, 9] } });

    expect(
      await screen.findByRole("progressbar", { name: "r1 objects hydrated" }),
    ).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();

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
    const table = screen.getByRole("table");
    expect(
      within(table)
        .getAllByRole("row")
        .slice(1)
        .map((row) => within(row).getAllByRole("cell")[2].textContent),
    ).toEqual(["Additive", "Full", "Full"]);
  });

  it("shows the history and object timings when the catalog fails to load", async () => {
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
    // Without the catalog, objects go by their ids.
    const waterfall = await screen.findByRole("img", {
      name: "Object hydration times",
    });
    expect(
      within(waterfall).getByText("u11", { selector: "title" }),
    ).toBeInTheDocument();
  });

  it("keeps live progress on environments without hydration history", async () => {
    await renderHydrationTab({
      environment: ENVIRONMENT_WITHOUT_HISTORY,
      progress: { u1: [3, 9], u2: [9, 9] },
    });

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
