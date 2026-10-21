// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  act,
  fireEvent,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { sort } from "d3";
import { http, HttpResponse } from "msw";
import React from "react";
import { Route, Routes, useLocation } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";
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
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "~/platform/clusters/constants";
import {
  clusterQueryKeys,
  replicaStatusesUpdatedAt,
} from "~/platform/clusters/queries";
import { allClusters } from "~/store/allClusters";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { mockSubscribeSocket } from "~/test/mockSubscribeSocket";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { lightColors } from "~/theme/light";
import { formatDate, FRIENDLY_DATETIME_FORMAT } from "~/utils/dateFormat";
import { parseDbVersion } from "~/version/api";

import { ClusterMetrics } from "../ClusterMetrics";
import { ResourceUsage } from "./ResourceUsage";
import { CHART_MARGIN } from "./resourceUsageStyles";

// The Metrics page's other card has its own tests and queries.
vi.mock("~/platform/clusters/LargestMaintainedQueries", () => ({
  default: () => null,
}));

// jsdom lays nothing out, so give the charts a real width.
const WIDTH_PX = 900;
vi.mock("@visx/responsive", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@visx/responsive")>()),
  useParentSize: () => ({
    parentRef: () => {},
    width: WIDTH_PX,
    height: 0,
    resize: () => {},
  }),
}));
// jsdom has no SVG geometry, so read pointer positions straight off the event.
vi.mock("@visx/event", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@visx/event")>()),
  localPoint: (event: { clientX: number; clientY: number }) => ({
    x: event.clientX,
    y: event.clientY,
  }),
}));

const HOUR_MS = 60 * 60_000;
const DAY_MS = 24 * HOUR_MS;
// The 7-day window reads the polled hourly overview view, not a subscribe.
const TIME_PERIOD_MINUTES = 7 * 24 * 60;
const NOW_MS = Date.now();
const WINDOW_START_MS = NOW_MS - TIME_PERIOD_MINUTES * 60_000;
const FIRST_BUCKET_MS = Math.ceil(WINDOW_START_MS / HOUR_MS) * HOUR_MS;
const BUCKET_COUNT = 160;
const OOM_AT_MS = FIRST_BUCKET_MS + 100 * HOUR_MS + 10 * 60_000;
const DDL_AT_MS = FIRST_BUCKET_MS + 20 * HOUR_MS;
const GIB = 2 ** 30;
const HEAP_LIMIT_BYTES = 25 * GIB;
// One 100cc process's allocation. Its heap limit is its RAM plus 5 GiB of swap.
const ALLOCATION = {
  cpuNanoCores: 2e9,
  memoryBytes: 20 * GIB,
  diskBytes: 8 * GIB,
};
// `CpuLimit::MAX`, which a size without a CPU limit reports.
const UNLIMITED_CPU_NANO_CORES = "18446744073709000000";

interface CreatedObject {
  id: string;
  name: string;
  objectType: string;
  occurredAtMs: number;
}

const CREATED_INDEX: CreatedObject = {
  id: "u100",
  name: "orders_by_day_idx",
  objectType: "index",
  occurredAtMs: DDL_AT_MS,
};

const setClusterObjects = (objects: CreatedObject[]) =>
  allObjectsCollection.applySnapshot({
    data: objects.map(({ id, name, objectType }) => ({
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
      clusterName: "prod",
    })),
    snapshotComplete: true,
    error: undefined,
  });

const NEW_ENVIRONMENT = {
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion("v26.45.0 (0000000000)"),
  },
};

const REPLICA_NAMES: Record<string, string> = { u1: "r1", u2: "r2" };

const buildCluster = (replicaNames: Record<string, string>) =>
  buildClusterServerResponse(
    { id: "u9", name: "prod" },
    Object.entries(replicaNames).map(([id, name]) => ({
      id,
      name,
      size: "100cc",
      disk: false,
      statuses: [],
    })),
  );
// The status history query is keyed on it.
const STATUSES_UPDATED_AT = replicaStatusesUpdatedAt(buildCluster({}).replicas);
// A replica's status transitions are its process 0's.
const onProcessZero = <T extends object>(rows: T[]) =>
  rows.map((row) => ({ processId: "0", ...row }));

const timestamptz = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.timestamptz });
const float8 = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.float8 });

type Readings = Partial<
  Record<"maxCpuPercent" | "maxMemoryPercent" | "maxDiskPercent", number>
>;

// With swap, a sample holds 10 GiB of RAM and 5 GiB of swap, which counts as
// disk.
const utilizationRows = ({
  withSwap,
  withoutHeapLimit,
  emptyReadings,
  r2CpuPercent,
  readings,
  replicaIds,
  replicaNames,
}: {
  withSwap: boolean;
  withoutHeapLimit: boolean;
  emptyReadings: boolean;
  r2CpuPercent: number;
  readings?: Readings;
  replicaIds: string[];
  replicaNames: Record<string, string>;
}) =>
  replicaIds.flatMap((replicaId) =>
    Array.from({ length: BUCKET_COUNT }, (_, bucketIndex) => {
      const bucketStartMs = FIRST_BUCKET_MS + bucketIndex * HOUR_MS;
      const at = String(bucketStartMs);
      return {
        bucketStart: at,
        replicaId,
        maxMemoryPercent: emptyReadings ? null : 0.5,
        maxMemoryAt: at,
        maxDiskPercent: emptyReadings || !withSwap ? null : 0.625,
        maxDiskAt: at,
        maxCpuPercent: emptyReadings
          ? null
          : replicaId === "u1"
            ? 0.45
            : r2CpuPercent,
        maxCpuAt: at,
        maxHeapPercent: emptyReadings ? null : 0.6,
        maxHeapAt: at,
        maxMemoryAndDiskPercent: null,
        maxMemoryAndDiskMemoryPercent: null,
        maxMemoryAndDiskDiskPercent: null,
        maxMemoryAndDiskAt: at,
        offlineEvents: null,
        bucketEnd: String(bucketStartMs + HOUR_MS),
        name: replicaNames[replicaId],
        clusterId: "u9",
        size: "100cc",
        ...(withSwap ? { swapOfRamPercent: 0.25, heapLimitPercent: 1.25 } : {}),
        ...(withoutHeapLimit
          ? { swapOfRamPercent: null, heapLimitPercent: null }
          : {}),
        ...readings,
      };
    }),
  );

const UTILIZATION_COLUMNS = [
  ...[
    "bucketStart",
    "bucketEnd",
    "maxMemoryAt",
    "maxDiskAt",
    "maxCpuAt",
    "maxHeapAt",
    "maxMemoryAndDiskAt",
  ].map(timestamptz),
  ...[
    "maxMemoryPercent",
    "maxDiskPercent",
    "maxCpuPercent",
    "maxHeapPercent",
    "swapOfRamPercent",
    "heapLimitPercent",
  ].map(float8),
];

const sizeAllocationsHandler = ({
  processes,
  cpuNanoCores = String(ALLOCATION.cpuNanoCores),
}: {
  processes: number;
  cpuNanoCores?: string;
}) =>
  buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.replicaSizeAllocations({ sizes: ["100cc"] }),
    results: mapKyselyToTabular({
      columns: [
        buildColumn({ name: "size" }),
        ...["processes", "cpuNanoCores", "memoryBytes", "diskBytes"].map(
          float8,
        ),
      ],
      rows: [
        {
          size: "100cc",
          processes: String(processes),
          cpuNanoCores,
          memoryBytes: String(ALLOCATION.memoryBytes),
          diskBytes: String(ALLOCATION.diskBytes),
        },
      ],
    }),
  });

const registerHandlers = ({
  withSwap,
  withoutHeapLimit = false,
  r2CpuPercent = 0.9,
  readings,
  processes = 1,
  cpuNanoCores,
  r2OfflineAllWindow = false,
  emptyReadings = false,
  statusRows,
  statusDelayMs,
  r2UnhydratedObjectIds = [],
  utilizationError = false,
  utilizationRowCount,
  createdObjects = [CREATED_INDEX],
  objectHydrations = [],
  replicaNames,
}: {
  withSwap: boolean;
  /** Rows carry the memory breakdown, but no sample has a heap limit. */
  withoutHeapLimit?: boolean;
  r2CpuPercent?: number;
  /** Replaces every replica's readings. */
  readings?: Readings;
  /** How many processes the replicas' size runs. */
  processes?: number;
  cpuNanoCores?: string;
  /** r2 went offline before the window and reports no samples in it. */
  r2OfflineAllWindow?: boolean;
  /** Every sample has empty readings, as a down replica reports in cloud. */
  emptyReadings?: boolean;
  /** Replaces the replica status history. */
  statusRows?: Array<{
    replicaId: string;
    occurredAt: string;
    status: string;
    reason: string | null;
  }>;
  statusDelayMs?: number;
  r2UnhydratedObjectIds?: string[];
  utilizationError?: boolean;
  utilizationRowCount?: number;
  /** Indexes and materialized views on the cluster, with creation times. */
  createdObjects?: CreatedObject[];
  /** Each replica's hydration time for the created objects. */
  objectHydrations?: Array<{
    replicaId: string;
    objectId: string;
    hydrationMs: number | null;
  }>;
  replicaNames: Record<string, string>;
}) => {
  setClusterObjects(createdObjects);
  const utilizationKey = clusterQueryKeys.replicaUtilizationHistory({
    bucketSizeMs: TIME_PERIOD_MINUTES * 1000,
    timePeriodMinutes: TIME_PERIOD_MINUTES,
    clusterIds: ["u9"],
    replicaId: undefined,
    includeMemoryBreakdown: withSwap || withoutHeapLimit || undefined,
  });
  const rows = utilizationRows({
    withSwap,
    withoutHeapLimit,
    emptyReadings,
    r2CpuPercent,
    readings,
    replicaIds: r2OfflineAllWindow ? ["u1"] : ["u1", "u2"],
    replicaNames,
  }).slice(0, utilizationRowCount);
  server.use(
    sizeAllocationsHandler({ processes, cpuNanoCores }),
    buildSqlQueryHandlerV2({
      queryKey: [...utilizationKey, "deploymentLineage"],
      results: mapKyselyToTabular({
        rows: [],
        columns: buildColumns([
          "clusterId",
          "currentDeploymentClusterId",
          "clusterName",
        ]),
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: utilizationKey,
      results: utilizationError
        ? {
            notices: [],
            error: {
              message: "Something went wrong",
              code: ErrorCode.INTERNAL_ERROR,
            },
          }
        : mapKyselyToTabular({
            rows,
            columns:
              rows.length > 0
                ? UTILIZATION_COLUMNS
                : buildColumns(["bucketStart"]),
          }),
    }),
    buildSqlQueryHandlerV2(
      {
        queryKey: clusterQueryKeys.replicaStatusHistory({
          replicaIds: ["u1", "u2"],
          statusesUpdatedAt: STATUSES_UPDATED_AT,
        }),
        results: mapKyselyToTabular({
          rows: onProcessZero(
            statusRows ??
              (r2OfflineAllWindow
                ? [
                    {
                      replicaId: "u1",
                      occurredAt: String(WINDOW_START_MS - DAY_MS),
                      status: "online",
                      reason: null,
                    },
                    {
                      replicaId: "u2",
                      occurredAt: String(WINDOW_START_MS - DAY_MS),
                      status: "offline",
                      reason: null,
                    },
                  ]
                : [
                    {
                      replicaId: "u1",
                      occurredAt: String(WINDOW_START_MS - DAY_MS),
                      status: "online",
                      reason: null,
                    },
                    {
                      replicaId: "u2",
                      occurredAt: String(WINDOW_START_MS - DAY_MS),
                      status: "online",
                      reason: null,
                    },
                    {
                      replicaId: "u2",
                      occurredAt: String(OOM_AT_MS),
                      status: "offline",
                      reason: "oom-killed",
                    },
                    {
                      replicaId: "u2",
                      occurredAt: String(OOM_AT_MS + 5 * 60_000),
                      status: "online",
                      reason: null,
                    },
                  ]),
          ),
          columns: [timestamptz("occurredAt")],
        }),
      },
      { waitTimeMs: statusDelayMs },
    ),
    // r2 came back online inside the window, so its hydration is looked up.
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.unhydratedComputeObjects({
        replicaIds: ["u1", "u2"],
      }),
      results: mapKyselyToTabular({
        rows: r2UnhydratedObjectIds.map((objectId) => ({
          replicaId: "u2",
          objectId,
        })),
        columns: buildColumns(["replicaId", "objectId"]),
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaHydrationEpisodes({
        replicaIds: ["u2"],
        timePeriodMinutes: TIME_PERIOD_MINUTES,
      }),
      results: mapKyselyToTabular({
        rows: [
          {
            replicaId: "u2",
            startedAt: String(OOM_AT_MS + 5 * 60_000),
            finishedAt: String(OOM_AT_MS + 30 * 60_000),
          },
        ],
        columns: [timestamptz("startedAt"), timestamptz("finishedAt")],
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
          heapLimit: withoutHeapLimit ? null : String(HEAP_LIMIT_BYTES),
        })),
        columns: buildColumns(["replicaId", "processId", "heapLimit"]),
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.objectCreationTimes({
        objectIds: createdObjects.map(({ id }) => id).sort(),
        timePeriodMinutes: TIME_PERIOD_MINUTES,
      }),
      results: mapKyselyToTabular({
        rows: createdObjects.map(({ id, occurredAtMs }) => ({
          id,
          occurredAt: String(occurredAtMs),
        })),
        columns: [timestamptz("occurredAt")],
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.objectHydrationTimes({
        replicaIds: ["u1", "u2"],
        objects: sort(createdObjects, ({ id }) => id).map(
          ({ id, occurredAtMs }) => ({ id, createdAtMs: occurredAtMs }),
        ),
      }),
      results: [
        mapKyselyToTabular({
          rows: objectHydrations,
          columns: [float8("hydrationMs")],
        }),
        mapKyselyToTabular({
          rows: [{ startedAt: String(WINDOW_START_MS - DAY_MS) }],
          columns: [timestamptz("startedAt")],
        }),
      ],
    }),
  );
};

// Query keys embed the environment version, so set the environment before
// building the handlers that match on them.
const renderResourceUsage = async ({
  environment = NEW_ENVIRONMENT,
  replicaNames = REPLICA_NAMES,
  selectedReplicaId,
  ...handlerOptions
}: Omit<Parameters<typeof registerHandlers>[0], "replicaNames"> & {
  environment?: typeof healthyEnvironment;
  replicaNames?: Record<string, string>;
  selectedReplicaId?: string;
}) => {
  await setFakeEnvironment(getStore().set, "aws/us-east-1", environment);
  registerHandlers({ ...handlerOptions, replicaNames });
  return renderComponent(
    <ResourceUsage
      cluster={buildCluster(replicaNames)}
      timePeriodMinutes={TIME_PERIOD_MINUTES}
      selectedReplicaId={selectedReplicaId}
    />,
    {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", environment),
    },
  );
};

const LocationSearch = () => (
  <div data-testid="location-search">{useLocation().search}</div>
);

// The whole Metrics page, whose toolbar picks the replica and time period.
const renderClusterMetrics = async (search = "", processes = 1) => {
  await setFakeEnvironment(getStore().set, "aws/us-east-1", NEW_ENVIRONMENT);
  registerHandlers({ withSwap: true, replicaNames: REPLICA_NAMES, processes });
  getStore().set(
    allClusters,
    mockSubscribeState({ data: [buildCluster(REPLICA_NAMES)] }),
  );
  return renderComponent(
    <Routes>
      <Route
        path=":clusterId/:clusterName"
        element={
          <>
            <ClusterMetrics />
            <LocationSearch />
          </>
        }
      />
    </Routes>,
    {
      initialRouterEntries: [`/u9/prod${search}`],
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", NEW_ENVIRONMENT),
    },
  );
};

// Hovers the memory chart at a time inside the selected window.
const hoverAt = async (timeMs: number) => {
  const svg = await screen.findByRole("img", { name: "Memory usage" });
  const overlay = svg.querySelector('rect[fill="transparent"]');
  if (!overlay) throw new Error("hover overlay not rendered");
  const plotWidthPx = WIDTH_PX - CHART_MARGIN.left - CHART_MARGIN.right;
  const clientX =
    CHART_MARGIN.left +
    ((timeMs - WINDOW_START_MS) / (NOW_MS - WINDOW_START_MS)) * plotWidthPx;
  fireEvent.pointerMove(overlay, { clientX, clientY: 80 });
  return screen.findByTestId("chart-tooltip");
};

describe("ResourceUsage", () => {
  beforeAll(() => {
    // jsdom has no PointerEvent, and the fallback Event drops clientX.
    if (!window.PointerEvent) {
      window.PointerEvent = class extends MouseEvent {} as typeof PointerEvent;
    }
  });

  beforeEach(() => {
    window.localStorage.setItem(
      "mz-cluster-graph-time-period",
      String(TIME_PERIOD_MINUTES),
    );
    setClusterObjects([CREATED_INDEX]);
  });

  it("shows each replica's CPU, memory and disk for the hovered time against its own allocation, and the total against the heap limit", async () => {
    await renderResourceUsage({ withSwap: true });

    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    // A 25 GB heap limit, 60% full: 10 GB of RAM and 5 GB of swap.
    expect(tooltip).toHaveTextContent(
      /r1.*Running.*CPU0\.9 cores · 45\.0%.*Memory10 GB · 50\.0% of 20 GB.*Disk5 GB · 62\.5% of 8 GB.*Total15 GB · 60\.0% of heap limit/,
    );
    expect(tooltip).toHaveTextContent(/r2.*Running.*CPU1\.8 cores · 90\.0%/);
  });

  it("reads a multi-process replica's memory and disk against its whole allocation, as the Clusters list does", async () => {
    // As `mz_console_cluster_utilization_overview_3h`, which the Clusters list
    // reads, computes a share: summed over processes, against one process's
    // allocation times the process count.
    const overviewShare = (perProcess: number[], allocation: number) =>
      perProcess.reduce((sum, value) => sum + value, 0) /
      (allocation * perProcess.length);
    await renderResourceUsage({
      withSwap: true,
      processes: 2,
      readings: {
        maxCpuPercent: overviewShare([1.5e9, 0.3e9], ALLOCATION.cpuNanoCores),
        maxMemoryPercent: overviewShare(
          [14 * GIB, 6 * GIB],
          ALLOCATION.memoryBytes,
        ),
        maxDiskPercent: overviewShare([4 * GIB, 2 * GIB], ALLOCATION.diskBytes),
      },
    });

    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(
      /r1.*CPU1\.8 cores · 45\.0%.*Memory20 GB · 50\.0% of 40 GB.*Disk6 GB · 37\.5% of 16 GB/,
    );
  });

  it("reads an idle disk as empty, not missing", async () => {
    // A 0% reading arrives as null.
    await renderResourceUsage({
      withSwap: true,
      readings: { maxDiskPercent: 0 },
    });

    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(/r1.*Disk0 B · 0\.0% of 8 GB/);
  });

  it("leaves cores out for a size without a CPU limit", async () => {
    await renderResourceUsage({
      withSwap: true,
      cpuNanoCores: UNLIMITED_CPU_NANO_CORES,
    });

    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(/r1.*CPU45\.0%Memory10 GB/);
    expect(tooltip).not.toHaveTextContent("cores");
  });

  it("marks an out-of-memory kill in the tooltip", async () => {
    await renderResourceUsage({ withSwap: true });

    const tooltip = await hoverAt(OOM_AT_MS);
    expect(tooltip).toHaveTextContent(/r2.*Out of Memory/);
  });

  it("labels the memory limits with their sizes", async () => {
    await renderResourceUsage({ withSwap: true });

    expect(await screen.findByText("heap limit · 25 GB")).toBeVisible();
    // The heap limit is 125% of memory, so memory ends at 80% of it.
    expect(screen.getByText("memory limit · 20 GB")).toBeVisible();
    expect(screen.getByText("disk")).toBeVisible();
  });

  it("shows heap alone on environments without the swap columns", async () => {
    await renderResourceUsage({
      withSwap: false,
      environment: healthyEnvironment,
    });

    expect(await screen.findByText("heap")).toBeVisible();
    expect(screen.queryByText("disk")).not.toBeInTheDocument();
    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(
      /r1.*CPU0\.9 cores · 45\.0%.*Heap15 GB · 60\.0%/,
    );
    expect(tooltip).not.toHaveTextContent("Disk");
    expect(tooltip).not.toHaveTextContent("Total");
  });

  it("labels memory as a share of the allocation when no sample has a heap limit", async () => {
    await renderResourceUsage({ withSwap: false, withoutHeapLimit: true });

    expect(await screen.findByText("memory limit")).toBeVisible();
    expect(screen.queryByText(/heap/i)).not.toBeInTheDocument();
    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(
      /r1.*CPU0\.9 cores · 45\.0%.*Memory10 GB · 50\.0% of 20 GB/,
    );
    expect(tooltip).not.toHaveTextContent(/heap/i);
  });

  it("extends the CPU axis past 100% when a replica exceeds its allocation", async () => {
    await renderResourceUsage({ withSwap: true, r2CpuPercent: 2.5 });

    const cpu = await screen.findByRole("img", { name: "CPU usage" });
    expect(within(cpu).getByText("250%")).toBeInTheDocument();
    expect(screen.getByText("CPU allocation")).toBeVisible();
    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(/r2.*CPU5\.0 cores · 250\.0%/);
  });

  it("keeps the CPU axis at 100% while replicas stay within their allocation", async () => {
    await renderResourceUsage({ withSwap: true });

    const cpu = await screen.findByRole("img", { name: "CPU usage" });
    expect(within(cpu).getByText("100%")).toBeInTheDocument();
    expect(screen.queryByText("CPU allocation")).not.toBeInTheDocument();
  });

  it("shows a status row for a replica offline the whole window", async () => {
    await renderResourceUsage({ withSwap: true, r2OfflineAllWindow: true });

    const status = await screen.findByRole("img", {
      name: "Replica status over time",
    });
    expect(within(status).getByText("r1")).toBeInTheDocument();
    expect(within(status).getByText("r2")).toBeInTheDocument();
  });

  it("truncates a long replica name in its status row, keeping the full name as its title", async () => {
    await renderResourceUsage({
      withSwap: true,
      replicaNames: { u1: "r1", u2: "blue_green_r2" },
    });

    const status = await screen.findByRole("img", {
      name: "Replica status over time",
    });
    expect(within(status).getByText("blue_…")).toBeInTheDocument();
    expect(within(status).getByText("r1")).toBeInTheDocument();
    // An SVG `title` is the browser's tooltip for its parent.
    expect(
      [...status.querySelectorAll("title")].map((title) => [
        title.textContent,
        title.parentElement?.textContent,
      ]),
    ).toEqual([["blue_green_r2", "blue_green_r2blue_…"]]);
  });

  it("orders the status rows like the replica pills", async () => {
    // Display order is by name, the reverse of id order here.
    await renderResourceUsage({
      withSwap: true,
      replicaNames: { u1: "zeta", u2: "alpha" },
    });

    const status = await screen.findByRole("img", {
      name: "Replica status over time",
    });
    const pills = screen.getByRole("list", { name: "Replicas" });
    const pillNames = within(pills)
      .getAllByText(/^(alpha|zeta)$/)
      .map((label) => label.textContent);
    const rowNames = within(status)
      .getAllByText(/^(alpha|zeta)$/)
      .map((label) => label.textContent);
    expect(pillNames).toEqual(["alpha", "zeta"]);
    expect(rowNames).toEqual(pillNames);
  });

  it("shows how many objects a replica has left to hydrate", async () => {
    await renderResourceUsage({
      withSwap: true,
      r2UnhydratedObjectIds: ["u100", "u101"],
    });

    const status = await screen.findByRole("img", {
      name: "Replica status over time",
    });
    expect(
      await within(status).findByText("Hydrating · 2 objects"),
    ).toBeInTheDocument();
  });

  it("hides the other replicas when one replica's toggle is clicked", async () => {
    const user = userEvent.setup();
    await renderResourceUsage({ withSwap: true });

    const pills = await screen.findByRole("list", { name: "Replicas" });
    await user.click(within(pills).getByRole("button", { name: /r1/ }));

    expect(within(pills).getByRole("button", { name: /r1/ })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(within(pills).getByRole("button", { name: /r2/ })).toHaveAttribute(
      "aria-pressed",
      "false",
    );
    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent("r1");
    expect(tooltip).not.toHaveTextContent("r2");
  });

  const findObjectsLane = () =>
    screen.findByRole("group", { name: "Objects created" });

  it("marks an index created on the cluster in the Objects Created row, and guides the charts to it only on hover", async () => {
    await renderResourceUsage({ withSwap: true });

    const tick = await within(await findObjectsLane()).findByRole("img", {
      name: /^1 object created, /,
    });
    const cpu = screen.getByRole("img", { name: "CPU usage" });
    const memory = screen.getByRole("img", { name: "Memory usage" });
    expect(
      within(cpu).queryByText(/orders_by_day_idx/),
    ).not.toBeInTheDocument();
    expect(within(cpu).queryByTestId("change-guide")).not.toBeInTheDocument();

    await userEvent.hover(tick);
    const tooltip = await screen.findByTestId("chart-tooltip");
    expect(tooltip).toHaveTextContent(
      `${formatDate(DDL_AT_MS, FRIENDLY_DATETIME_FORMAT)}1 object createdIndexorders_by_day_idx`,
    );
    expect(within(cpu).getByTestId("change-guide")).toBeInTheDocument();
    expect(within(memory).getByTestId("change-guide")).toBeInTheDocument();

    await userEvent.unhover(tick);
    expect(screen.queryByTestId("chart-tooltip")).not.toBeInTheDocument();
    expect(within(cpu).queryByTestId("change-guide")).not.toBeInTheDocument();
  });

  it("merges objects created close together into one tick that names the first few", async () => {
    // Seven views a minute apart share a pixel of the week; the index is days
    // later.
    const views = Array.from({ length: 7 }, (_, index) => ({
      id: `u${200 + index}`,
      name: `view_${index}`,
      objectType: "materialized-view",
      occurredAtMs: DDL_AT_MS + index * 60_000,
    }));
    await renderResourceUsage({
      withSwap: true,
      createdObjects: [
        ...views,
        { ...CREATED_INDEX, occurredAtMs: DDL_AT_MS + 3 * DAY_MS },
      ],
    });

    const lane = await findObjectsLane();
    await waitFor(() =>
      expect(within(lane).getAllByRole("img")).toHaveLength(2),
    );
    const burst = within(lane).getByRole("img", {
      name: /^7 objects created, /,
    });
    act(() => burst.focus());
    const tooltip = await screen.findByTestId("chart-tooltip");
    expect(tooltip).toHaveTextContent(
      /7 objects created(Materialized viewview_\d){5}and 2 more/,
    );
    expect(tooltip).not.toHaveTextContent("view_5");
    expect(
      within(screen.getByRole("img", { name: "CPU usage" })).getByTestId(
        "change-guide",
      ),
    ).toBeInTheDocument();

    act(() => burst.blur());
    expect(screen.queryByTestId("chart-tooltip")).not.toBeInTheDocument();
  });

  it("keeps the Objects Created row, headed and with its legend, when nothing was created in the window", async () => {
    await renderResourceUsage({ withSwap: true, createdObjects: [] });

    const lane = await findObjectsLane();
    expect(within(lane).queryAllByRole("img")).toHaveLength(0);
    expect(screen.getByText("Objects Created")).toBeVisible();
    expect(
      screen.getByText("index or materialized view created"),
    ).toBeVisible();
    expect(screen.queryByText("Changes")).not.toBeInTheDocument();
  });

  it("draws a brief hydration wide enough to see", async () => {
    // r2's 25-minute hydration after its restart spans about 2px of the week.
    await renderResourceUsage({ withSwap: true });

    const status = await screen.findByRole("img", {
      name: "Replica status over time",
    });
    await waitFor(() => {
      const hydrating = [...status.querySelectorAll("rect")].filter(
        (rect) => rect.getAttribute("fill") === lightColors.accent.darkYellow,
      );
      expect(hydrating).toHaveLength(1);
      expect(Number(hydrating[0].getAttribute("width"))).toBe(4);
    });
  });

  it("draws an object's hydration on a replica that was already running", async () => {
    await renderResourceUsage({
      withSwap: true,
      objectHydrations: [
        {
          replicaId: "u1",
          objectId: CREATED_INDEX.id,
          hydrationMs: 6 * HOUR_MS,
        },
      ],
    });

    const status = await screen.findByRole("img", {
      name: "Replica status over time",
    });
    // r2's brief hydration after its restart, and r1's of the new index.
    await waitFor(() => {
      const widths = [...status.querySelectorAll("rect")]
        .filter(
          (rect) => rect.getAttribute("fill") === lightColors.accent.darkYellow,
        )
        .map((rect) => Number(rect.getAttribute("width")));
      expect(widths).toHaveLength(2);
      expect(Math.max(...widths)).toBeGreaterThan(4);
    });
    expect(
      screen.queryByText("Hydration history is unavailable."),
    ).not.toBeInTheDocument();
  });

  it("says replicas ran unmeasured when the window has no readings", async () => {
    await renderResourceUsage({ withSwap: true, utilizationRowCount: 0 });

    expect(
      await screen.findByText(
        /Your replicas were running, but no resource usage was recorded/,
      ),
    ).toBeVisible();
    expect(screen.getByRole("link", { name: "contact support" })).toBeVisible();
    expect(
      await screen.findByRole("img", { name: "Replica status over time" }),
    ).toBeInTheDocument();
  });

  it("explains a window where every replica was down after running out of memory", async () => {
    await renderResourceUsage({
      withSwap: true,
      emptyReadings: true,
      statusRows: [
        {
          replicaId: "u1",
          occurredAt: String(WINDOW_START_MS - DAY_MS),
          status: "offline",
          reason: null,
        },
        {
          replicaId: "u2",
          occurredAt: String(WINDOW_START_MS - HOUR_MS),
          status: "offline",
          reason: "oom-killed",
        },
      ],
    });

    expect(
      await screen.findByText(
        /All replicas were offline for this period. They last went offline after running out of memory/,
      ),
    ).toBeVisible();
    expect(
      screen.getByRole("link", { name: "Troubleshoot memory usage" }),
    ).toBeVisible();
    const status = screen.getByRole("img", {
      name: "Replica status over time",
    });
    expect(within(status).getByText("r2")).toBeInTheDocument();
    expect(screen.queryByText(/Cluster metrics are unavailable/)).toBeNull();
  });

  it("says when every replica was down without a recorded reason", async () => {
    await renderResourceUsage({
      withSwap: true,
      emptyReadings: true,
      statusRows: ["u1", "u2"].map((replicaId) => ({
        replicaId,
        occurredAt: String(WINDOW_START_MS - DAY_MS),
        status: "offline",
        reason: null,
      })),
    });

    expect(
      await screen.findByText(/Materialize didn.t record why/),
    ).toBeVisible();
    expect(
      screen.getByRole("link", { name: "Troubleshoot hydration failures" }),
    ).toBeVisible();
  });

  it("waits for replica status before explaining a window without readings", async () => {
    await renderResourceUsage({
      withSwap: true,
      emptyReadings: true,
      statusDelayMs: 300,
      statusRows: ["u1", "u2"].map((replicaId) => ({
        replicaId,
        occurredAt: String(WINDOW_START_MS - DAY_MS),
        status: "offline",
        reason: "oom-killed",
      })),
    });

    // `waitFor` reruns on every DOM change, so it sees the fallback message
    // even if it renders only until status arrives.
    let sawFallback = false;
    await waitFor(() => {
      sawFallback ||= screen.queryByText(/metrics may be unavailable/) !== null;
      expect(
        screen.getByText(/They last went offline after running out of memory/),
      ).toBeVisible();
    });
    expect(sawFallback).toBe(false);
  });

  it("says it can't tell why without replica status", async () => {
    await renderResourceUsage({
      withSwap: true,
      emptyReadings: true,
      statusRows: [],
    });

    expect(
      await screen.findByText(
        /No resource usage was recorded for this period. The replicas may have been offline, or metrics may be unavailable/,
      ),
    ).toBeVisible();
  });

  it("overlaps the replicas' memory bars at each time", async () => {
    await renderResourceUsage({ withSwap: true });

    const memory = await screen.findByRole("img", { name: "Memory usage" });
    const bars = [...memory.querySelectorAll("rect")]
      .filter((rect) => rect.getAttribute("fill") !== "transparent")
      .map((rect) => ({
        x: Number(rect.getAttribute("x")),
        width: Number(rect.getAttribute("width")),
      }))
      .sort((a, b) => a.x - b.x);
    const first = bars[0];
    const next = bars.find((bar) => bar.x > first.x);
    expect(next?.x).toBeLessThan(first.x + first.width);
  });

  // Each replica's hourly rows from the week fixture, starting and ending
  // where `keep` says, for the given cluster.
  const replicaRows = (
    replicaId: string,
    name: string,
    keep: (bucketStartMs: number) => boolean,
    clusterId = "u9",
  ) =>
    utilizationRows({
      withSwap: true,
      withoutHeapLimit: false,
      emptyReadings: false,
      r2CpuPercent: 0.9,
      replicaIds: [replicaId],
      replicaNames: { [replicaId]: name },
    })
      .filter(({ bucketStart }) => keep(Number(bucketStart)))
      .map((row) => ({ ...row, clusterId }));

  const renderWeek = async ({
    rows,
    replicaIds,
    currentReplicas = {},
  }: {
    rows: ReturnType<typeof utilizationRows>;
    replicaIds: string[];
    currentReplicas?: Record<string, string>;
  }) => {
    await setFakeEnvironment(getStore().set, "aws/us-east-1", NEW_ENVIRONMENT);
    const utilizationKey = clusterQueryKeys.replicaUtilizationHistory({
      bucketSizeMs: TIME_PERIOD_MINUTES * 1000,
      timePeriodMinutes: TIME_PERIOD_MINUTES,
      clusterIds: ["u9"],
      replicaId: undefined,
      includeMemoryBreakdown: true,
    });
    server.use(
      sizeAllocationsHandler({ processes: 1 }),
      buildSqlQueryHandlerV2({
        queryKey: [...utilizationKey, "deploymentLineage"],
        results: mapKyselyToTabular({
          rows: [],
          columns: buildColumns([
            "clusterId",
            "currentDeploymentClusterId",
            "clusterName",
          ]),
        }),
      }),
      buildSqlQueryHandlerV2({
        queryKey: utilizationKey,
        results: mapKyselyToTabular({ rows, columns: UTILIZATION_COLUMNS }),
      }),
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.replicaStatusHistory({
          replicaIds: [...replicaIds].sort(),
          statusesUpdatedAt: STATUSES_UPDATED_AT,
        }),
        results: mapKyselyToTabular({
          rows: [],
          columns: [
            ...buildColumns(["replicaId", "status", "reason"]),
            timestamptz("occurredAt"),
          ],
        }),
      }),
    );
    renderComponent(
      <ResourceUsage
        cluster={buildCluster(currentReplicas)}
        timePeriodMinutes={TIME_PERIOD_MINUTES}
      />,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", NEW_ENVIRONMENT),
      },
    );
    return screen.findByRole("img", { name: "CPU usage" });
  };

  it("charts a scheduled cluster's refreshes as one replica", async () => {
    // Replication factor 0 between refreshes, and a new replica id under the
    // same name every 12h, up for 6 hourly buckets each time. Six hours span at
    // least two of the 3h buckets the week merges into, however they align.
    const refreshIds = Array.from(
      { length: 13 },
      (_, index) => `u${index + 10}`,
    );
    const refreshStartMs = (index: number) =>
      FIRST_BUCKET_MS + index * 12 * HOUR_MS;
    const cpu = await renderWeek({
      rows: refreshIds.flatMap((replicaId, index) =>
        replicaRows(
          replicaId,
          "r1",
          (startMs) =>
            startMs >= refreshStartMs(index) &&
            startMs < refreshStartMs(index) + 6 * HOUR_MS,
        ),
      ),
      replicaIds: refreshIds,
    });

    expect(
      within(screen.getByRole("list", { name: "Replicas" }))
        .getAllByRole("button")
        .map((button) => button.textContent),
    ).toEqual(["+1 past replica"]);
    const status = await screen.findByRole("img", {
      name: "Replica status over time",
    });
    expect(within(status).getAllByText("r1")).toHaveLength(1);
    // Every refresh's line has more than one point.
    const lines = [...cpu.querySelectorAll("path")].filter((path) =>
      path.getAttribute("d")?.includes("L"),
    );
    expect(lines).toHaveLength(refreshIds.length);
    const memory = screen.getByRole("img", { name: "Memory usage" });
    const barWidths = [...memory.querySelectorAll("rect")]
      .filter((rect) => rect.getAttribute("fill") !== "transparent")
      .map((rect) => Number(rect.getAttribute("width")));
    expect(barWidths.length).toBeGreaterThan(0);
    // No bar is split with replicas that weren't running at its time: each
    // spans at least one hourly bucket, less the 2px gap between times.
    const plotWidthPx = WIDTH_PX - CHART_MARGIN.left - CHART_MARGIN.right;
    const hourPx = (plotWidthPx * HOUR_MS) / (TIME_PERIOD_MINUTES * 60_000);
    expect(Math.min(...barWidths)).toBeGreaterThan(hourPx - 2 - 0.01);

    // Inside one refresh, the tooltip has one section for the name.
    const tooltip = await hoverAt(refreshStartMs(3) + 2 * HOUR_MS);
    expect(within(tooltip).getAllByText("r1")).toHaveLength(1);
  });

  it("draws a refresh that fits in one merged bucket as a dot", async () => {
    // Three hourly buckets starting on a 3h boundary merge into one of the
    // week's 3h buckets, a single reading with no line to draw.
    const alignedMs = Math.ceil(FIRST_BUCKET_MS / (3 * HOUR_MS)) * 3 * HOUR_MS;
    const lastMs = FIRST_BUCKET_MS + BUCKET_COUNT * HOUR_MS;
    const refreshIds = Array.from(
      {
        length: Math.floor((lastMs - alignedMs - 3 * HOUR_MS) / (6 * HOUR_MS)),
      },
      (_, index) => `u${index + 10}`,
    );
    const cpu = await renderWeek({
      rows: refreshIds.flatMap((replicaId, index) =>
        replicaRows(
          replicaId,
          "r1",
          (startMs) =>
            startMs >= alignedMs + index * 6 * HOUR_MS &&
            startMs < alignedMs + (index * 6 + 3) * HOUR_MS,
        ),
      ),
      replicaIds: refreshIds,
    });

    expect(cpu.querySelectorAll("circle")).toHaveLength(refreshIds.length);
  });

  it("keeps a past blue-green deployment's replica apart from the current one", async () => {
    // The past deployment's r1 ran alongside the current r1 while it hydrated.
    await renderWeek({
      rows: [
        ...replicaRows("u1", "r1", () => true),
        ...replicaRows(
          "u5",
          "r1",
          (startMs) => startMs < FIRST_BUCKET_MS + 80 * HOUR_MS,
          "u8",
        ),
      ],
      replicaIds: ["u1", "u5"],
      currentReplicas: { u1: "r1" },
    });

    expect(
      within(screen.getByRole("list", { name: "Replicas" }))
        .getAllByRole("button")
        .map((button) => button.textContent),
    ).toEqual(["r1100cc", "+1 past replica"]);
    const status = await screen.findByRole("img", {
      name: "Replica status over time",
    });
    expect(within(status).getAllByText("r1")).toHaveLength(2);
  });

  describe("with many past blue-green deployments", () => {
    // Each deploy is a new cluster, whose r1 ran a day before the next one's.
    const PAST_IDS = ["u5", "u6", "u7", "u8"];
    const renderDeployments = () =>
      renderWeek({
        rows: [
          ...replicaRows("u1", "r1", () => true),
          ...PAST_IDS.flatMap((replicaId, index) =>
            replicaRows(
              replicaId,
              "r1",
              (startMs) =>
                startMs >= FIRST_BUCKET_MS + index * DAY_MS &&
                startMs < FIRST_BUCKET_MS + (index + 1) * DAY_MS,
              `u${50 + index}`,
            ),
          ),
        ],
        replicaIds: ["u1", ...PAST_IDS],
        currentReplicas: { u1: "r1" },
      });
    const statusRowCount = async () =>
      within(
        await screen.findByRole("img", { name: "Replica status over time" }),
      ).getAllByText("r1").length;

    it("folds them into one toggle that names them, and never wraps a label", async () => {
      await renderDeployments();

      const toggles = within(
        screen.getByRole("list", { name: "Replicas" }),
      ).getAllByRole("button");
      expect(toggles.map((button) => button.textContent)).toEqual([
        "r1100cc",
        "+4 past replicas",
      ]);
      for (const toggle of toggles) {
        expect(toggle).toHaveStyle({ whiteSpace: "nowrap" });
      }

      await userEvent.hover(toggles[1]);
      const tooltip = await screen.findByRole("tooltip");
      expect(within(tooltip).getAllByRole("listitem")).toHaveLength(4);
      expect(tooltip).toHaveTextContent(/^(r1100cc){4}$/);
    });

    it("shows or hides them together", async () => {
      await renderDeployments();
      const pastToggle = screen.getByRole("button", {
        name: "+4 past replicas",
      });
      expect(await statusRowCount()).toBe(5);

      fireEvent.click(pastToggle, { ctrlKey: true });
      expect(pastToggle).toHaveAttribute("aria-pressed", "false");
      expect(await statusRowCount()).toBe(1);

      fireEvent.click(pastToggle, { metaKey: true });
      expect(pastToggle).toHaveAttribute("aria-pressed", "true");
      expect(await statusRowCount()).toBe(5);
    });

    it("works from the keyboard", async () => {
      const user = userEvent.setup();
      await renderDeployments();

      await user.tab();
      await user.tab();
      const pastToggle = screen.getByRole("button", {
        name: "+4 past replicas",
      });
      expect(pastToggle).toHaveFocus();
      expect(await screen.findByRole("tooltip")).toHaveTextContent(/r1100cc/);
      await user.keyboard("{Enter}");
      expect(screen.getByRole("button", { name: "r1 100cc" })).toHaveAttribute(
        "aria-pressed",
        "false",
      );
      expect(await statusRowCount()).toBe(4);
    });
  });

  // r1's sample for one process, in percent of its CPU allocation and a
  // 1000-byte heap limit. Null readings are a failed fetch.
  const processRow = (
    processId: string,
    atMs: number,
    cpuPercent: number | null,
    heapPercent: number | null,
  ) => [
    "u1",
    processId,
    String(atMs),
    cpuPercent === null
      ? null
      : String((cpuPercent / 100) * ALLOCATION.cpuNanoCores),
    cpuPercent === null ? null : "1",
    null,
    heapPercent === null ? null : String(heapPercent * 10),
    heapPercent === null ? null : "1000",
  ];

  const processSamplesSocket = (rows: unknown[][]) =>
    mockSubscribeSocket((sql) =>
      sql.includes("mz_cluster_replica_metrics_history")
        ? {
            columns: [
              buildColumn({ name: "replicaId" }),
              buildColumn({ name: "processId" }),
              timestamptz("occurredAt"),
              ...[
                "cpuNanoCores",
                "memoryBytes",
                "diskBytes",
                "heapBytes",
                "heapLimit",
              ].map(float8),
            ],
            rows,
          }
        : undefined,
    );

  const openProcessesDrawer = async (processes: number, rows: unknown[][]) => {
    server.use(processSamplesSocket(rows));
    await renderResourceUsage({ withSwap: true, processes });
    await userEvent.click(
      await screen.findByRole("button", { name: "Processes" }),
    );
    return screen.findByRole("dialog");
  };

  it("has no processes button when every size runs one process", async () => {
    await renderResourceUsage({ withSwap: true, processes: 1 });

    await screen.findByRole("img", { name: "CPU usage" });
    // Give the allocations time to arrive.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(
      screen.queryByRole("button", { name: "Processes" }),
    ).not.toBeInTheDocument();
  });

  it("shows a multi-process replica's busiest process and a selectable line per process in a drawer", async () => {
    // A day's window has 24-minute buckets, so samples 30 minutes apart land
    // in two and each process draws a segment.
    const drawer = await openProcessesDrawer(
      2,
      [NOW_MS - 60 * 60_000, NOW_MS - 30 * 60_000].flatMap((atMs) => [
        processRow("0", atMs, 90, 40),
        processRow("1", atMs, 30, 20),
      ]),
    );
    expect(
      within(drawer)
        .getAllByRole("tab")
        .map((tab) => tab.textContent),
    ).toEqual(["r1", "r2"]);
    expect(
      await within(drawer).findByText(
        "Process 0 used the most CPU at any moment: 90.0%, 3.0× the other process (30.0%).",
      ),
    ).toBeVisible();
    expect(
      within(drawer).getByText(
        "Process 0 used the most memory at any moment: 40.0%, 2.0× the other process (20.0%).",
      ),
    ).toBeVisible();
    // The card shows 7 days, more than the drawer reads.
    expect(
      within(drawer).getByText(
        /from the last 24 hours, the most this drawer reads\.$/,
      ),
    ).toBeVisible();

    const statistic = within(drawer).getByRole("combobox", {
      name: "Which reading to compare processes by",
    });
    await userEvent.selectOptions(statistic, "on average");
    expect(
      within(drawer).getByText(
        "Process 0 used the most CPU on average: 90.0%, 3.0× the other process (30.0%).",
      ),
    ).toBeVisible();
    expect(within(drawer).getByText("over the last 24 hours")).toBeVisible();
    await userEvent.selectOptions(statistic, "right now");
    expect(
      within(drawer).queryByText("over the last 24 hours"),
    ).not.toBeInTheDocument();

    // A path through a lone point has no segment.
    const lineCount = (name: string) =>
      [
        ...within(drawer)
          .getByRole("img", { name })
          .querySelectorAll(".visx-linepath"),
      ].filter((path) => path.getAttribute("d")?.includes("L")).length;
    expect(lineCount("CPU by Process")).toEqual(2);
    expect(lineCount("Memory by Process")).toEqual(2);
    const legendLabels = () =>
      within(drawer)
        .queryAllByText(/^r1 · process \d+$/)
        .map((label) => label.textContent);
    expect(legendLabels()).toEqual(["r1 · process 0", "r1 · process 1"]);
    expect(
      within(drawer).getByText(
        "Check a process to draw its line. Memory is a share of each process's heap limit.",
      ),
    ).toBeVisible();

    const table = within(drawer).getByRole("table");
    const showEvery = within(table).getByRole("checkbox", {
      name: "Show every process",
    });
    expect(showEvery).toBeChecked();
    await userEvent.click(
      within(table).getByRole("checkbox", { name: "Show process 1" }),
    );
    expect(lineCount("CPU by Process")).toEqual(1);
    expect(legendLabels()).toEqual(["r1 · process 0"]);
    expect(showEvery).toBePartiallyChecked();
    await userEvent.click(showEvery);
    expect(lineCount("Memory by Process")).toEqual(2);
    await userEvent.click(showEvery);
    expect(lineCount("Memory by Process")).toEqual(0);
    expect(showEvery).not.toBeChecked();

    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((header) => header.textContent),
    ).toEqual([
      "",
      "Process",
      "CPU now",
      "CPU peak",
      "CPU average",
      "Memory now",
      "Memory peak",
      "Memory average",
    ]);
    expect(
      within(table)
        .getAllByRole("row")
        .slice(1)
        .map((row) => row.textContent),
    ).toEqual([
      "Process 0busiest90.0%90.0%90.0%40.0%40.0%40.0%",
      "Process 130.0%30.0%30.0%20.0%20.0%20.0%",
    ]);
  });

  it("reads a process that is down in the newest round as having no reading right now", async () => {
    const beforeMs = NOW_MS - 32 * 60_000;
    const latestMs = NOW_MS - 31 * 60_000;
    // Process 1 read 94% before it went down.
    const drawer = await openProcessesDrawer(2, [
      processRow("0", beforeMs, 30, 30),
      processRow("1", beforeMs, 94, 94),
      processRow("0", latestMs, 30, 30),
      processRow("1", latestMs, null, null),
    ]);
    await userEvent.selectOptions(
      await within(drawer).findByRole("combobox", {
        name: "Which reading to compare processes by",
      }),
      "right now",
    );

    expect(
      within(drawer).getByText("No reading right now from process 1."),
    ).toBeVisible();
    expect(
      within(drawer).queryByText(/used the most CPU right now/),
    ).not.toBeInTheDocument();
    expect(
      within(within(drawer).getByRole("table"))
        .getAllByRole("row")
        .slice(1)
        .map((tableRow) => tableRow.textContent),
    ).toEqual([
      "Process 030.0%30.0%30.0%30.0%30.0%30.0%",
      "Process 1-94.0%94.0%-94.0%94.0%",
    ]);
  });

  it("reads idle processes as even, however large the ratio between them", async () => {
    const atMs = NOW_MS - 30 * 60_000;
    const drawer = await openProcessesDrawer(2, [
      processRow("0", atMs, 0, 0.1),
      processRow("1", atMs, 0, 0.3),
    ]);

    expect(
      await within(drawer).findByText(
        "CPU is spread evenly at any moment: process 0 read 0.0%, against 0.0% for the other process.",
      ),
    ).toBeVisible();
    expect(
      within(drawer).getByText(
        "Memory is spread evenly at any moment: process 1 read 0.3%, against 0.1% for the other process.",
      ),
    ).toBeVisible();
    expect(
      within(within(drawer).getByRole("table")).queryByText(/busiest/),
    ).not.toBeInTheDocument();
  });

  describe("with more processes than colors", () => {
    const BUCKET_MS = DAY_MS / 60;
    const OLDER_MS = NOW_MS - 60 * 60_000;
    const NEWER_MS = NOW_MS - 30 * 60_000;
    // Process 0 reads 90% CPU against 10% for the other seven, and process
    // 3's 0.3% heap leads their 0.1%. Only process 0 reports in the newer
    // sample, a bucket later.
    const rows = [
      ...Array.from({ length: 8 }, (_, process) =>
        processRow(
          String(process),
          OLDER_MS,
          process === 0 ? 90 : 10,
          process === 3 ? 0.3 : 0.1,
        ),
      ),
      processRow("0", NEWER_MS, 90, 0.1),
      ...Array.from({ length: 7 }, (_, index) =>
        processRow(String(index + 1), NEWER_MS, null, null),
      ),
    ];

    it("colors only a process that stands out", async () => {
      const drawer = await openProcessesDrawer(8, rows);

      expect(
        await within(drawer).findByText(
          "Memory is spread evenly at any moment: process 3 read 0.3%, against 0.1% for the other 7 processes.",
        ),
      ).toBeVisible();
      expect(
        within(drawer)
          .queryAllByText(/^r1 · process \d+$/)
          .map((label) => label.textContent),
      ).toEqual(["r1 · process 0"]);
      expect(within(drawer).getByText("Other processes")).toBeVisible();
    });

    it("highlights a hovered grey process in a color outside the palette", async () => {
      const drawer = await openProcessesDrawer(8, rows);

      fireEvent.mouseOver(
        within(await within(drawer).findByRole("table")).getByText("Process 2"),
      );
      const label = within(
        within(drawer).getByRole("img", { name: "CPU by Process" }),
      ).getByText("p2");
      expect(label).toHaveAttribute("fill", lightColors.foreground.primary);
      expect(lightColors.lineGraph).not.toContain(label.getAttribute("fill"));
    });

    it("draws a reading with no neighbor as a dot", async () => {
      const drawer = await openProcessesDrawer(8, rows);

      const chart = await within(drawer).findByRole("img", {
        name: "CPU by Process",
      });
      expect(chart.querySelectorAll("circle")).toHaveLength(7);
    });

    it("names at most six processes with no reading in the tooltip", async () => {
      const drawer = await openProcessesDrawer(8, rows);

      const chart = await within(drawer).findByRole("img", {
        name: "CPU by Process",
      });
      const overlay = chart.querySelector('rect[fill="transparent"]');
      if (!overlay) throw new Error("hover overlay not rendered");
      const endMs = Date.now();
      const hoverMs =
        Math.floor(NEWER_MS / BUCKET_MS) * BUCKET_MS + BUCKET_MS / 2;
      const plotWidthPx = WIDTH_PX - CHART_MARGIN.left - CHART_MARGIN.right;
      fireEvent.pointerMove(overlay, {
        clientX:
          CHART_MARGIN.left +
          ((hoverMs - (endMs - DAY_MS)) / DAY_MS) * plotWidthPx,
        clientY: 80,
      });

      expect(
        within(await screen.findByTestId("chart-tooltip")).getByText(
          "No reading from process 1, process 2, process 3, process 4, process 5, process 6 and 1 more",
        ),
      ).toBeVisible();
    });
  });

  it("refetches status history when a replica changes status, though the cluster's latest status update doesn't move", async () => {
    // As on a system cluster, whose `latestStatusUpdate` is always null.
    const cluster = buildCluster(REPLICA_NAMES);
    let setCluster: (next: Cluster) => void = () => {};
    const Harness = () => {
      const [shown, setShown] = React.useState(cluster);
      React.useEffect(() => {
        setCluster = setShown;
      }, []);
      return (
        <ResourceUsage
          cluster={shown}
          timePeriodMinutes={TIME_PERIOD_MINUTES}
        />
      );
    };
    await setFakeEnvironment(getStore().set, "aws/us-east-1", NEW_ENVIRONMENT);
    registerHandlers({ withSwap: true, replicaNames: REPLICA_NAMES });
    await renderComponent(<Harness />, {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", NEW_ENVIRONMENT),
    });
    await screen.findByRole("img", { name: "CPU usage" });
    const requestedKeys: unknown[] = [];
    server.use(
      http.post("*/api/sql", ({ request }) => {
        const queryKey = JSON.parse(
          new URL(request.url).searchParams.get("query_key") ?? "[]",
        );
        if (queryKey[1]?.scope !== "replicaStatusHistory") return undefined;
        requestedKeys.push(queryKey[1].statusesUpdatedAt);
        return HttpResponse.json({
          results: [
            mapKyselyToTabular({
              rows: [],
              columns: [timestamptz("occurredAt")],
            }),
          ],
        });
      }),
    );
    const changedAt = "2026-10-20T00:07:00.000Z";
    act(() =>
      setCluster({
        ...cluster,
        replicas: cluster.replicas.map((replica) => ({
          ...replica,
          statuses: [
            {
              replica_id: replica.id,
              process_id: "0",
              reason: null,
              status: "online",
              updated_at: changedAt,
            },
          ],
        })),
      }),
    );

    await waitFor(() =>
      expect(requestedKeys).toEqual([new Date(changedAt).getTime()]),
    );
  });

  it("shows an error when utilization fails to load", async () => {
    await renderResourceUsage({ withSwap: true, utilizationError: true });

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });

  describe("on the Metrics page", () => {
    const findReplicaSelect = () =>
      screen.findByRole("combobox", { name: "Replica" });
    const statusRowNames = async () =>
      within(
        await screen.findByRole("img", { name: "Replica status over time" }),
      )
        .getAllByText(/^r\d$/)
        .map((label) => label.textContent);

    it("narrows the card to the replica picked in the toolbar, and keeps the pick in the URL", async () => {
      await renderClusterMetrics();

      const select = await findReplicaSelect();
      expect(
        within(select)
          .getAllByRole("option")
          .map((option) => option.textContent),
      ).toEqual(["All replicas", "r1 · 100cc", "r2 · 100cc"]);
      expect(await statusRowNames()).toEqual(["r1", "r2"]);

      await userEvent.selectOptions(select, "r2 · 100cc");
      expect(screen.getByTestId("location-search")).toHaveTextContent(
        "?replica=r2",
      );
      expect(
        screen.queryByRole("list", { name: "Replicas" }),
      ).not.toBeInTheDocument();
      expect(await statusRowNames()).toEqual(["r2"]);
      const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
      expect(tooltip).toHaveTextContent(/r2.*CPU1\.8 cores · 90\.0%/);
      expect(tooltip).not.toHaveTextContent("r1");

      await userEvent.selectOptions(select, "All replicas");
      expect(screen.getByTestId("location-search")).not.toHaveTextContent(
        "replica=",
      );
      expect(screen.getByRole("list", { name: "Replicas" })).toBeVisible();
      expect(await statusRowNames()).toEqual(["r1", "r2"]);
    });

    it("opens on the replica the URL names", async () => {
      await renderClusterMetrics("?replica=r2");

      expect(await findReplicaSelect()).toHaveValue("r2");
      expect(await statusRowNames()).toEqual(["r2"]);
    });

    it("shows every replica when the URL names one that no longer exists", async () => {
      await renderClusterMetrics("?replica=r9");

      expect(await findReplicaSelect()).toHaveValue("");
      expect(await statusRowNames()).toEqual(["r1", "r2"]);
      expect(screen.getByRole("list", { name: "Replicas" })).toBeVisible();
    });

    it("shows only the selected replica in the processes drawer", async () => {
      server.use(processSamplesSocket([]));
      await renderClusterMetrics("?replica=r2", 2);

      await userEvent.click(
        await screen.findByRole("button", { name: "Processes" }),
      );
      const drawer = await screen.findByRole("dialog");
      expect(
        within(drawer)
          .getAllByRole("tab")
          .map((tab) => tab.textContent),
      ).toEqual(["r2"]);
    });
  });
});
