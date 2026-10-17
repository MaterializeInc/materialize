// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

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
import { clusterQueryKeys } from "~/platform/clusters/queries";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { mockSubscribeSocket } from "~/test/mockSubscribeSocket";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

import { ResourceUsage } from "./ResourceUsage";
import { CHART_MARGIN } from "./resourceUsageStyles";

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
const HEAP_LIMIT_BYTES = 25 * 2 ** 30;

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

const timestamptz = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.timestamptz });
const float8 = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.float8 });

const utilizationRows = ({
  withSwap,
  withoutHeapLimit,
  emptyReadings,
  r2CpuPercent,
  replicaIds,
  replicaNames,
}: {
  withSwap: boolean;
  withoutHeapLimit: boolean;
  emptyReadings: boolean;
  r2CpuPercent: number;
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
        maxDiskPercent: null,
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

const registerHandlers = ({
  withSwap,
  withoutHeapLimit = false,
  r2CpuPercent = 0.9,
  r2OfflineAllWindow = false,
  emptyReadings = false,
  statusRows,
  statusDelayMs,
  r2UnhydratedObjectIds = [],
  utilizationError = false,
  utilizationRowCount,
  replicaNames,
}: {
  withSwap: boolean;
  /** Rows carry the memory breakdown, but no sample has a heap limit. */
  withoutHeapLimit?: boolean;
  r2CpuPercent?: number;
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
  replicaNames: Record<string, string>;
}) => {
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
    replicaIds: r2OfflineAllWindow ? ["u1"] : ["u1", "u2"],
    replicaNames,
  }).slice(0, utilizationRowCount);
  server.use(
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
        }),
        results: mapKyselyToTabular({
          rows:
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
        objectIds: ["u100"],
        timePeriodMinutes: TIME_PERIOD_MINUTES,
      }),
      results: mapKyselyToTabular({
        rows: [{ id: "u100", occurredAt: String(DDL_AT_MS) }],
        columns: [timestamptz("occurredAt")],
      }),
    }),
  );
};

// Query keys embed the environment version, so set the environment before
// building the handlers that match on them.
const renderResourceUsage = async ({
  environment = NEW_ENVIRONMENT,
  replicaNames = REPLICA_NAMES,
  ...handlerOptions
}: Omit<Parameters<typeof registerHandlers>[0], "replicaNames"> & {
  environment?: typeof healthyEnvironment;
  replicaNames?: Record<string, string>;
}) => {
  await setFakeEnvironment(getStore().set, "aws/us-east-1", environment);
  registerHandlers({ ...handlerOptions, replicaNames });
  return renderComponent(
    <ResourceUsage cluster={buildCluster(replicaNames)} />,
    {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", environment),
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
    allObjectsCollection.applySnapshot({
      data: [
        {
          databaseName: "materialize",
          databaseId: "u1",
          name: "orders_by_day_idx",
          schemaName: "public",
          schemaId: "u2",
          id: "u100",
          objectType: "index",
          sourceType: null,
          isWebhookTable: null,
          clusterId: "u9",
          clusterName: "prod",
        },
      ],
      snapshotComplete: true,
      error: undefined,
    });
  });

  it("shows each replica's CPU, memory and swap for the hovered time", async () => {
    await renderResourceUsage({ withSwap: true });

    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(
      /r1.*Running.*CPU45\.0%.*Memory40\.0%.*Swap20\.0%/,
    );
    expect(tooltip).toHaveTextContent(/r2.*Running.*CPU90\.0%/);
  });

  it("marks an out-of-memory kill in the tooltip", async () => {
    await renderResourceUsage({ withSwap: true });

    const tooltip = await hoverAt(OOM_AT_MS);
    expect(tooltip).toHaveTextContent(/r2.*Out of Memory/);
  });

  it("labels the memory limits with their sizes", async () => {
    await renderResourceUsage({ withSwap: true });

    expect(await screen.findByText("heap limit · 25 GB")).toBeVisible();
    // The heap limit is 125% of RAM, so RAM ends at 80% of it.
    expect(screen.getByText("RAM limit · 20 GB")).toBeVisible();
    expect(screen.getByText("swap")).toBeVisible();
  });

  it("shows heap alone on environments without the swap columns", async () => {
    await renderResourceUsage({
      withSwap: false,
      environment: healthyEnvironment,
    });

    expect(await screen.findByText("heap")).toBeVisible();
    expect(screen.queryByText("swap")).not.toBeInTheDocument();
    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(/r1.*CPU45\.0%.*Heap60\.0%/);
    expect(tooltip).not.toHaveTextContent("Swap");
  });

  it("labels memory as a share of the allocation when no sample has a heap limit", async () => {
    await renderResourceUsage({ withSwap: false, withoutHeapLimit: true });

    expect(await screen.findByText("% of memory allocation")).toBeVisible();
    expect(screen.getByText("memory limit")).toBeVisible();
    expect(screen.queryByText(/heap/i)).not.toBeInTheDocument();
    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(/r1.*CPU45\.0%.*Memory60\.0%/);
  });

  it("extends the CPU axis past 100% when a replica exceeds its allocation", async () => {
    await renderResourceUsage({ withSwap: true, r2CpuPercent: 2.5 });

    const cpu = await screen.findByRole("img", { name: "CPU usage" });
    expect(within(cpu).getByText("250%")).toBeInTheDocument();
    expect(screen.getByText("CPU allocation")).toBeVisible();
    const tooltip = await hoverAt(FIRST_BUCKET_MS + 50 * HOUR_MS);
    expect(tooltip).toHaveTextContent(/r2.*CPU250\.0%/);
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

  it("labels an index created on the cluster in the CPU chart", async () => {
    await renderResourceUsage({ withSwap: true });

    const cpu = await screen.findByRole("img", { name: "CPU usage" });
    expect(
      await within(cpu).findByText("CREATE INDEX orders_by_day_idx"),
    ).toBeInTheDocument();
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
    renderComponent(<ResourceUsage cluster={buildCluster(currentReplicas)} />, {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", NEW_ENVIRONMENT),
    });
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
      within(screen.getByRole("list", { name: "Replicas" })).getAllByRole(
        "button",
      ),
    ).toHaveLength(1);
    expect(
      screen.getByRole("button", { name: /r1 \(dropped\)/ }),
    ).toBeVisible();
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
    ).toEqual(["r1100cc", "r1 (dropped)100cc"]);
  });

  const sizeAllocationsHandler = (processes: number) =>
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
            cpuNanoCores: "1000000000",
            memoryBytes: String(8 * 2 ** 30),
            diskBytes: "0",
          },
        ],
      }),
    });

  it("has no processes button when every size runs one process", async () => {
    // Query keys embed the environment version.
    await setFakeEnvironment(getStore().set, "aws/us-east-1", NEW_ENVIRONMENT);
    server.use(sizeAllocationsHandler(1));
    await renderResourceUsage({ withSwap: true });

    await screen.findByRole("img", { name: "CPU usage" });
    // Give the allocations time to arrive.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(
      screen.queryByRole("button", { name: "Processes" }),
    ).not.toBeInTheDocument();
  });

  it("shows a multi-process replica's busiest process and a selectable line per process in a drawer", async () => {
    const sampleAt = NOW_MS - 30 * 60_000;
    await setFakeEnvironment(getStore().set, "aws/us-east-1", NEW_ENVIRONMENT);
    server.use(
      sizeAllocationsHandler(2),
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
              rows: [
                [
                  "u1",
                  "0",
                  String(sampleAt),
                  "900000000",
                  "1",
                  null,
                  "40",
                  "100",
                ],
                [
                  "u1",
                  "1",
                  String(sampleAt),
                  "300000000",
                  "1",
                  null,
                  "20",
                  "100",
                ],
              ],
            }
          : undefined,
      ),
    );
    await renderResourceUsage({ withSwap: true });

    await userEvent.click(
      await screen.findByRole("button", { name: "Processes" }),
    );

    const drawer = await screen.findByRole("dialog");
    expect(
      within(drawer)
        .getAllByRole("tab")
        .map((tab) => tab.textContent),
    ).toEqual(["r1", "r2"]);
    expect(
      await within(drawer).findByText(
        "Process 0 used the most CPU: 90.0% on average, 3.0× the other process (30.0%).",
      ),
    ).toBeVisible();
    expect(
      within(drawer).getByText(
        "Process 0 used the most memory: 40.0% on average, 2.0× the other process (20.0%).",
      ),
    ).toBeVisible();
    const lineCount = (name: string) =>
      within(drawer)
        .getByRole("img", { name })
        .querySelectorAll(".visx-linepath").length;
    expect(lineCount("CPU by process")).toEqual(2);
    expect(lineCount("Memory by process")).toEqual(2);
    const legend = within(drawer).getByRole("list", { name: "Processes" });
    expect(
      within(legend)
        .getAllByRole("button", { pressed: true })
        .map((chip) => chip.textContent),
    ).toEqual(["Process 0busiest", "Process 1"]);

    await userEvent.click(
      within(legend).getByRole("button", { name: /Process 1/ }),
    );
    expect(lineCount("CPU by process")).toEqual(1);
    await userEvent.click(
      within(legend).getByRole("button", { name: "Clear all" }),
    );
    expect(lineCount("Memory by process")).toEqual(0);
    await userEvent.click(
      within(legend).getByRole("button", { name: "Select all" }),
    );
    expect(lineCount("Memory by process")).toEqual(2);

    const rows = within(within(drawer).getByRole("table")).getAllByRole("row");
    expect(rows.slice(1).map((row) => row.textContent)).toEqual([
      "Process 0busiest90.0%90.0%40.0%40.0%",
      "Process 130.0%30.0%20.0%20.0%",
    ]);
  });

  it("shows an error when utilization fails to load", async () => {
    await renderResourceUsage({ withSwap: true, utilizationError: true });

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });
});
