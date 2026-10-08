// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen, within } from "@testing-library/react";
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
  ParentSize: ({
    children,
  }: {
    children: (size: { width: number; height: number }) => React.ReactNode;
  }) => <>{children({ width: WIDTH_PX, height: 240 })}</>,
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

const cluster = buildClusterServerResponse({ id: "u9", name: "prod" }, [
  { id: "u1", name: "r1", size: "100cc", disk: false, statuses: [] },
  { id: "u2", name: "r2", size: "100cc", disk: false, statuses: [] },
]);

const timestamptz = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.timestamptz });
const float8 = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.float8 });

const utilizationRows = ({
  withSwap,
  withoutHeapLimit,
  r2CpuPercent,
  replicaIds,
}: {
  withSwap: boolean;
  withoutHeapLimit: boolean;
  r2CpuPercent: number;
  replicaIds: string[];
}) =>
  replicaIds.flatMap((replicaId) =>
    Array.from({ length: BUCKET_COUNT }, (_, bucketIndex) => {
      const bucketStartMs = FIRST_BUCKET_MS + bucketIndex * HOUR_MS;
      const at = String(bucketStartMs);
      return {
        bucketStart: at,
        replicaId,
        maxMemoryPercent: 0.5,
        maxMemoryAt: at,
        maxDiskPercent: null,
        maxDiskAt: at,
        maxCpuPercent: replicaId === "u1" ? 0.45 : r2CpuPercent,
        maxCpuAt: at,
        maxHeapPercent: 0.6,
        maxHeapAt: at,
        maxMemoryAndDiskPercent: null,
        maxMemoryAndDiskMemoryPercent: null,
        maxMemoryAndDiskDiskPercent: null,
        maxMemoryAndDiskAt: at,
        offlineEvents: null,
        bucketEnd: String(bucketStartMs + HOUR_MS),
        name: replicaId === "u1" ? "r1" : "r2",
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
  r2UnhydratedObjectIds = [],
  utilizationError = false,
  utilizationRowCount,
}: {
  withSwap: boolean;
  /** Rows carry the memory breakdown, but no sample has a heap limit. */
  withoutHeapLimit?: boolean;
  r2CpuPercent?: number;
  /** r2 went offline before the window and reports no samples in it. */
  r2OfflineAllWindow?: boolean;
  r2UnhydratedObjectIds?: string[];
  utilizationError?: boolean;
  utilizationRowCount?: number;
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
    r2CpuPercent,
    replicaIds: r2OfflineAllWindow ? ["u1"] : ["u1", "u2"],
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
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaStatusHistory({
        replicaIds: ["u1", "u2"],
      }),
      results: mapKyselyToTabular({
        rows: r2OfflineAllWindow
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
            ],
        columns: [timestamptz("occurredAt")],
      }),
    }),
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
  ...handlerOptions
}: Parameters<typeof registerHandlers>[0] & {
  environment?: typeof healthyEnvironment;
}) => {
  await setFakeEnvironment(getStore().set, "aws/us-east-1", environment);
  registerHandlers(handlerOptions);
  return renderComponent(<ResourceUsage cluster={cluster} />, {
    initializeState: ({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", environment),
  });
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

  it("says so when the window has no usage", async () => {
    await renderResourceUsage({ withSwap: true, utilizationRowCount: 0 });

    expect(
      await screen.findByText("No resource usage in this time period."),
    ).toBeVisible();
    expect(
      await screen.findByRole("img", { name: "Replica status over time" }),
    ).toBeInTheDocument();
  });

  it("shows an error when utilization fails to load", async () => {
    await renderResourceUsage({ withSwap: true, utilizationError: true });

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });
});
