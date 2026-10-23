// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { focusManager } from "@tanstack/react-query";
import {
  act,
  fireEvent,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import parsePostgresInterval from "postgres-interval";
import React from "react";
import { Route, Routes, useLocation } from "react-router-dom";

import { MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { buildClusterServerResponse } from "~/platform/clusters/clustersTestUtils";
import { clusterQueryKeys } from "~/platform/clusters/queries";
import { allClusters } from "~/store/allClusters";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { buildValidMaterializationLagHandler } from "~/test/clusterQueryBuilders";
import { mockSubscribeState } from "~/test/mockSubscribe";
import {
  MockSubscribeConnection,
  mockSubscribeSocket,
} from "~/test/mockSubscribeSocket";
import {
  buildDatabaseObject,
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { formatDate, TIME_FORMAT } from "~/utils/dateFormat";
import { parseDbVersion } from "~/version/api";

import { ClusterWorkers } from "./ClusterWorkers";
import { CPU_DOCS } from "./WorkerCpu/workerCpuModel";

// jsdom lays nothing out, so give the charts a real width.
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
    version: parseDbVersion("v26.45.0 (0000000000)"),
  },
};

// Size by replica, and each size's processes and workers per process.
const SIZES: Record<string, [number, number]> = {
  "25cc": [1, 1],
  "100cc": [1, 2],
  "200cc": [2, 2],
};

// The tab keeps a replica's readings and measurement for its next visit, by
// replica id, so each test reads replicas of its own.
let replicaIdBase = 0;

beforeEach(() => {
  replicaIdBase += 10;
});

const buildCluster = (replicas: Record<string, string>) =>
  buildClusterServerResponse(
    { id: "u9", name: "prod" },
    Object.entries(replicas).map(([name, size], index) => ({
      id: `u${replicaIdBase + index + 1}`,
      name,
      size,
      disk: false,
      statuses: [],
    })),
  );

const float8 = (name: string) =>
  buildColumn({ name, type_oid: MzDataType.float8 });

const PROCESS_SAMPLE_COLUMNS = [
  buildColumn({ name: "replicaId" }),
  buildColumn({ name: "processId" }),
  buildColumn({ name: "occurredAt", type_oid: MzDataType.timestamptz }),
  ...["cpuNanoCores", "memoryBytes", "diskBytes", "heapBytes", "heapLimit"].map(
    float8,
  ),
];

/** Process 0's sample at `atMs`, using `cores` of CPU. */
const processSample = (replicaId: string, atMs: number, cores: number) => [
  replicaId,
  "0",
  String(atMs),
  String(cores * 1e9),
  "1",
  null,
  null,
  null,
];

/** The replica's process samples, and the sessions that subscribe to them. */
interface ServedMetrics {
  rows: unknown[][];
  connections: MockSubscribeConnection[];
}

const WORKER_CPU_COLUMNS = [
  buildColumn({ name: "kind" }),
  buildColumn({ name: "dataflowId" }),
  buildColumn({ name: "workerId", type_oid: MzDataType.int4 }),
  float8("elapsedNs"),
  buildColumn({ name: "objectId" }),
  buildColumn({ name: "connectionId", type_oid: MzDataType.uint4 }),
  buildColumn({ name: "dataflowName" }),
];

const SECOND_NS = 1e9;

const elapsedRow = (
  dataflowId: string,
  workerId: number,
  elapsedNs: number,
) => [
  "elapsed",
  dataflowId,
  String(workerId),
  String(elapsedNs),
  null,
  null,
  null,
];

const exportRow = (dataflowId: string, objectId: string) => [
  "export",
  dataflowId,
  null,
  null,
  objectId,
  null,
  null,
];

const FIXED_ROWS = [0, 1].map((workerId) =>
  elapsedRow("1", workerId, SECOND_NS),
);

const OBJECTS = [
  buildDatabaseObject({
    id: "u200",
    name: "orders_by_customer",
    clusterId: "u9",
    clusterName: "prod",
  }),
  buildDatabaseObject({
    id: "u201",
    name: "customers_idx",
    objectType: "index",
    clusterId: "u9",
    clusterName: "prod",
  }),
];
const OBJECT_BY_DATAFLOW: Record<string, string> = { "1": "u200", "2": "u201" };

interface Session {
  connection: MockSubscribeConnection;
  replicaName: unknown;
}

/** What the tab's worker CPU subscribe reads from a replica. */
interface ServedReplica {
  /** The counters as last reported, and when. */
  counters: () => { rows: unknown[][]; atMs: number };
  /** The worker CPU sessions opened on it. */
  sessions: Session[];
}

/**
 * Serves the tab's subscribes from `replica`, or from counters that never
 * change, holding the worker CPU snapshot back with `holdSnapshot`. Returns
 * the worker CPU sessions they open.
 */
const serveSubscribes = ({
  holdSnapshot = false,
  replica,
  metrics,
  failing,
}: {
  holdSnapshot?: boolean;
  replica?: ServedReplica;
  metrics?: ServedMetrics;
  /** Fails the worker CPU or the process metrics subscribe. */
  failing?: "workerCpu" | "metrics";
} = {}) => {
  const sessions = replica?.sessions ?? [];
  server.use(
    mockSubscribeSocket((sql, _params, connection) => {
      if (sql.includes("mz_scheduling_elapsed_raw")) {
        sessions.push({
          connection,
          replicaName: connection.options.cluster_replica,
        });
        if (failing === "workerCpu") {
          connection.error(
            "permission denied for SOURCE mz_scheduling_elapsed_raw",
          );
          return undefined;
        }
        if (holdSnapshot) return undefined;
        const { rows, atMs } = replica?.counters() ?? {
          rows: FIXED_ROWS,
          atMs: Date.now(),
        };
        // The snapshot's upper is then `atMs`.
        return { columns: WORKER_CPU_COLUMNS, rows, timestamp: atMs - 1 };
      }
      if (sql.includes("mz_cluster_replica_metrics_history")) {
        if (failing === "metrics") {
          connection.error(
            "permission denied for TABLE mz_cluster_replica_metrics_history",
          );
          return undefined;
        }
        if (!metrics) return { columns: buildColumns(["replicaId"]), rows: [] };
        metrics.connections.push(connection);
        return { columns: PROCESS_SAMPLE_COLUMNS, rows: metrics.rows };
      }
      return undefined;
    }),
  );
  return sessions;
};

const openSessions = (sessions: Session[]) =>
  sessions.filter(({ connection }) => !connection.isClosed);

// Past the longest test's clock.
const REPORT_HORIZON_S = 4 * 60;

type WorkerShares = Record<string, number[]>;

/**
 * A replica running `u200` on dataflow 1 and `u201` on dataflow 2, where each
 * dataflow spends `perSecond[dataflowId][workerId]` of each second on each
 * worker. It reports its counters each second, as introspection does.
 * Create it before rendering, so a report lands before a reading due at the
 * same instant.
 */
const createReplica = (perSecond: WorkerShares) => {
  const elapsedNs = new Map(
    Object.keys(OBJECT_BY_DATAFLOW).map((dataflowId) => [
      dataflowId,
      perSecond[dataflowId].map(() => 0),
    ]),
  );
  let reportedAtMs = Date.now();
  const rows = () => [
    ...[...elapsedNs].flatMap(([dataflowId, byWorker]) =>
      byWorker.map((ns, workerId) => elapsedRow(dataflowId, workerId, ns)),
    ),
    ...Object.entries(OBJECT_BY_DATAFLOW).map(([dataflowId, objectId]) =>
      exportRow(dataflowId, objectId),
    ),
  ];
  const replica = {
    perSecond,
    sessions: [] as Session[],
    counters: () => ({ rows: rows(), atMs: reportedAtMs }),
  };
  for (let second = 1; second <= REPORT_HORIZON_S; second++) {
    setTimeout(() => {
      for (const [dataflowId, byWorker] of elapsedNs) {
        replica.perSecond[dataflowId].forEach((share, workerId) => {
          byWorker[workerId] += share * SECOND_NS;
        });
      }
      reportedAtMs = Date.now();
      for (const { connection } of openSessions(replica.sessions)) {
        connection.advance(reportedAtMs, rows());
      }
    }, second * 1000);
  }
  return replica;
};

const LocationProbe = () => {
  const location = useLocation();
  return (
    <div data-testid="location">
      {location.pathname}
      {location.search}
      {location.state ? ` ${JSON.stringify(location.state)}` : ""}
    </div>
  );
};

const renderWorkers = async (
  replicas: Record<string, string>,
  search = "",
  options: Parameters<typeof serveSubscribes>[0] = {},
) => {
  await setFakeEnvironment(getStore().set, "aws/us-east-1", ENVIRONMENT);
  const cluster = buildCluster(replicas);
  getStore().set(allClusters, mockSubscribeState({ data: [cluster] }));
  const sizes = [...new Set(Object.values(replicas))].sort();
  const replicaIds = cluster.replicas.map(({ id }) => id).sort();
  server.use(
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.replicaSizeAllocations({ sizes }),
      results: mapKyselyToTabular({
        columns: [
          buildColumn({ name: "size" }),
          ...[
            "processes",
            "workers",
            "cpuNanoCores",
            "memoryBytes",
            "diskBytes",
          ].map(float8),
        ],
        rows: sizes
          .filter((size) => SIZES[size])
          .map((size) => ({
            size,
            processes: String(SIZES[size][0]),
            workers: String(SIZES[size][1]),
            cpuNanoCores: "1000000000",
            memoryBytes: String(8 * 2 ** 30),
            diskBytes: "0",
          })),
      }),
    }),
    buildSqlQueryHandlerV2({
      queryKey: clusterQueryKeys.unhydratedComputeObjects({ replicaIds }),
      results: mapKyselyToTabular({
        rows: [],
        columns: buildColumns(["replicaId", "objectId"]),
      }),
    }),
    buildValidMaterializationLagHandler(
      { objectIds: OBJECTS.map(({ id }) => id) },
      [
        {
          targetObjectId: "u200",
          type: "materialized-view",
          hydrated: true,
          lag: parsePostgresInterval("00:01:33.676"),
          isOutdated: true,
        },
        {
          targetObjectId: "u201",
          type: "index",
          hydrated: true,
          lag: parsePostgresInterval("00:00:00.4"),
          isOutdated: false,
        },
      ],
    ),
  );
  const sessions = serveSubscribes(options);
  const result = await renderComponent(
    <Routes>
      <Route path=":clusterId/:clusterName">
        <Route index element={<LocationProbe />} />
        <Route
          path="workers"
          element={
            <>
              <ClusterWorkers />
              <LocationProbe />
            </>
          }
        />
      </Route>
    </Routes>,
    {
      initialRouterEntries: [`/u9/prod/workers${search}`],
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", ENVIRONMENT),
    },
  );
  return { ...result, sessions };
};

const findReplicaSelect = () =>
  screen.findByRole("combobox", { name: "Replica" });

const optionsOf = (select: HTMLElement) =>
  within(select)
    .getAllByRole("option")
    .map((option) => option.textContent);

describe("ClusterWorkers", () => {
  it("explains what workers are and why skew matters", async () => {
    await renderWorkers({ r1: "100cc" });

    expect(
      await screen.findByText(
        /^Each worker is a thread that runs a share of every object's work\..*one worker does most of the work while the others wait\.$/,
      ),
    ).toBeVisible();
    expect(screen.getByRole("link", { name: "Learn more" })).toHaveAttribute(
      "href",
      `${CPU_DOCS}#worker-skew`,
    );
  });

  it("offers every replica whose size it knows, and opens on the URL's", async () => {
    await renderWorkers(
      { r1: "25cc", r2: "100cc", r3: "200cc", r9: "custom" },
      "?replica=r3",
    );

    const select = await findReplicaSelect();
    expect(optionsOf(select)).toEqual([
      "r1 · 25cc",
      "r2 · 100cc",
      "r3 · 200cc",
    ]);
    expect(select).toHaveValue("r3");
    expect(await screen.findByText("4 workers")).toHaveTextContent(
      "4 workers · 2 processes × 2 workers",
    );
  });

  it("opens on the first replica when the URL names none, leaving the URL to show every replica on the other tabs", async () => {
    const { sessions } = await renderWorkers({
      r1: "25cc",
      r2: "100cc",
      r3: "200cc",
    });

    const select = await findReplicaSelect();
    await waitFor(() => expect(select).toHaveValue("r1"));
    expect(screen.getByTestId("location").textContent).toBe("/u9/prod/workers");
    await waitFor(() =>
      expect(
        openSessions(sessions).map(({ replicaName }) => replicaName),
      ).toEqual(["r1"]),
    );
  });

  it("reads a replica that runs a single worker", async () => {
    const { sessions } = await renderWorkers(
      { r1: "25cc", r2: "100cc" },
      "?replica=r1",
    );

    expect(await screen.findByText("1 worker")).toBeVisible();
    expect(await findReplicaSelect()).toHaveValue("r1");
    await waitFor(() =>
      expect(
        openSessions(sessions).map(({ replicaName }) => replicaName),
      ).toEqual(["r1"]),
    );
  });

  it("says the cluster has no replica a stale link names", async () => {
    const { sessions } = await renderWorkers({ r2: "100cc" }, "?replica=r9");

    expect(await screen.findByText("prod has no replica r9.")).toBeVisible();
    expect(sessions).toEqual([]);
  });

  it("stops reading a replica that's dropped, and says so", async () => {
    const { sessions } = await renderWorkers(
      { r2: "100cc", r3: "200cc" },
      "?replica=r3",
    );
    await waitFor(() => expect(openSessions(sessions)).toHaveLength(1));

    getStore().set(
      allClusters,
      mockSubscribeState({ data: [buildCluster({ r2: "100cc" })] }),
    );

    expect(await screen.findByText("prod has no replica r3.")).toBeVisible();
    await waitFor(() => expect(openSessions(sessions)).toEqual([]));
  });

  it("keeps a picked replica in the URL, and reads that replica", async () => {
    const { sessions } = await renderWorkers({ r2: "100cc", r3: "200cc" });
    const select = await findReplicaSelect();
    await waitFor(() => expect(select).toHaveValue("r2"));

    await userEvent.selectOptions(select, "r3 · 200cc");

    expect(screen.getByTestId("location")).toHaveTextContent(
      "/u9/prod/workers?replica=r3",
    );
    await waitFor(() =>
      expect(
        openSessions(sessions).map(({ replicaName }) => replicaName),
      ).toEqual(["r3"]),
    );
  });

  it("says why there's nothing to read when it can't tell how many workers any replica runs", async () => {
    const { sessions } = await renderWorkers({ r9: "custom" });

    expect(
      await screen.findByText(
        "Worker counts for prod's replica sizes are unavailable.",
      ),
    ).toBeVisible();
    expect(
      screen.queryByRole("combobox", { name: "Replica" }),
    ).not.toBeInTheDocument();
    expect(sessions).toEqual([]);
  });

  it("says when it can't tell how many workers the URL's replica runs, instead of reading another", async () => {
    const { sessions } = await renderWorkers(
      { r1: "25cc", r9: "custom" },
      "?replica=r9",
    );

    expect(
      await screen.findByText("The worker count for r9's size is unavailable."),
    ).toBeVisible();
    const select = await findReplicaSelect();
    expect(select).toHaveValue("");
    expect(optionsOf(select)).toEqual(["Choose a replica", "r1 · 25cc"]);
    expect(sessions).toEqual([]);
  });

  it("says why there's nothing to read on a cluster without replicas", async () => {
    await renderWorkers({});

    expect(await screen.findByText("prod has no replicas.")).toBeVisible();
  });

  it("links a multi-process replica to its processes on the Metrics tab", async () => {
    await renderWorkers({ r3: "200cc" });

    await userEvent.click(
      await screen.findByRole("link", { name: "CPU and memory by process" }),
    );

    expect(screen.getByTestId("location")).toHaveTextContent(
      '/u9/prod?replica=r3 {"openProcesses":true}',
    );
  });

  it("has no process link for a replica with one process", async () => {
    await renderWorkers({ r2: "100cc" });

    await findReplicaSelect();
    expect(
      screen.queryByRole("link", { name: "CPU and memory by process" }),
    ).not.toBeInTheDocument();
  });

  it("shows a failed read once, in place of the report", async () => {
    await renderWorkers({ r1: "100cc" }, "?replica=r1", {
      failing: "workerCpu",
    });

    expect(
      await screen.findAllByText(/^Couldn't read worker CPU on r1:/),
    ).toHaveLength(1);
    expect(screen.getByText("Stopped reading worker CPU on r1.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Try again" })).toBeEnabled();
  });

  it("says the finding leaves the replica's CPU out when its metrics can't be read", async () => {
    await renderWorkers({ r1: "100cc" }, "?replica=r1", { failing: "metrics" });

    expect(
      await screen.findByText(
        "Couldn't read r1's CPU metrics, so the finding leaves its CPU out.",
      ),
    ).toBeVisible();
  });

  it("says it's waiting while the replica's counters haven't arrived", async () => {
    await renderWorkers({ r2: "100cc" }, "?replica=r2", {
      holdSnapshot: true,
    });

    expect(await screen.findByText("Waiting for r2's counters.")).toBeVisible();
  });

  it("pauses and resumes its reads, closing its session while paused", async () => {
    const { sessions } = await renderWorkers({ r2: "100cc" }, "?replica=r2");
    await waitFor(() => expect(openSessions(sessions)).toHaveLength(1));
    expect(
      await screen.findByText(/^Reading worker CPU on r2 every 5s/),
    ).toBeVisible();

    await userEvent.click(screen.getByRole("button", { name: "Pause" }));

    expect(await screen.findByText("Paused.")).toBeVisible();
    await waitFor(() => expect(openSessions(sessions)).toHaveLength(0));

    await userEvent.click(screen.getByRole("button", { name: "Resume" }));

    expect(
      await screen.findByText(/^Reading worker CPU on r2 every 5s/),
    ).toBeVisible();
    await waitFor(() => expect(openSessions(sessions)).toHaveLength(1));
  });

  describe("in a browser tab that's hidden", () => {
    const setVisibility = (visibilityState: DocumentVisibilityState) => {
      Object.defineProperty(document, "visibilityState", {
        configurable: true,
        get: () => visibilityState,
      });
      document.dispatchEvent(new Event("visibilitychange"));
    };

    afterEach(() => setVisibility("visible"));

    it("closes its session, and reads again once the tab is shown", async () => {
      const { sessions } = await renderWorkers({ r2: "100cc" }, "?replica=r2");
      await waitFor(() => expect(openSessions(sessions)).toHaveLength(1));

      act(() => setVisibility("hidden"));

      expect(
        await screen.findByText("Paused while this browser tab is hidden."),
      ).toBeVisible();
      await waitFor(() => expect(openSessions(sessions)).toEqual([]));

      act(() => setVisibility("visible"));

      expect(
        await screen.findByText(/^Reading worker CPU on r2 every 5s/),
      ).toBeVisible();
      await waitFor(() => expect(openSessions(sessions)).toHaveLength(1));
      expect(sessions).toHaveLength(2);
    });
  });

  it("stops reading when the tab closes", async () => {
    const { sessions, unmount } = await renderWorkers(
      { r2: "100cc" },
      "?replica=r2",
    );
    await waitFor(() => expect(sessions).toHaveLength(1));

    unmount();

    await waitFor(() => expect(openSessions(sessions)).toEqual([]));
  });

  describe("measuring a minute", () => {
    const START_MS = Date.UTC(2026, 9, 16, 12, 7, 30);
    // w1 spends most of its time on u200.
    const SKEWED: WorkerShares = { "1": [0.05, 0.9], "2": [0.04, 0.04] };
    const EVEN: WorkerShares = { "1": [0.3, 0.3], "2": [0.04, 0.04] };

    const clockTime = (sinceStartMs: number) =>
      formatDate(new Date(START_MS + sinceStartMs), TIME_FORMAT);

    // React renders when an `act` ends, so each step ends in one: a request
    // starts from a render, and its response waits on a timer.
    // NOTE: `userEvent` and `waitFor` hang under these timers. Testing
    // Library's async wrapper waits on a `setTimeout` it advances only under
    // Jest, so clicks here are `fireEvent`s.
    const step = (ms: number) =>
      act(async () => {
        await vi.advanceTimersByTimeAsync(ms);
      });

    /** Moves the clock a second at a time. */
    const advance = async (ms: number) => {
      for (let elapsed = 0; elapsed < ms; elapsed += 1000) {
        await step(Math.min(1000, ms - elapsed));
      }
    };

    /** Moves the clock a millisecond at a time until `assertion` passes. */
    const advanceUntil = async (assertion: () => void) => {
      for (let ms = 1; ; ms++) {
        await step(1);
        try {
          assertion();
          return;
        } catch (error) {
          if (ms >= 100) throw error;
        }
      }
    };

    /** Renders the tab on replica `name`, once its first reading is taken. */
    const renderReplica = async (
      perSecond: WorkerShares,
      [name, size] = ["r2", "100cc"],
      metrics?: ServedMetrics,
    ) => {
      const replica = createReplica(perSecond);
      await renderWorkers({ [name]: size }, `?replica=${name}`, {
        replica,
        metrics,
      });
      await advanceUntil(() =>
        expect(
          screen.getByText(
            new RegExp(
              `^Reading worker CPU on ${name} every 5s since ${clockTime(0)}`,
            ),
          ),
        ).toBeVisible(),
      );
      return replica;
    };

    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(START_MS);
      allObjectsCollection.applySnapshot({
        data: OBJECTS,
        snapshotComplete: true,
        error: undefined,
      });
    });

    afterEach(() => allObjectsCollection.reset());

    it("shows a first look while it measures the first minute, then names the overloaded worker and the object behind it", async () => {
      await renderReplica(SKEWED);
      await advance(30_000);

      expect(screen.queryByText(/^Measured /)).not.toBeInTheDocument();
      expect(
        screen.getByText("Measuring r2's 2 workers for a minute (30s of 60s)"),
      ).toBeVisible();
      expect(
        screen.getByRole("button", { name: "Measure again" }),
      ).toBeDisabled();
      expect(screen.queryByText("Workers")).not.toBeInTheDocument();
      expect(screen.queryByText("Objects")).not.toBeInTheDocument();

      await advance(30_000);

      expect(
        screen.getByText(`Measured ${clockTime(0)} to ${clockTime(60_000)}`),
      ).toBeVisible();
      expect(
        screen.getByText(
          "w1 is overloaded, and most of its time goes to orders_by_customer",
        ),
      ).toBeVisible();
      expect(
        screen.queryByText(/^Measuring r2's 2 workers/),
      ).not.toBeInTheDocument();
      expect(
        screen.getByRole("button", { name: "Measure again" }),
      ).toBeEnabled();
    });

    it("shows each worker's time for the measured minute, and the picked object's part of it", async () => {
      await renderReplica(SKEWED);
      await advance(60_000);

      expect(
        screen.getByText(`Measured ${clockTime(0)} to ${clockTime(60_000)}`),
      ).toBeVisible();
      expect(screen.getByText("Workers")).toBeVisible();
      expect(
        screen.getByText(/^% of one core over the measured span/),
      ).toBeVisible();
      expect(screen.getByText("Objects")).toBeVisible();
      expect(
        screen.getByText(
          "Ranked by their time on w1, the busiest worker, which was 94% busy",
        ),
      ).toBeVisible();
      expect(
        screen.getByRole("img", {
          name: "2 workers. The busiest, w1, was 94% busy, and the other workers averaged 9%.",
        }),
      ).toBeVisible();
      const hover = (workerId: number) => {
        fireEvent.pointerEnter(
          document.querySelector(`[data-worker-id="${workerId}"]`)!,
        );
        return screen.getByTestId("chart-tooltip");
      };
      expect(hover(1)).toHaveTextContent("orders_by_customer90%");

      const customersRow = screen.getByRole("row", { name: /customers_idx/ });
      fireEvent.click(within(customersRow).getByText("4% of w1"));

      expect(customersRow).toHaveClass("picked");
      expect(
        screen.getByRole("row", { name: /orders_by_customer/ }),
      ).not.toHaveClass("picked");
      expect(hover(1)).toHaveTextContent("customers_idx4%");
      expect(hover(0)).toHaveTextContent("customers_idx4%");
    });

    it("ranks the objects by a clicked worker's time", async () => {
      await renderReplica(SKEWED);
      await advance(60_000);

      fireEvent.click(document.querySelector('[data-worker-id="0"]')!);

      expect(
        screen.getByRole("group", { name: /w0 is selected/ }),
      ).toBeInTheDocument();
      expect(screen.getByText(/^was 9% busy:/)).toHaveTextContent(
        "w0 was 9% busy: 56% on orders_by_customer, 44% on customers_idx.",
      );
      expect(
        screen.getByText("Ranked by their time on w0, which was 9% busy"),
      ).toBeVisible();
      expect(
        screen.getByRole("columnheader", { name: /Time on w0/ }),
      ).toBeVisible();
      expect(
        within(
          screen.getByRole("row", { name: /orders_by_customer/ }),
        ).getByText("56% of w0"),
      ).toBeVisible();
    });

    it("reads a single-worker replica's minute as how busy its one worker was", async () => {
      await renderReplica({ "1": [0.5], "2": [0.04] }, ["r1", "25cc"]);
      await advance(60_000);

      expect(
        screen.getByRole("heading", {
          name: "r1 runs one worker, so its work can't be uneven",
        }),
      ).toBeVisible();
      expect(
        screen.getByText(
          "Ranked by their time on w0, the only worker, which was 54% busy",
        ),
      ).toBeVisible();
      expect(
        screen.queryByRole("columnheader", { name: /Spread across workers/ }),
      ).not.toBeInTheDocument();
    });

    it("keeps its report while counters arrive, and on Measure again reports the minute after the click", async () => {
      const replica = await renderReplica(SKEWED);
      await advance(60_000);
      const firstSpan = `Measured ${clockTime(0)} to ${clockTime(60_000)}`;
      expect(screen.getByText(firstSpan)).toBeVisible();

      replica.perSecond = EVEN;
      await advance(100_000);

      expect(openSessions(replica.sessions)).toHaveLength(1);
      expect(screen.getByText(firstSpan)).toBeVisible();
      expect(
        screen.getByText(
          "w1 is overloaded, and most of its time goes to orders_by_customer",
        ),
      ).toBeVisible();

      fireEvent.click(screen.getByRole("button", { name: "Measure again" }));

      expect(screen.getByText("Measuring a new minute…")).toBeVisible();
      expect(
        screen.getByRole("button", { name: "Measure again" }),
      ).toBeDisabled();

      await advance(5_000);

      expect(
        screen.getByText("Measuring a new minute… 60s left"),
      ).toBeVisible();
      expect(
        screen.getByText(
          "w1 is overloaded, and most of its time goes to orders_by_customer",
        ),
      ).toBeVisible();

      await advance(60_000);

      expect(
        screen.getByText(
          `Measured ${clockTime(165_000)} to ${clockTime(225_000)}`,
        ),
      ).toBeVisible();
      expect(screen.getByText("Work is spread evenly")).toBeVisible();
      expect(screen.queryByText(/is overloaded/)).not.toBeInTheDocument();
    });

    it("shows how far each measured object's results trail its inputs", async () => {
      await renderReplica(SKEWED);
      await advance(60_000);

      await advanceUntil(() =>
        expect(
          within(
            screen.getByRole("row", { name: /orders_by_customer/ }),
          ).getByText("1m 33s"),
        ).toBeVisible(),
      );
      expect(
        within(screen.getByRole("row", { name: /customers_idx/ })).getByText(
          "Less than 1s",
        ),
      ).toBeVisible();
      expect(
        screen.getByText("orders_by_customer is 1m 33s behind its inputs."),
      ).toBeVisible();
    });

    it("keeps the finding's replica CPU when a sample from the measured minute lands after it", async () => {
      const replicaId = `u${replicaIdBase + 1}`;
      const metrics: ServedMetrics = {
        rows: [processSample(replicaId, START_MS - 30_000, 0.6)],
        connections: [],
      };
      await renderReplica(EVEN, ["r2", "100cc"], metrics);
      await advance(60_000);
      const finding = screen.getByRole("heading", { level: 6 }).textContent;
      expect(finding).not.toMatch(/of its CPU/);

      // 100% in the measured minute's last bucket would lift its average to
      // 80%, past the high-CPU bar.
      for (const connection of metrics.connections) {
        connection.advance(Date.now(), [
          processSample(replicaId, START_MS + 55_000, 1),
        ]);
      }
      await advance(5_000);

      expect(screen.getByRole("heading", { level: 6 })).toHaveTextContent(
        finding ?? "",
      );
      expect(screen.queryByText(/80% of its CPU/)).not.toBeInTheDocument();
    });

    it("keeps the freshness it was measured with when the window regains focus", async () => {
      await renderReplica(SKEWED);
      await advance(60_000);
      const ordersRow = () =>
        within(screen.getByRole("row", { name: /orders_by_customer/ }));
      await advanceUntil(() =>
        expect(ordersRow().getByText("1m 33s")).toBeVisible(),
      );

      server.use(
        buildValidMaterializationLagHandler(
          { objectIds: OBJECTS.map(({ id }) => id) },
          [
            {
              targetObjectId: "u200",
              type: "materialized-view",
              hydrated: true,
              lag: parsePostgresInterval("00:00:02"),
              isOutdated: false,
            },
          ],
        ),
      );
      act(() => {
        focusManager.setFocused(false);
        focusManager.setFocused(true);
      });
      await advance(5_000);

      expect(ordersRow().getByText("1m 33s")).toBeVisible();
      expect(
        screen.getByText("orders_by_customer is 1m 33s behind its inputs."),
      ).toBeVisible();
    });

    it("reads the measured objects' freshness when the next measurement lands, keeping the current report's until then", async () => {
      await renderReplica(SKEWED);
      await advance(60_000);
      const ordersRow = () =>
        within(screen.getByRole("row", { name: /orders_by_customer/ }));
      await advanceUntil(() =>
        expect(ordersRow().getByText("1m 33s")).toBeVisible(),
      );

      fireEvent.click(screen.getByRole("button", { name: "Measure again" }));
      await advance(30_000);
      // Freshness changes while the next minute is measured.
      server.use(
        buildValidMaterializationLagHandler(
          { objectIds: OBJECTS.map(({ id }) => id) },
          [
            {
              targetObjectId: "u200",
              type: "materialized-view",
              hydrated: true,
              lag: parsePostgresInterval("00:00:02"),
              isOutdated: false,
            },
          ],
        ),
      );
      await advance(5_000);
      expect(ordersRow().getByText("1m 33s")).toBeVisible();

      await advance(30_000);
      await advanceUntil(() =>
        expect(ordersRow().getByText("2s")).toBeVisible(),
      );
      expect(screen.queryByText(/behind its inputs\./)).not.toBeInTheDocument();
    });
  });
});
