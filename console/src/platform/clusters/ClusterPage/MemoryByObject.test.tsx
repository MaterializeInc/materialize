// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { http } from "msw";
import React from "react";

import { Cluster, Replica } from "~/api/materialize/cluster/clusterList";
import { DatabaseObject } from "~/api/materialize/objects";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
  SqlHandlerOptions,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { buildClusterServerResponse } from "~/platform/clusters/clustersTestUtils";
import { clusterQueryKeys } from "~/platform/clusters/queries";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { syncEngineCacheScopeAtom } from "~/store/syncEngineCache";
import { mockSubscribeSocket } from "~/test/mockSubscribeSocket";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { assert } from "~/util";
import { parseDbVersion } from "~/version/api";

import { MemoryByObject } from "./MemoryByObject";
import { otherMemory, segmentWidths } from "./memoryByObjectModel";
import { sizesByReplica } from "./useLiveLargestObjects";

const flags = vi.hoisted(() => ({ values: {} as Record<string, boolean> }));

vi.mock("~/hooks/useFlags", () => ({ useFlags: () => flags.values }));

const HEAP_LIMIT = 4069523456;

const buildReplica = (id: string, name: string): Replica => ({
  id,
  name,
  size: "25cc",
  disk: true,
  statuses: [],
});

const buildCluster = (replicas: Replica[]) =>
  buildClusterServerResponse({ id: "u1", name: "quickstart" }, replicas);

/**
 * Answers the page's heap reads: each process's latest heap limit and use, by
 * replica id, and the size's process count. Build after setting the
 * environment, since the query keys embed its version.
 */
const heapReadsHandlers = (
  cluster: Cluster,
  {
    processes = 1,
    heapLimit = String(HEAP_LIMIT),
    heapBytes = {},
  }: {
    processes?: number;
    heapLimit?: string | null;
    /** Each process's heap use, by replica id. Unlisted replicas report none. */
    heapBytes?: Record<string, string>;
  } = {},
) => [
  buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.replicaHeapLimits({
      replicaIds: cluster.replicas.map(({ id }) => id).sort(),
    }),
    results: mapKyselyToTabular({
      columns: buildColumns([
        "replicaId",
        "processId",
        "heapLimit",
        "memoryBytes",
        "heapBytes",
      ]),
      rows: cluster.replicas.flatMap(({ id }) =>
        Array.from({ length: processes }, (_, processId) => ({
          replicaId: id,
          processId: String(processId),
          heapLimit,
          memoryBytes: "1",
          heapBytes: heapBytes[id] ?? null,
        })),
      ),
    }),
  }),
  buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.replicaSizeAllocations({ sizes: ["25cc"] }),
    results: mapKyselyToTabular({
      columns: [
        buildColumn({ name: "size" }),
        ...["processes", "cpuNanoCores", "memoryBytes", "diskBytes"].map(
          (name) => buildColumn({ name, type_oid: MzDataType.float8 }),
        ),
      ],
      rows: [
        {
          size: "25cc",
          processes: String(processes),
          cpuNanoCores: "1",
          memoryBytes: "1",
          diskBytes: "1",
        },
      ],
    }),
  }),
];

/** Counts largest replica queries, leaving their answers to other handlers. */
const countLargestReplicaQueries = () => {
  const requests = { count: 0 };
  const handler = http.post("*/api/sql", ({ request }) => {
    const queryKey = JSON.parse(
      new URL(request.url).searchParams.get("query_key") ?? "[]",
    );
    if (queryKey[1]?.scope === "largestClusterReplica") requests.count += 1;
    return undefined;
  });
  return { requests, handler };
};

/**
 * Holds requests whose query key part matches `matches` until `release` is
 * called, leaving their answers to other handlers. `requested` resolves on the
 * first held request.
 */
const holdQueries = (
  matches: (keyPart: Record<string, unknown>) => boolean,
) => {
  let release = () => {};
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  let markRequested = () => {};
  const requested = new Promise<void>((resolve) => {
    markRequested = resolve;
  });
  const handler = http.post("*/api/sql", async ({ request }) => {
    const queryKey = JSON.parse(
      new URL(request.url).searchParams.get("query_key") ?? "[]",
    );
    if (queryKey[1] && matches(queryKey[1])) {
      markRequested();
      await released;
    }
    return undefined;
  });
  return { handler, requested, release };
};

/**
 * The card under a replica select, as the Metrics page lays them out. Picks
 * in a transition, as react-router's navigation does.
 */
const ReplicaSwitcher = ({
  cluster,
  initialReplicaName,
}: {
  cluster: Cluster;
  initialReplicaName?: string;
}) => {
  const [replicaName, setReplicaName] = React.useState(initialReplicaName);
  return (
    <>
      <select
        aria-label="Replica"
        value={replicaName ?? ""}
        onChange={(event) => {
          const name = event.target.value || undefined;
          React.startTransition(() => setReplicaName(name));
        }}
      >
        <option value="">All replicas</option>
        {cluster.replicas.map(({ id, name }) => (
          <option key={id} value={name}>
            {name}
          </option>
        ))}
      </select>
      <MemoryByObject
        cluster={cluster}
        replica={cluster.replicas.find(({ name }) => name === replicaName)}
      />
    </>
  );
};

/** The select's value for all replicas. */
const ALL_REPLICAS = "";

const selectReplica = (name: string) =>
  fireEvent.change(screen.getByRole("combobox", { name: "Replica" }), {
    target: { value: name },
  });

const largestReplicaHandler = (
  rows: Array<Record<string, string | null>>,
  options?: SqlHandlerOptions,
) =>
  buildSqlQueryHandlerV2(
    {
      queryKey: clusterQueryKeys.largestClusterReplica({ clusterId: "u1" }),
      results: mapKyselyToTabular({
        columns: buildColumns([
          "id",
          "name",
          "size",
          { type_oid: MzDataType.numeric, name: "heapLimit" },
        ]),
        rows,
      }),
    },
    options,
  );

const largestObjectsKey = (
  replicaHeapLimit: number | null = HEAP_LIMIT,
  replicaName = "r1",
) =>
  clusterQueryKeys.largestMaintainedQueries({
    clusterId: "u1",
    clusterName: "quickstart",
    limit: 10,
    replicaName,
    replicaHeapLimit,
    unifiedSizes: false,
  });

const largestObjectsHandler = (
  rows: Array<Record<string, string | null>> = [
    {
      id: "u188",
      name: "customer_view",
      size: "5469140917",
      memoryPercentage: "31.8345902256",
      type: "materialized-view",
      schemaName: "public",
      databaseName: "materialize",
      dataflowId: "7",
      dataflowName: "Dataflow: materialize.public.customer_view",
    },
    // The dataflow of a dropped object: the left join to mz_objects finds no
    // row, so even its id is NULL.
    {
      id: null,
      name: null,
      size: "424919434",
      memoryPercentage: "11.2686157226",
      type: null,
      schemaName: null,
      databaseName: null,
      dataflowId: "124",
      dataflowName: "Dataflow: materialize.deleted_schema.orphaned_view",
    },
  ],
  replicaHeapLimit: number | null = HEAP_LIMIT,
  replicaName = "r1",
) =>
  buildSqlQueryHandlerV2({
    queryKey: largestObjectsKey(replicaHeapLimit, replicaName),
    results: mapKyselyToTabular({
      columns: buildColumns([
        "id",
        "name",
        "size",
        { type_oid: MzDataType.numeric, name: "memoryPercentage" },
        "type",
        "schemaName",
        "databaseName",
        "dataflowId",
        "dataflowName",
      ]),
      rows,
    }),
  });

const R1 = buildReplica("u678", "r1");
const R2 = buildReplica("u679", "r2");

const R2_OBJECTS = [
  {
    id: "u404",
    name: "order_totals",
    size: "1073741824",
    memoryPercentage: "26.3852",
    type: "index",
    schemaName: "public",
    databaseName: "materialize",
    dataflowId: "9",
    dataflowName: "Dataflow: materialize.public.order_totals",
  },
];

const renderMemoryByObject = (cluster = buildCluster([R1])) => {
  server.use(...heapReadsHandlers(cluster));
  return renderComponent(<MemoryByObject cluster={cluster} />);
};

describe("MemoryByObject", () => {
  beforeEach(() => {
    flags.values = {};
  });

  it("lists each object's memory on the largest replica", async () => {
    flags.values = { "maintained-objects-ui-50": true };
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler(),
    );
    await renderMemoryByObject();

    // The title shows while loading too, so wait on the subtitle.
    expect(await screen.findByText("% of heap limit on r1")).toBeVisible();
    expect(screen.getByText("Objects by Memory")).toBeVisible();
    const objects = screen.getByRole("list", { name: "Objects" });
    expect(
      within(objects).getByRole("link", { name: "customer_view" }),
    ).toHaveAttribute(
      "href",
      expect.stringMatching(/\/maintained-objects\/u188$/),
    );
    expect(within(objects).getByText("5.09 GB (31.8%)")).toBeVisible();
  });

  it("reads a named replica from the page instead of querying the largest", async () => {
    const cluster = buildCluster([R1]);
    const { requests, handler } = countLargestReplicaQueries();
    server.use(handler, ...heapReadsHandlers(cluster), largestObjectsHandler());
    await renderComponent(<MemoryByObject cluster={cluster} replica={R1} />);

    expect(await screen.findByText("% of heap limit on r1")).toBeVisible();
    expect(screen.getByText("5.09 GB (31.8%)")).toBeVisible();
    expect(requests.count).toEqual(0);
  });

  it("links to the workflow graph while the maintained objects page is off", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler(),
    );
    await renderMemoryByObject();

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(
      within(objects).getByRole("link", { name: "customer_view" }),
    ).toHaveAttribute("href", expect.stringMatching(/\/workflow$/));
  });

  it("names a dropped object's dataflow without linking it", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler(),
    );
    await renderMemoryByObject();

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(within(objects).getByText("orphaned_view")).toBeVisible();
    expect(
      within(objects).queryByRole("link", { name: "orphaned_view" }),
    ).not.toBeInTheDocument();
  });

  it("lists sizes without a bar when the replica reports no heap limit", async () => {
    server.use(
      largestReplicaHandler([{ name: "r1", size: "25cc", heapLimit: null }]),
      largestObjectsHandler(
        [
          {
            id: "u188",
            name: "customer_view",
            size: "5469140917",
            memoryPercentage: null,
            type: "materialized-view",
            schemaName: "public",
            databaseName: "materialize",
            dataflowId: "7",
            dataflowName: "Dataflow: materialize.public.customer_view",
          },
        ],
        null,
      ),
    );
    await renderMemoryByObject();

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(within(objects).getByText("5.09 GB")).toBeVisible();
    expect(screen.getByText("Size on r1")).toBeVisible();
    expect(
      screen.queryByRole("img", { name: "Memory by object" }),
    ).not.toBeInTheDocument();
  });

  it("shows the card's title while loading", async () => {
    server.use(largestReplicaHandler([], { waitTimeMs: "infinite" }));
    await renderMemoryByObject();

    expect(
      screen.getByRole("heading", { name: "Objects by Memory" }),
    ).toBeVisible();
    expect(screen.getByTestId("loading-spinner")).toBeVisible();
  });

  it("says when the replica has no objects", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler([]),
    );
    await renderMemoryByObject();

    expect(
      await screen.findByText("No objects are using memory on r1."),
    ).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Objects by Memory" }),
    ).toBeVisible();
  });

  it("shows nothing for a cluster without replicas", async () => {
    server.use(largestReplicaHandler([]));
    await renderMemoryByObject(buildCluster([]));

    await waitFor(() =>
      expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument(),
    );
    expect(document.body.textContent).toEqual("");
  });

  it("says the replica may be busy when the objects fail to load", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      buildSqlQueryHandlerV2({
        queryKey: largestObjectsKey(),
        results: {
          error: {
            code: ErrorCode.INTERNAL_ERROR,
            message: "largestMaintainedQueries failed",
          },
          notices: [],
        },
      }),
    );
    await renderMemoryByObject();

    expect(
      await screen.findByText(/from replica r1, which might mean it's busy/),
    ).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Objects by Memory" }),
    ).toBeVisible();
  });

  it("leaves a replica's load error behind when another replica is picked", async () => {
    const cluster = buildCluster([R1, R2]);
    server.use(
      ...heapReadsHandlers(cluster),
      buildSqlQueryHandlerV2({
        queryKey: largestObjectsKey(),
        results: {
          error: {
            code: ErrorCode.INTERNAL_ERROR,
            message: "largestMaintainedQueries failed",
          },
          notices: [],
        },
      }),
      largestObjectsHandler(R2_OBJECTS, HEAP_LIMIT, "r2"),
    );
    await renderComponent(
      <ReplicaSwitcher cluster={cluster} initialReplicaName="r1" />,
    );
    expect(
      await screen.findByText(/from replica r1, which might mean it's busy/),
    ).toBeVisible();

    selectReplica("r2");
    expect(await screen.findByText("% of heap limit on r2")).toBeVisible();
    expect(screen.getByText("order_totals")).toBeVisible();
    expect(screen.queryByText(/might mean it's busy/)).not.toBeInTheDocument();
  });

  it("keeps the previous replica's card up while the next replica's objects load", async () => {
    const cluster = buildCluster([R1, R2]);
    const r2Objects = holdQueries(
      ({ scope, replicaName }) =>
        scope === "largestMaintainedQueries" && replicaName === "r2",
    );
    server.use(
      r2Objects.handler,
      ...heapReadsHandlers(cluster),
      largestObjectsHandler(),
      largestObjectsHandler(R2_OBJECTS, HEAP_LIMIT, "r2"),
    );
    await renderComponent(
      <ReplicaSwitcher cluster={cluster} initialReplicaName="r1" />,
    );
    expect(await screen.findByText("% of heap limit on r1")).toBeVisible();

    selectReplica("r2");
    await r2Objects.requested;
    expect(screen.getByText("% of heap limit on r1")).toBeVisible();
    expect(screen.getByText("customer_view")).toBeVisible();
    expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();

    r2Objects.release();
    expect(await screen.findByText("% of heap limit on r2")).toBeVisible();
    expect(screen.getByText("order_totals")).toBeVisible();
  });
});

// Sizes stream from mz_object_arrangement_sizes on 26.35 and later.
const LIVE_SIZES_ENVIRONMENT = {
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion("v26.45.0 (0000000000)"),
  },
};
const GIB = 2 ** 30;

const catalogObject = (id: string, name: string): DatabaseObject => ({
  databaseName: "materialize",
  databaseId: "u1",
  name,
  schemaName: "public",
  schemaId: "u2",
  id,
  objectType: "materialized-view",
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u1",
  clusterName: "quickstart",
});

const sizesSnapshot = (rows: Array<[string, number]>) => ({
  columns: [
    buildColumn({ name: "objectId" }),
    buildColumn({ name: "size", type_oid: MzDataType.float8 }),
  ],
  rows: rows.map(([objectId, size]) => [objectId, String(size)]),
});

const setLiveSizesEnvironment = () =>
  setFakeEnvironment(getStore().set, "aws/us-east-1", LIVE_SIZES_ENVIRONMENT);

const renderLive = (element: React.ReactElement) =>
  renderComponent(element, {
    initializeState: ({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", LIVE_SIZES_ENVIRONMENT),
  });

// Query keys embed the environment version, so set the environment before
// building the handlers that match on them. Each test streams replicas of its
// own: the sizes collections outlive a test.
const renderLiveMemoryByObject = async ({
  replicaId,
  heapLimit = String(HEAP_LIMIT),
}: {
  replicaId: string;
  heapLimit?: string | null;
}) => {
  await setLiveSizesEnvironment();
  const cluster = buildCluster([buildReplica(replicaId, "r1")]);
  server.use(
    ...heapReadsHandlers(cluster),
    largestReplicaHandler([
      { id: replicaId, name: "r1", size: "25cc", heapLimit },
    ]),
    mockSubscribeSocket((sql, params) =>
      sql.includes("mz_object_arrangement_sizes") && params.includes(replicaId)
        ? sizesSnapshot([
            ["u188", 2 * GIB],
            ["u404", GIB],
          ])
        : undefined,
    ),
  );
  return renderLive(<MemoryByObject cluster={cluster} />);
};

describe("MemoryByObject with streamed sizes", () => {
  beforeEach(() => {
    flags.values = {};
    allObjectsCollection.applySnapshot({
      data: [catalogObject("u188", "customer_view")],
      snapshotComplete: true,
      error: undefined,
    });
  });

  it("names streamed objects from the catalog", async () => {
    await renderLiveMemoryByObject({ replicaId: "u5" });

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(
      within(objects).getByRole("link", { name: "customer_view" }),
    ).toBeVisible();
    // Not in the catalog, so a dropped object's dataflow.
    expect(within(objects).getByText("u404")).toBeVisible();
    expect(
      within(objects).queryByRole("link", { name: "u404" }),
    ).not.toBeInTheDocument();
  });

  it("lists sizes without a bar when the replica reports no heap limit", async () => {
    await renderLiveMemoryByObject({ replicaId: "u6", heapLimit: null });

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(within(objects).getByText("customer_view")).toBeVisible();
    expect(within(objects).getByText("2.00 GB")).toBeVisible();
    expect(screen.getByText("Size on r1")).toBeVisible();
    expect(
      screen.queryByRole("img", { name: "Memory by object" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText(/Infinity|NaN/)).not.toBeInTheDocument();
  });

  it("waits for the catalog before naming objects", async () => {
    allObjectsCollection.reset();
    await renderLiveMemoryByObject({ replicaId: "u7" });

    const scope = await getStore().get(syncEngineCacheScopeAtom);
    assert(scope);
    const sizes = sizesByReplica.get(scope, "u7");
    await waitFor(() =>
      expect(getStore().get(sizes.statusAtom).snapshotComplete).toBe(true),
    );
    expect(screen.getByTestId("loading-spinner")).toBeVisible();
    expect(screen.queryByText("u188")).not.toBeInTheDocument();
    allObjectsCollection.applySnapshot({
      data: [catalogObject("u188", "customer_view")],
      snapshotComplete: true,
      error: undefined,
    });
    expect(await screen.findByText("customer_view")).toBeVisible();
  });

  it("takes a named replica's heap limit across its processes", async () => {
    await setLiveSizesEnvironment();
    const replica = buildReplica("u20", "r1");
    const cluster = buildCluster([replica]);
    const { requests, handler } = countLargestReplicaQueries();
    server.use(
      handler,
      ...heapReadsHandlers(cluster, {
        processes: 2,
        heapLimit: String(4 * GIB),
      }),
      mockSubscribeSocket((sql, params) =>
        params.includes("u20") ? sizesSnapshot([["u188", 2 * GIB]]) : undefined,
      ),
    );
    await renderLive(<MemoryByObject cluster={cluster} replica={replica} />);

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(within(objects).getByText("2.00 GB (25.0%)")).toBeVisible();
    expect(
      screen.getByText("% of heap limit across 2 processes on r1"),
    ).toBeVisible();
    expect(requests.count).toEqual(0);
  });

  it("keeps the previous replica's objects on screen until the next replica's snapshot", async () => {
    await setLiveSizesEnvironment();
    const cluster = buildCluster([
      buildReplica("u21", "r1"),
      buildReplica("u22", "r2"),
    ]);
    const { requests, handler } = countLargestReplicaQueries();
    let releaseR2 = () => {};
    const r2Released = new Promise<void>((resolve) => {
      releaseR2 = resolve;
    });
    server.use(
      handler,
      ...heapReadsHandlers(cluster),
      mockSubscribeSocket(async (sql, params) => {
        if (params.includes("u21")) return sizesSnapshot([["u188", GIB]]);
        if (!params.includes("u22")) return undefined;
        await r2Released;
        return sizesSnapshot([["u404", 2 * GIB]]);
      }),
    );
    await renderLive(
      <ReplicaSwitcher cluster={cluster} initialReplicaName="r1" />,
    );
    expect(await screen.findByText("customer_view")).toBeVisible();

    selectReplica("r2");
    expect(await screen.findByTestId("refreshing-spinner")).toBeVisible();
    expect(screen.getByText("% of heap limit on r1")).toBeVisible();
    expect(screen.getByText("customer_view")).toBeVisible();
    expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();

    releaseR2();
    expect(await screen.findByText("% of heap limit on r2")).toBeVisible();
    expect(screen.getByText("u404")).toBeVisible();
    expect(screen.queryByText("customer_view")).not.toBeInTheDocument();
    expect(screen.queryByTestId("refreshing-spinner")).not.toBeInTheDocument();
    expect(requests.count).toEqual(0);
  });

  it("queries the largest replica once all replicas are selected", async () => {
    await setLiveSizesEnvironment();
    const cluster = buildCluster([
      buildReplica("u23", "r1"),
      buildReplica("u24", "r2"),
    ]);
    const { requests, handler } = countLargestReplicaQueries();
    server.use(
      handler,
      ...heapReadsHandlers(cluster),
      largestReplicaHandler([
        { id: "u24", name: "r2", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      mockSubscribeSocket((sql, params) =>
        params.includes("u23") || params.includes("u24")
          ? sizesSnapshot([["u188", GIB]])
          : undefined,
      ),
    );
    await renderLive(
      <ReplicaSwitcher cluster={cluster} initialReplicaName="r1" />,
    );
    expect(await screen.findByText("% of heap limit on r1")).toBeVisible();
    expect(requests.count).toEqual(0);

    selectReplica(ALL_REPLICAS);
    expect(await screen.findByText("% of heap limit on r2")).toBeVisible();
    expect(requests.count).toEqual(1);
  });

  it("keeps a linked replica's objects on screen until the largest replica is known", async () => {
    await setLiveSizesEnvironment();
    const cluster = buildCluster([
      buildReplica("u25", "r1"),
      buildReplica("u26", "r2"),
    ]);
    const largestReplica = holdQueries(
      ({ scope }) => scope === "largestClusterReplica",
    );
    server.use(
      largestReplica.handler,
      ...heapReadsHandlers(cluster),
      largestReplicaHandler([
        { id: "u26", name: "r2", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      mockSubscribeSocket((sql, params) => {
        if (params.includes("u25")) return sizesSnapshot([["u188", GIB]]);
        if (params.includes("u26")) return sizesSnapshot([["u404", 2 * GIB]]);
        return undefined;
      }),
    );
    await renderLive(
      <ReplicaSwitcher cluster={cluster} initialReplicaName="r1" />,
    );
    expect(await screen.findByText("customer_view")).toBeVisible();

    selectReplica(ALL_REPLICAS);
    await largestReplica.requested;
    expect(screen.getByTestId("refreshing-spinner")).toBeVisible();
    expect(screen.getByText("% of heap limit on r1")).toBeVisible();
    expect(screen.getByText("customer_view")).toBeVisible();
    expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();

    largestReplica.release();
    expect(await screen.findByText("% of heap limit on r2")).toBeVisible();
    expect(screen.getByText("u404")).toBeVisible();
    expect(screen.queryByTestId("refreshing-spinner")).not.toBeInTheDocument();
  });

  describe("other memory", () => {
    const renderOtherMemory = async ({
      replicaId,
      heapLimit = String(4 * GIB),
      heapBytes,
    }: {
      replicaId: string;
      heapLimit?: string | null;
      heapBytes?: string;
    }) => {
      await setLiveSizesEnvironment();
      const replica = buildReplica(replicaId, "r1");
      const cluster = buildCluster([replica]);
      server.use(
        ...heapReadsHandlers(cluster, {
          processes: 2,
          heapLimit,
          heapBytes: heapBytes ? { [replicaId]: heapBytes } : {},
        }),
        mockSubscribeSocket((sql, params) =>
          params.includes(replicaId)
            ? sizesSnapshot([
                ["u188", 2 * GIB],
                ["u404", GIB],
              ])
            : undefined,
        ),
      );
      await renderLive(<MemoryByObject cluster={cluster} replica={replica} />);
      return screen.findByRole("list", { name: "Objects" });
    };

    it("sizes it by the heap in use beyond the listed objects", async () => {
      // 6 GiB in use over two processes, 3 GiB of it listed, of 8 GiB.
      const objects = await renderOtherMemory({
        replicaId: "u30",
        heapBytes: String(3 * GIB),
      });

      const other = within(objects).getByText("Other memory").closest("li");
      assert(other);
      expect(within(other).getByText("3.00 GB (37.5%)")).toBeVisible();
      expect(
        within(other).getByLabelText(
          "Heap in use beyond the objects listed: smaller objects, and memory that arrangement sizes don't count.",
        ),
      ).toBeInTheDocument();
    });

    it("leaves it out while the replica reports no heap use", async () => {
      const objects = await renderOtherMemory({ replicaId: "u31" });

      expect(within(objects).getByText("customer_view")).toBeVisible();
      expect(
        within(objects).queryByText("Other memory"),
      ).not.toBeInTheDocument();
    });

    it("leaves it out while the replica reports no heap limit", async () => {
      const objects = await renderOtherMemory({
        replicaId: "u32",
        heapLimit: null,
        heapBytes: String(3 * GIB),
      });

      expect(within(objects).getByText("2.00 GB")).toBeVisible();
      expect(
        within(objects).queryByText("Other memory"),
      ).not.toBeInTheDocument();
    });
  });
});

describe("otherMemory", () => {
  it("is the heap in use beyond the listed sizes, as a share of the limit", () => {
    expect(
      otherMemory(600, 1000, [
        { size: 200 },
        { size: BigInt(100) },
        { size: null },
      ]),
    ).toEqual({ size: 300, memoryPercentage: 30 });
  });

  it("is undefined when the listed sizes cover the heap in use", () => {
    expect(otherMemory(300, 1000, [{ size: 200 }, { size: 100 }])).toBe(
      undefined,
    );
  });
});

describe("segmentWidths", () => {
  it("stops the bar at the heap limit", () => {
    expect(segmentWidths([60, 30, 25, null])).toEqual([60, 30, 10, 0]);
  });
});
