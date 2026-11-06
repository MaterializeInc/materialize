// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hashKey, QueryKey } from "@tanstack/react-query";
import { act, fireEvent, screen } from "@testing-library/react";
import { millisecondsInMinute } from "date-fns/constants";
import React from "react";

import { DatabaseObject } from "~/api/materialize/objects";
import { SUBSCRIBE_ERROR_CODE } from "~/api/materialize/SubscribeManager";
import { MzDataType } from "~/api/materialize/types";
import { WorkflowGraphEdgeRow } from "~/api/materialize/workflowGraphEdges";
import { WorkflowGraphCanvasNode } from "~/api/materialize/workflowGraphNodes";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { clusterQueryKeys } from "~/platform/clusters/queries";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { workflowGraphEdgesCollection } from "~/store/workflowGraphEdgesCollection";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

import { workflowGraphQueryKeys } from "./queries";
import WorkflowGraph from "./WorkflowGraph";

const environmentAt = (version: string) => ({
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion(`v${version} (0000000000)`),
  },
});
// The first version with mz_object_graph_edges, and the one before it.
const WITH_EDGES = environmentAt("26.39.0");
const WITHOUT_EDGES = environmentAt("26.38.0");
// A failed health check, which drops the version.
const CRASHED = {
  ...healthyEnvironment,
  status: { health: "crashed" as const, errors: [] },
};

const catalogObject = (
  id: string,
  name: string,
  overrides: Partial<DatabaseObject> = {},
): DatabaseObject => ({
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
  ...overrides,
});

const nodeDetails = (object: DatabaseObject): WorkflowGraphCanvasNode => ({
  id: object.id,
  name: object.name,
  databaseName: object.databaseName,
  schemaName: object.schemaName,
  type: object.objectType,
  clusterId: object.clusterId,
  clusterName: object.clusterName,
  sourceType: object.sourceType,
  createdAt: new Date("2026-11-01T00:00:00Z"),
  sourceStatus: null,
  sourceSnapshotCommitted: null,
  sinkStatus: null,
  connectionId: null,
  connectionName: null,
});

//   orders -> revenue_by_region -> top_region
const ORDERS = catalogObject("u10", "orders", { objectType: "table" });
const REVENUE = catalogObject("u11", "revenue_by_region");
const TOP = catalogObject("u12", "top_region");
const OBJECTS = [ORDERS, REVENUE, TOP];
const EDGES: WorkflowGraphEdgeRow[] = [
  { parentId: ORDERS.id, childId: REVENUE.id },
  { parentId: REVENUE.id, childId: TOP.id },
];
const NODE_IDS = OBJECTS.map((object) => object.id).sort();

const nodeTitle = (object: DatabaseObject) =>
  `${object.databaseName}.${object.schemaName}.${object.name}`;

// The canvas and the sidebar's related nodes share a title.
const findNode = async (object: DatabaseObject) =>
  (await screen.findAllByTitle(nodeTitle(object)))[0];

interface LagRow {
  targetObjectId: string;
  hydrated: boolean;
  /** An interval, as the SQL API sends it. */
  lag: string | null;
}

const hydrated = (targetObjectId: string): LagRow => ({
  targetObjectId,
  hydrated: true,
  lag: null,
});

const lagHandler = (
  objectIds: string[],
  rows: LagRow[] = [],
  waitTimeMs?: number,
) =>
  buildSqlQueryHandlerV2(
    {
      queryKey: clusterQueryKeys.materializationLag({ objectIds }),
      results: mapKyselyToTabular({
        rows: rows.map((row) => ({ ...row, isOutdated: false })),
        columns: [
          buildColumn({ name: "targetObjectId" }),
          buildColumn({ name: "hydrated" }),
          buildColumn({ name: "lag", type_oid: MzDataType.interval }),
          buildColumn({ name: "isOutdated" }),
        ],
      }),
    },
    { waitTimeMs },
  );

const detailsHandler = (
  objectIds: string[],
  rows: WorkflowGraphCanvasNode[],
  waitTimeMs?: number,
) =>
  buildSqlQueryHandlerV2(
    {
      queryKey: workflowGraphQueryKeys.nodes({ objectIds }),
      results: mapKyselyToTabular({ rows }),
    },
    { waitTimeMs },
  );

/** Counts the SQL requests sent under `queryKey`. */
const countRequests = (queryKey: QueryKey) => {
  let count = 0;
  const onRequest = ({ request }: { request: Request }) => {
    const key = new URL(request.url).searchParams.get("query_key");
    if (key === hashKey(queryKey)) count++;
  };
  server.events.on("request:start", onRequest);
  onTestFinished(() => {
    server.events.removeListener("request:start", onRequest);
  });
  return () => count;
};

/** The vitest config fakes timeouts but not the interval React Query polls on. */
const fakePollTimers = () =>
  vi.useFakeTimers({
    shouldAdvanceTime: true,
    toFake: [
      "Date",
      "setTimeout",
      "clearTimeout",
      "setInterval",
      "clearInterval",
    ],
  });

const renderAt = async (
  environment: typeof WITH_EDGES,
  focusedObjectId: string,
) =>
  renderComponent(<WorkflowGraph focusedObjectId={focusedObjectId} />, {
    initializeState: ({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", environment),
  });

describe("WorkflowGraph from the edges collection", () => {
  beforeEach(async () => {
    // Query keys embed the environment version, so set it before building
    // the handlers.
    await setFakeEnvironment(getStore().set, "aws/us-east-1", WITH_EDGES);
    allObjectsCollection.applySnapshot({
      data: OBJECTS,
      snapshotComplete: true,
      error: undefined,
    });
    workflowGraphEdgesCollection.applySnapshot({
      data: EDGES,
      snapshotComplete: true,
      error: undefined,
    });
    server.use(lagHandler(NODE_IDS));
  });

  afterEach(() => {
    vi.useRealTimers();
    allObjectsCollection.reset();
    workflowGraphEdgesCollection.reset();
  });

  it("draws the graph before node details load", async () => {
    server.use(
      detailsHandler(NODE_IDS, OBJECTS.map(nodeDetails), 60 * 60 * 1000),
    );
    await renderAt(WITH_EDGES, REVENUE.id);

    for (const object of OBJECTS) {
      expect(await findNode(object)).toBeVisible();
    }
    expect(screen.queryByText("Created")).not.toBeInTheDocument();
  });

  it("shows a renamed object's new name once its details have loaded", async () => {
    server.use(detailsHandler(NODE_IDS, OBJECTS.map(nodeDetails)));
    await renderAt(WITH_EDGES, REVENUE.id);
    // The focused node's details include its creation time.
    expect(await screen.findByText("Created")).toBeVisible();

    const renamed = { ...TOP, name: "best_region" };
    allObjectsCollection.applySnapshot({
      data: [ORDERS, REVENUE, renamed],
      snapshotComplete: true,
      error: undefined,
    });

    expect(await findNode(renamed)).toBeVisible();
    expect(screen.queryAllByTitle(nodeTitle(TOP))).toHaveLength(0);
  });

  it("keeps lag up while a new node's lag loads", async () => {
    server.use(
      detailsHandler(NODE_IDS, OBJECTS.map(nodeDetails)),
      lagHandler(NODE_IDS, [hydrated(REVENUE.id)]),
    );
    await renderAt(WITH_EDGES, REVENUE.id);
    expect(await screen.findByText("Data Freshness")).toBeVisible();

    const added = catalogObject("u13", "low_regions");
    const nextIds = [...NODE_IDS, added.id].sort();
    const slow = 60 * 60 * 1000;
    server.use(
      detailsHandler(nextIds, [...OBJECTS, added].map(nodeDetails), slow),
      lagHandler(nextIds, [hydrated(REVENUE.id)], slow),
    );
    allObjectsCollection.applySnapshot({
      data: [...OBJECTS, added],
      snapshotComplete: true,
      error: undefined,
    });
    workflowGraphEdgesCollection.applySnapshot({
      data: [...EDGES, { parentId: REVENUE.id, childId: added.id }],
      snapshotComplete: true,
      error: undefined,
    });

    expect(await findNode(added)).toBeVisible();
    expect(screen.getByText("Data Freshness")).toBeVisible();
    expect(screen.getByText("Created")).toBeVisible();
  });

  it("shows a node's freshness once it hydrates", async () => {
    fakePollTimers();
    server.use(
      detailsHandler(NODE_IDS, OBJECTS.map(nodeDetails)),
      lagHandler(NODE_IDS, [
        hydrated(REVENUE.id),
        { targetObjectId: TOP.id, hydrated: false, lag: null },
      ]),
    );
    await renderAt(WITH_EDGES, REVENUE.id);
    expect(await screen.findByText("Not hydrated")).toBeVisible();

    server.use(
      lagHandler(NODE_IDS, [
        hydrated(REVENUE.id),
        { targetObjectId: TOP.id, hydrated: true, lag: "00:00:02" },
      ]),
    );
    // Lag history is sampled once a minute.
    await act(() => vi.advanceTimersByTimeAsync(millisecondsInMinute));

    expect(await screen.findByText("2s")).toBeVisible();
    expect(screen.queryByText("Not hydrated")).not.toBeInTheDocument();
  });

  it("polls a node that never hydrates at most once a minute", async () => {
    fakePollTimers();
    server.use(
      detailsHandler(NODE_IDS, OBJECTS.map(nodeDetails)),
      lagHandler(NODE_IDS, [
        hydrated(REVENUE.id),
        { targetObjectId: TOP.id, hydrated: false, lag: null },
      ]),
    );
    await renderAt(WITH_EDGES, REVENUE.id);
    const lagRequests = countRequests(
      clusterQueryKeys.materializationLag({ objectIds: NODE_IDS }),
    );
    expect(await screen.findByText("Not hydrated")).toBeVisible();

    await act(() => vi.advanceTimersByTimeAsync(5 * millisecondsInMinute));
    const afterFiveMinutes = lagRequests();
    await act(() => vi.advanceTimersByTimeAsync(5 * millisecondsInMinute));

    const lastFiveMinutes = lagRequests() - afterFiveMinutes;
    expect(lastFiveMinutes).toBeGreaterThanOrEqual(4);
    expect(lastFiveMinutes).toBeLessThanOrEqual(5);
  });

  it("selects the focus again when the selected node is dropped", async () => {
    const remainingIds = [ORDERS.id, REVENUE.id];
    server.use(
      detailsHandler(NODE_IDS, OBJECTS.map(nodeDetails)),
      detailsHandler(remainingIds, [ORDERS, REVENUE].map(nodeDetails)),
      lagHandler(remainingIds),
    );
    await renderAt(WITH_EDGES, REVENUE.id);
    fireEvent.click(await findNode(TOP));
    expect(await screen.findByTitle(TOP.name)).toBeVisible();
    expect(screen.queryByTitle(REVENUE.name)).not.toBeInTheDocument();

    allObjectsCollection.applySnapshot({
      data: [ORDERS, REVENUE],
      snapshotComplete: true,
      error: undefined,
    });
    workflowGraphEdgesCollection.applySnapshot({
      data: [EDGES[0]],
      snapshotComplete: true,
      error: undefined,
    });

    expect(await screen.findByTitle(REVENUE.name)).toBeVisible();
    expect(screen.queryByTitle(TOP.name)).not.toBeInTheDocument();
  });

  it("waits for a new session's edges to draw an object the held edges predate", async () => {
    const added = catalogObject("u13", "low_regions");
    const addedIds = [ORDERS.id, REVENUE.id, added.id];
    server.use(
      detailsHandler([added.id], [nodeDetails(added)]),
      detailsHandler(addedIds, [ORDERS, REVENUE, added].map(nodeDetails)),
      lagHandler(addedIds),
    );
    allObjectsCollection.applySnapshot({
      data: [...OBJECTS, added],
      snapshotComplete: true,
      error: undefined,
    });
    // A new session's empty pre-snapshot, over edges held from before `added`.
    workflowGraphEdgesCollection.applySnapshot({
      data: [],
      snapshotComplete: false,
      error: undefined,
    });
    await renderAt(WITH_EDGES, added.id);
    expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    expect(screen.queryAllByTitle(nodeTitle(added))).toHaveLength(0);

    workflowGraphEdgesCollection.applySnapshot({
      data: [...EDGES, { parentId: REVENUE.id, childId: added.id }],
      snapshotComplete: true,
      error: undefined,
    });

    expect(await findNode(added)).toBeVisible();
    expect(await findNode(REVENUE)).toBeVisible();
  });

  it("keeps the graph and selection through a dropped connection", async () => {
    server.use(detailsHandler(NODE_IDS, OBJECTS.map(nodeDetails)));
    await renderAt(WITH_EDGES, REVENUE.id);
    fireEvent.click(await findNode(TOP));
    expect(await screen.findByTitle(TOP.name)).toBeVisible();

    workflowGraphEdgesCollection.applySnapshot({
      data: EDGES,
      snapshotComplete: true,
      error: {
        code: SUBSCRIBE_ERROR_CODE.CONNECTION_CLOSED,
        message: "Socket closed unexpectedly",
      },
    });

    expect(await findNode(ORDERS)).toBeVisible();
    expect(screen.getByTitle(TOP.name)).toBeVisible();
    expect(
      screen.queryByText("An error occurred fetching workflow data."),
    ).not.toBeInTheDocument();
  });

  it("shows an error when the connection drops before any edges load", async () => {
    workflowGraphEdgesCollection.reset();
    workflowGraphEdgesCollection.applySnapshot({
      data: [],
      snapshotComplete: false,
      error: {
        code: SUBSCRIBE_ERROR_CODE.CONNECTION_CLOSED,
        message: "Socket closed unexpectedly",
      },
    });
    await renderAt(WITH_EDGES, REVENUE.id);

    expect(
      await screen.findByText("An error occurred fetching workflow data."),
    ).toBeVisible();
  });

  it("keeps the graph through a failed health check", async () => {
    server.use(detailsHandler(NODE_IDS, OBJECTS.map(nodeDetails)));
    await renderAt(WITH_EDGES, REVENUE.id);
    expect(await findNode(TOP)).toBeVisible();

    await setFakeEnvironment(getStore().set, "aws/us-east-1", CRASHED);

    // The SQL path would suspend on its own query in place of the canvas.
    expect(await findNode(TOP)).toBeVisible();
    expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
  });

  it("waits for the edges snapshot", async () => {
    workflowGraphEdgesCollection.reset();
    server.use(detailsHandler([REVENUE.id], [nodeDetails(REVENUE)]));
    await renderAt(WITH_EDGES, REVENUE.id);

    expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    expect(screen.queryAllByTitle(nodeTitle(REVENUE))).toHaveLength(0);
  });

  it("shows an error when the catalog SUBSCRIBE fails", async () => {
    allObjectsCollection.applySnapshot({
      data: OBJECTS,
      snapshotComplete: true,
      error: { code: "XX000", message: "subscribe failed" },
    });
    await renderAt(WITH_EDGES, REVENUE.id);

    expect(
      await screen.findByText("An error occurred fetching workflow data."),
    ).toBeVisible();
  });

  it("shows an error when the edges SUBSCRIBE fails", async () => {
    workflowGraphEdgesCollection.reset();
    workflowGraphEdgesCollection.applySnapshot({
      data: [],
      snapshotComplete: false,
      error: { code: "XX000", message: "subscribe failed" },
    });
    await renderAt(WITH_EDGES, REVENUE.id);

    expect(
      await screen.findByText("An error occurred fetching workflow data."),
    ).toBeVisible();
  });

  it("has no graph for a progress source", async () => {
    const progress = catalogObject("u20", "src_progress", {
      objectType: "source",
      sourceType: "progress",
    });
    allObjectsCollection.applySnapshot({
      data: [...OBJECTS, progress],
      snapshotComplete: true,
      error: undefined,
    });
    server.use(
      lagHandler([progress.id]),
      detailsHandler([progress.id], [nodeDetails(progress)]),
    );
    await renderAt(WITH_EDGES, progress.id);

    expect(
      await screen.findByText("Unable to render workflow graph."),
    ).toBeVisible();
  });
});

describe("WorkflowGraph before mz_object_graph_edges", () => {
  beforeEach(async () => {
    await setFakeEnvironment(getStore().set, "aws/us-east-1", WITHOUT_EDGES);
  });

  it("draws the graph from the SQL query", async () => {
    // The query's own order: each edge's parent, then its child.
    const queriedIds = [ORDERS.id, REVENUE.id, TOP.id];
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: workflowGraphQueryKeys.graph({ objectId: REVENUE.id }),
        results: mapKyselyToTabular({ rows: EDGES }),
      }),
      detailsHandler(queriedIds, OBJECTS.map(nodeDetails)),
      lagHandler(queriedIds),
    );
    await renderAt(WITHOUT_EDGES, REVENUE.id);

    for (const object of OBJECTS) {
      expect(await findNode(object)).toBeVisible();
    }
  });
});
