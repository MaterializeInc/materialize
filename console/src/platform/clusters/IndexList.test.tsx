// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hashKey } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { Index } from "~/api/materialize/cluster/indexesList";
import { ErrorCode } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { buildValidMaterializationLagHandler } from "~/test/clusterQueryBuilders";
import { mockSubscribeState } from "~/test/mockSubscribe";
import {
  arrangmentMemoryColumns,
  clusterReplicasColumns,
} from "~/test/queries";
import { renderComponent } from "~/test/utils";

import { detailPageSetupHelpers } from "./clustersTestUtils";
import IndexList from "./IndexList";
import { clusterQueryKeys } from "./queries";

const { detailPageCluster, detailPageInitialRouteEntries } =
  detailPageSetupHelpers();

function buildIndex(overrides?: Partial<Index>) {
  return {
    id: "u123",
    name: "test_index",
    schemaName: "public",
    databaseName: "materialize",
    relationName: "test_view_1",
    indexedColumns: ["test_col"],
    relationType: "view",
    ...overrides,
  };
}

const indexesListColumns = buildColumns([
  "id",
  "databaseName",
  "schemaName",
  "name",
  "relationName",
  "relationType",
]);

const emptyIndexesListHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.indexesList({
    clusterId: detailPageCluster.id,
    includeSystemObjects: false,
  }),
  results: mapKyselyToTabular({
    rows: [],
    columns: indexesListColumns,
  }),
});
const testIndex = buildIndex();
const validIndexesListHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.indexesList({
    clusterId: detailPageCluster.id,
    includeSystemObjects: false,
  }),
  results: mapKyselyToTabular({
    rows: [testIndex],
    columns: indexesListColumns,
  }),
});
const testReplicaSize = {
  name: detailPageCluster.replicas[0].name,
  memoryBytes: "4294967296",
  diskBytes: "1000",
  heapLimit: "2147483648",
  size: "25cc",
};
const validReplicaSizesHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.replicas({
    clusterId: detailPageCluster.id,
    orderBy: "heapLimit",
  }),
  results: mapKyselyToTabular({
    rows: [testReplicaSize],
    columns: clusterReplicasColumns,
  }),
});
const pendingReplicaSizesHandler = buildSqlQueryHandlerV2(
  {
    queryKey: clusterQueryKeys.replicas({
      clusterId: detailPageCluster.id,
      orderBy: "heapLimit",
    }),
    results: mapKyselyToTabular({
      rows: [testReplicaSize],
      columns: clusterReplicasColumns,
    }),
  },
  { waitTimeMs: "infinite" },
);
const emptyArrangmentMemoryHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.arrangementMemory({
    arrangmentIds: [testIndex.id],
    replicaHeapLimit: Number(testReplicaSize.heapLimit),
    clusterName: detailPageCluster.name,
    replicaName: detailPageCluster.replicas[0]?.name,
  }),
  results: mapKyselyToTabular({
    rows: [],
    columns: arrangmentMemoryColumns,
  }),
});
const arrangementMemoryKey = clusterQueryKeys.arrangementMemory({
  arrangmentIds: [testIndex.id],
  replicaHeapLimit: Number(testReplicaSize.heapLimit),
  clusterName: detailPageCluster.name,
  replicaName: detailPageCluster.replicas[0]?.name,
});

const pendingArrangmentMemoryHandler = buildSqlQueryHandlerV2(
  {
    queryKey: arrangementMemoryKey,
    results: mapKyselyToTabular({
      rows: [],
      columns: arrangmentMemoryColumns,
    }),
  },
  { waitTimeMs: "infinite" },
);

/**
 * Resolves once the memory query is sent for the replica it reads from, so a
 * test asserts while that query, not the replicas query, is pending.
 */
const arrangementMemoryRequested = () =>
  new Promise<void>((resolve) => {
    const onStart = ({ request }: { request: Request }) => {
      const queryKey = new URL(request.url).searchParams.get("query_key");
      if (queryKey === hashKey(arrangementMemoryKey)) {
        server.events.removeListener("request:start", onStart);
        resolve();
      }
    };
    server.events.on("request:start", onStart);
  });
const validArrangmentMemoryHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.arrangementMemory({
    arrangmentIds: [testIndex.id],
    replicaHeapLimit: Number(testReplicaSize.heapLimit),
    clusterName: detailPageCluster.name,
    replicaName: detailPageCluster.replicas[0]?.name,
  }),
  results: mapKyselyToTabular({
    rows: [
      { id: testIndex.id, size: "1739186876", memoryPercentage: "10.1234" },
    ],
    columns: arrangmentMemoryColumns,
  }),
});

const pendingMaterializationLagHandler = buildSqlQueryHandlerV2(
  {
    queryKey: clusterQueryKeys.materializationLag({
      objectIds: [testIndex.id],
    }),
    results: mapKyselyToTabular({
      rows: [],
      columns: buildColumns(["targetObjectId"]),
    }),
  },
  { waitTimeMs: "infinite" },
);
const emptyMaterializationLagHandler = buildValidMaterializationLagHandler(
  { objectIds: [testIndex.id] },
  [],
);

const errorArrangmentMemoryHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.arrangementMemory({
    arrangmentIds: [testIndex.id],
    replicaHeapLimit: Number(testReplicaSize.heapLimit),
    clusterName: detailPageCluster.name,
    replicaName: detailPageCluster.replicas[0]?.name,
  }),
  results: {
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
    notices: [],
  },
});

function renderIndexList() {
  renderComponent(
    <Routes>
      <Route path=":clusterId">
        <Route index path="*" element={<IndexList />} />
      </Route>
    </Routes>,
    {
      initialRouterEntries: detailPageInitialRouteEntries,
    },
  );
}

describe("IndexList", () => {
  beforeEach(() => {
    server.use(validReplicaSizesHandler, emptyArrangmentMemoryHandler);
    const store = getStore();
    store.set(allClusters, mockSubscribeState({ data: [detailPageCluster] }));
  });

  it("shows the empty state when there are no results", async () => {
    server.use(emptyIndexesListHandler);
    renderIndexList();

    expect(
      await screen.findByText("This cluster has no indexes"),
    ).toBeVisible();
  });

  it("renders the indexes list without memory usage initially", async () => {
    server.use(validIndexesListHandler);
    renderIndexList();

    expect((await screen.findAllByText("materialize.public")).length).toBe(2);
    expect(await screen.findByText("test_index")).toBeVisible();
    expect(await screen.findByText("test_view_1 (test_col)")).toBeVisible();
    expect(await screen.findByText("view")).toBeVisible();
  });

  it("renders memory usage once it's available", async () => {
    server.use(validIndexesListHandler, validArrangmentMemoryHandler);
    renderIndexList();

    expect(await screen.findByText("test_index")).toBeVisible();
    expect(await screen.findByText("1.62 GB (10.1%)")).toBeVisible();
  });

  it("shows a loading indicator as memory usage while it loads", async () => {
    server.use(validIndexesListHandler, pendingArrangmentMemoryHandler);
    const requested = arrangementMemoryRequested();
    renderIndexList();
    await requested;

    const memoryUsage = await screen.findByTestId("memory-usage");
    expect(within(memoryUsage).getByTestId("metric-loading")).toBeVisible();
    expect(memoryUsage).not.toHaveTextContent("-");
  });

  it("shows a loading indicator as memory usage while the replica it reads from loads", async () => {
    server.use(validIndexesListHandler, pendingReplicaSizesHandler);
    renderIndexList();

    const memoryUsage = await screen.findByTestId("memory-usage");
    expect(within(memoryUsage).getByTestId("metric-loading")).toBeVisible();
    expect(memoryUsage).not.toHaveTextContent("-");
  });

  it("shows '-' as memory usage once loaded without a value", async () => {
    server.use(validIndexesListHandler);
    renderIndexList();

    const memoryUsage = await screen.findByTestId("memory-usage");
    await waitFor(() => expect(memoryUsage).toHaveTextContent(/^-$/));
    expect(
      within(memoryUsage).queryByTestId("metric-loading"),
    ).not.toBeInTheDocument();
  });

  it("shows '-' as memory usage if it fails to load", async () => {
    server.use(validIndexesListHandler, errorArrangmentMemoryHandler);
    renderIndexList();

    expect(await screen.findByText("test_index")).toBeVisible();
    const memoryUsage = await screen.findByTestId("memory-usage");
    await waitFor(() => expect(memoryUsage).toHaveTextContent(/^-$/));
    expect(memoryUsage).toBeVisible();
  });

  it("shows a loading indicator as freshness while it loads", async () => {
    server.use(validIndexesListHandler, pendingMaterializationLagHandler);
    renderIndexList();

    const freshness = await screen.findByTestId("freshness");
    expect(within(freshness).getByTestId("metric-loading")).toBeVisible();
    expect(freshness).not.toHaveTextContent("-");
  });

  it("shows '-' as freshness once loaded without a value", async () => {
    server.use(validIndexesListHandler, emptyMaterializationLagHandler);
    renderIndexList();

    const freshness = await screen.findByTestId("freshness");
    await waitFor(() => expect(freshness).toHaveTextContent(/^-$/));
    expect(
      within(freshness).queryByTestId("metric-loading"),
    ).not.toBeInTheDocument();
  });
});
