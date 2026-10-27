// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { ErrorCode } from "~/api/materialize/types";
import {
  buildSqlQueryHandlerV2,
  buildUseSqlQueryHandler,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { mockSubscribeState } from "~/test/mockSubscribe";
import {
  arrangmentMemoryColumns,
  clusterReplicasColumns,
} from "~/test/queries";
import { renderComponent } from "~/test/utils";

import { detailPageSetupHelpers } from "./clustersTestUtils";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import MaterializedViewsList from "./MaterializedViewsList";
import { clusterQueryKeys } from "./queries";

const {
  detailPageCluster,
  detailPageSetupHandler,
  detailPageInitialRouteEntries,
} = detailPageSetupHelpers();

const testMaterializedViewId = "u234";

const MaterializedViewsListColumns = [
  "id",
  "name",
  "definition",
  "databaseName",
  "schemaName",
];

const emptyMaterializeViewListHandler = buildUseSqlQueryHandler({
  type: "SELECT" as const,
  rows: [],
  columns: MaterializedViewsListColumns,
});

const errorMaterializeViewListHandler = buildUseSqlQueryHandler({
  type: "SELECT" as const,
  error: {
    message: "Invalid sql",
    code: ErrorCode.INTERNAL_ERROR,
  },
  rows: [],
  columns: MaterializedViewsListColumns,
});

const validMaterializeViewListHandler = buildUseSqlQueryHandler({
  type: "SELECT" as const,
  rows: [
    [
      testMaterializedViewId,
      "test_materialized_view",
      "test_definition",
      "materialize",
      "public",
    ],
  ],
  columns: MaterializedViewsListColumns,
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
const emptyArrangmentMemoryHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.arrangementMemory({
    arrangmentIds: [testMaterializedViewId],
    replicaHeapLimit: Number(testReplicaSize.heapLimit),
    clusterName: detailPageCluster.name,
    replicaName: detailPageCluster.replicas[0]?.name,
  }),
  results: mapKyselyToTabular({
    rows: [],
    columns: arrangmentMemoryColumns,
  }),
});
const validArrangmentMemoryHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.arrangementMemory({
    arrangmentIds: [testMaterializedViewId],
    replicaHeapLimit: Number(testReplicaSize.heapLimit),
    clusterName: detailPageCluster.name,
    replicaName: detailPageCluster.replicas[0]?.name,
  }),
  results: mapKyselyToTabular({
    rows: [
      {
        id: testMaterializedViewId,
        size: "1739186876",
        memoryPercentage: "10.1234",
      },
    ],
    columns: arrangmentMemoryColumns,
  }),
});

const errorArrangmentMemoryHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.arrangementMemory({
    arrangmentIds: [testMaterializedViewId],
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

function renderMaterializedViewList() {
  renderComponent(
    <Routes>
      <Route path=":clusterId">
        <Route index path="*" element={<MaterializedViewsList />} />
      </Route>
    </Routes>,
    {
      initialRouterEntries: detailPageInitialRouteEntries,
    },
  );
}

describe("MaterializedViews", () => {
  beforeEach(() => {
    server.use(
      detailPageSetupHandler,
      validMaterializeViewListHandler,
      validReplicaSizesHandler,
      emptyArrangmentMemoryHandler,
    );
    const store = getStore();
    store.set(allClusters, mockSubscribeState({ data: [detailPageCluster] }));
  });

  it("shows a spinner initially", async () => {
    renderMaterializedViewList();

    expect(await screen.findByTestId("loading-spinner")).toBeVisible();
  });

  it("shows an error state if results fail to load", async () => {
    server.use(errorMaterializeViewListHandler);
    renderMaterializedViewList();

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });

  it("shows the empty state when there are no results", async () => {
    server.use(emptyMaterializeViewListHandler);
    renderMaterializedViewList();

    expect(
      await screen.findByText("This cluster has no materialized views"),
    ).toBeVisible();
  });

  it("renders the materialized views list", async () => {
    renderMaterializedViewList();

    expect(await screen.findByText("materialize.public")).toBeVisible();
    expect(await screen.findByText("test_materialized_view")).toBeVisible();
  });

  it("renders memory usage once it's available", async () => {
    server.use(validArrangmentMemoryHandler);
    renderMaterializedViewList();

    expect(await screen.findByText("test_materialized_view")).toBeVisible();
    expect(await screen.findByText("1.62 GB (10.1%)")).toBeVisible();
  });

  it("shows '-' as memory usage if unavailable", async () => {
    server.use(errorArrangmentMemoryHandler);
    renderMaterializedViewList();

    expect(await screen.findByText("test_materialized_view")).toBeVisible();
    const memoryUsage = await screen.findByTestId("memory-usage");
    expect(memoryUsage).toHaveTextContent("-");
    expect(memoryUsage).toBeVisible();
  });
});
