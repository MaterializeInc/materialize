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

import { ErrorCode } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  buildUseSqlQueryHandler,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { getQueryClient } from "~/queryClient";
import { allClusters } from "~/store/allClusters";
import { buildValidMaterializationLagHandler } from "~/test/clusterQueryBuilders";
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

const arrangementMemoryKey = clusterQueryKeys.arrangementMemory({
  arrangmentIds: [testMaterializedViewId],
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

/**
 * Resolves once the memory query has settled without a replica to read from,
 * so a test asserts while only the replicas query is pending.
 */
const replicalessMemorySettled = () =>
  waitFor(() =>
    expect(
      getQueryClient()
        .getQueryCache()
        .findAll({
          predicate: ({ queryKey, state }) => {
            const part = queryKey.at(-1) as
              { scope?: string; replicaName?: string } | undefined;
            return (
              part?.scope === "arrangementMemory" &&
              part.replicaName === undefined &&
              state.status === "success"
            );
          },
        }),
    ).toHaveLength(1),
  );

const pendingMaterializationLagHandler = buildSqlQueryHandlerV2(
  {
    queryKey: clusterQueryKeys.materializationLag({
      objectIds: [testMaterializedViewId],
    }),
    results: mapKyselyToTabular({
      rows: [],
      columns: buildColumns(["targetObjectId"]),
    }),
  },
  { waitTimeMs: "infinite" },
);
const emptyMaterializationLagHandler = buildValidMaterializationLagHandler(
  { objectIds: [testMaterializedViewId] },
  [],
);

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

  it("shows a loading indicator as memory usage while it loads", async () => {
    server.use(pendingArrangmentMemoryHandler);
    const requested = arrangementMemoryRequested();
    renderMaterializedViewList();
    await requested;

    const memoryUsage = await screen.findByTestId("memory-usage");
    expect(within(memoryUsage).getByTestId("metric-loading")).toBeVisible();
    expect(memoryUsage).not.toHaveTextContent("-");
  });

  it("shows a loading indicator as memory usage while the replica it reads from loads", async () => {
    server.use(pendingReplicaSizesHandler);
    renderMaterializedViewList();
    await replicalessMemorySettled();

    const memoryUsage = await screen.findByTestId("memory-usage");
    expect(within(memoryUsage).getByTestId("metric-loading")).toBeVisible();
    expect(memoryUsage).not.toHaveTextContent("-");
  });

  it("shows '-' as memory usage once loaded without a value", async () => {
    renderMaterializedViewList();

    const memoryUsage = await screen.findByTestId("memory-usage");
    await waitFor(() => expect(memoryUsage).toHaveTextContent(/^-$/));
    expect(
      within(memoryUsage).queryByTestId("metric-loading"),
    ).not.toBeInTheDocument();
  });

  it("shows '-' as memory usage if it fails to load", async () => {
    server.use(errorArrangmentMemoryHandler);
    renderMaterializedViewList();

    expect(await screen.findByText("test_materialized_view")).toBeVisible();
    const memoryUsage = await screen.findByTestId("memory-usage");
    await waitFor(() => expect(memoryUsage).toHaveTextContent(/^-$/));
    expect(memoryUsage).toBeVisible();
  });

  it("shows a loading indicator as freshness while it loads", async () => {
    server.use(pendingMaterializationLagHandler);
    renderMaterializedViewList();

    const freshness = await screen.findByTestId("freshness");
    expect(within(freshness).getByTestId("metric-loading")).toBeVisible();
    expect(freshness).not.toHaveTextContent("-");
  });

  it("shows '-' as freshness once loaded without a value", async () => {
    server.use(emptyMaterializationLagHandler);
    renderMaterializedViewList();

    const freshness = await screen.findByTestId("freshness");
    await waitFor(() => expect(freshness).toHaveTextContent(/^-$/));
    expect(
      within(freshness).queryByTestId("metric-loading"),
    ).not.toBeInTheDocument();
  });
});
