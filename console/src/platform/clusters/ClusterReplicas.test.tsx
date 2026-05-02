// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { renderComponent } from "~/test/utils";

import ClusterReplicas from "./ClusterReplicas";
import { detailPageSetupHelpers } from "./clustersTestUtils";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import { clusterQueryKeys } from "./queries";

const ClusterReplicasWithRoute = () => (
  <Routes>
    <Route path=":clusterId/:clusterName" element={<ClusterReplicas />} />
  </Routes>
);

const replicasWithUtilizationColumns = buildColumns([
  "id",
  "name",
  "clusterName",
  "size",
  { name: "cpuPercent", type_oid: MzDataType.float8 },
  { name: "memoryPercent", type_oid: MzDataType.float8 },
  { name: "diskPercent", type_oid: MzDataType.float8 },
  { name: "memoryUtilizationPercent", type_oid: MzDataType.float8 },
  { name: "managed", type_oid: MzDataType.bool },
  { name: "isOwner", type_oid: MzDataType.bool },
]);

const {
  detailPageCluster,
  detailPageSetupHandler,
  detailPageInitialRouteEntries,
} = detailPageSetupHelpers();

const emptyReplicaListHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.replicasWithUtilization({
    clusterId: detailPageCluster.id,
  }),
  results: mapKyselyToTabular({
    rows: [],
    columns: replicasWithUtilizationColumns,
  }),
});

const validReplicaListHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.replicasWithUtilization({
    clusterId: detailPageCluster.id,
  }),
  results: mapKyselyToTabular({
    rows: [
      {
        id: "u123",
        name: "test_replica",
        clusterName: "test_cluster",
        size: "50cc",
        cpuPercent: 18.0015698,
        memoryPercent: 6.836938858032227,
        diskPercent: 2.1,
        memoryUtilizationPercent: 4.5,
        managed: true,
        isOwner: true,
      },
    ],
    columns: replicasWithUtilizationColumns,
  }),
});

describe("ClusterReplicas", () => {
  beforeEach(() => {
    server.use(detailPageSetupHandler);
  });

  it("shows a spinner initially", async () => {
    server.use(emptyReplicaListHandler);
    renderComponent(<ClusterReplicasWithRoute />, {
      initialRouterEntries: detailPageInitialRouteEntries,
    });

    await waitFor(() => {
      expect(screen.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  it("shows an error state if results fail to load", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.replicasWithUtilization({
          clusterId: detailPageCluster.id,
        }),
        results: {
          error: {
            message: "Something went wrong",
            code: ErrorCode.INTERNAL_ERROR,
          },
          notices: [],
        },
      }),
    );
    renderComponent(<ClusterReplicasWithRoute />, {
      initialRouterEntries: detailPageInitialRouteEntries,
    });

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });

  it("shows the empty state when there are no results", async () => {
    server.use(emptyReplicaListHandler);
    renderComponent(<ClusterReplicasWithRoute />, {
      initialRouterEntries: detailPageInitialRouteEntries,
    });

    expect(
      await screen.findByText("This cluster has no replicas"),
    ).toBeVisible();
  });

  it("renders the cluster replicas list", async () => {
    server.use(validReplicaListHandler);
    renderComponent(<ClusterReplicasWithRoute />, {
      initialRouterEntries: detailPageInitialRouteEntries,
    });

    expect(await screen.findByText("test_replica")).toBeVisible();
    expect(await screen.findByText("50cc")).toBeVisible();
    expect(await screen.findByText("18.0")).toBeVisible();
    expect(await screen.findByText("4.5")).toBeVisible();
  });
});
