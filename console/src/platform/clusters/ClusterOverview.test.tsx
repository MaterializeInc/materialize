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
import { buildSqlQueryHandlerV2 } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";

import ClusterOverview from "./ClusterOverview";
import { detailPageSetupHelpers } from "./clustersTestUtils";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import { clusterQueryKeys } from "./queries";

const ClusterOverviewWithRoute = () => (
  <Routes>
    <Route path=":clusterId/:clusterName" element={<ClusterOverview />} />
  </Routes>
);

const {
  detailPageInitialRouteEntries,
  detailPageSetupHandler,
  detailPageCluster,
} = detailPageSetupHelpers();

describe("ClusterOverview", () => {
  beforeEach(() => {
    server.use(detailPageSetupHandler);
  });

  it("shows an error state when cluster utilization websocket fails and data has not loaded", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.replicaUtilizationHistory({
          clusterIds: [detailPageCluster.id],
          bucketSizeMs: 60_000,
          timePeriodMinutes: 60,
          replicaId: undefined,
        }),
        results: {
          notices: [],
          error: {
            message: "Something went wrong",
            code: ErrorCode.INTERNAL_ERROR,
          },
        },
      }),
    );

    renderComponent(<ClusterOverviewWithRoute />, {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      initialRouterEntries: detailPageInitialRouteEntries,
    });

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });
});
