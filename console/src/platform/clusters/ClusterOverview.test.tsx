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
import { getStore } from "~/jotai";
import {
  defaultRegionId,
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

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

/** An environment new enough to serve the chart from the utilization SUBSCRIBEs. */
const environmentV2632 = {
  ...healthyEnvironment,
  status: {
    health: "healthy" as const,
    version: parseDbVersion("v26.32.0 (ea0d129f)"),
    errors: [],
  },
};

/**
 * A websocket that never opens. The test environment has no websocket server,
 * so a real socket errors and shows the chart's error state on its own.
 */
class PendingWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readonly readyState = PendingWebSocket.CONNECTING;
  addEventListener() {}
  removeEventListener() {}
  send() {}
  close() {}
}

describe("ClusterOverview", () => {
  beforeEach(() => {
    server.use(detailPageSetupHandler);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
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

  it("shows an error state when the cluster lineage lookup fails", async () => {
    vi.stubGlobal("WebSocket", PendingWebSocket);
    // Query keys embed the environment version at build time, so seed the
    // v26.32 environment before building this test's handler.
    await setFakeEnvironment(getStore().set, defaultRegionId, environmentV2632);
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.deploymentLineage({
          clusterIdsKey: detailPageCluster.id,
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
        setFakeEnvironment(set, defaultRegionId, environmentV2632),
      initialRouterEntries: detailPageInitialRouteEntries,
    });

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });
});
