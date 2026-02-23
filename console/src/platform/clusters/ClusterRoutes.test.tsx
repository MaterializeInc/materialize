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

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { ErrorCode } from "~/api/materialize/types";
import {
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import {
  clustersFetchColumns,
  emptyClustersResponse,
  validClustersResponse,
} from "~/test/clusterQueryBuilders";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent, RenderWithPathname } from "~/test/utils";

import ClusterRoutes from "./ClusterRoutes";
import { buildClusterServerResponse } from "./clustersTestUtils";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import { clusterQueryKeys, useClusters } from "./queries";

vi.mock("~/platform/clusters/ClusterDetail", () => ({
  default: function () {
    // Given we're mocking the default export, we don't need to capitalize the function name
    // eslint-disable-next-line react-hooks/rules-of-hooks
    useClusters();
    return <div>ClusterDetail component</div>;
  },
}));

vi.mock("~/api/materialize/cluster/useLatestNotReadyReplica", () => ({
  __esModule: true,
  default: vi.fn(() => ({
    loading: false,
    error: null,
    data: new Map(),
  })),
}));

const validCluster: Cluster = {
  id: "u1",
  name: "default",
  size: "50cc",
  managed: true,
  replicas: [
    {
      id: "u678",
      name: "r1",
      size: "50cc",
      disk: true,
      statuses: [
        {
          replica_id: "u678",
          process_id: "0",
          reason: null,
          status: "online",
          updated_at: "2024-01-01T00:00:00.000Z",
        },
      ],
    },
  ],
  disk: true,
  latestStatusUpdate: "2024-01-01T00:00:00.000Z",
};

export const noSystemObjectClustersResponse = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.list({
    includeSystemObjects: false,
  }),
  results: mapKyselyToTabular({
    columns: clustersFetchColumns,
    rows: [
      buildClusterServerResponse({ id: "u1", name: "default" }),
      buildClusterServerResponse({ id: "u2", name: "user_cluster" }),
    ],
  }),
});

export const errorClustersResponse = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.list({ includeSystemObjects: false }),
  results: {
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
    notices: [],
  },
});

describe("ClusterRoutes", () => {
  beforeEach(() => {
    const store = getStore();
    store.set(allClusters, mockSubscribeState({ data: [validCluster] }));
  });

  it("shows a spinner initially", async () => {
    server.use(emptyClustersResponse);
    renderComponent(<ClusterRoutes />);

    expect(await screen.findByText("Clusters")).toBeVisible();
    await waitFor(() => {
      expect(screen.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  it("shows the empty state when there are no results", async () => {
    server.use(emptyClustersResponse);
    renderComponent(<ClusterRoutes />);

    expect(await screen.findByText("No available clusters")).toBeVisible();
  });

  it("shows an error state when clusters fail to load", async () => {
    server.use(errorClustersResponse);
    renderComponent(<ClusterRoutes />);

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });

  it("renders the cluster list", async () => {
    // The cluster routes use the unfiltered response and the list uses the filtered response
    server.use(noSystemObjectClustersResponse);
    renderComponent(<ClusterRoutes />);

    expect(await screen.findByText("Clusters")).toBeVisible();

    await waitFor(async () => {
      expect(await screen.findByText("default")).toBeVisible();
    });

    expect(await screen.findByText("user_cluster")).toBeVisible();
  });

  it("redirects back to the list for invalid clusters", async () => {
    renderComponent(
      <RenderWithPathname>
        <ClusterRoutes />
      </RenderWithPathname>,
      {
        initialRouterEntries: ["/u99/does_not_exist"],
      },
    );

    await waitFor(() =>
      expect(screen.getByTestId("pathname")).toHaveTextContent(/^\/$/),
    );
    expect(screen.queryByText("ClusterDetail component")).toBeNull();
  });

  it("shows cluster details", async () => {
    server.use(validClustersResponse);
    renderComponent(<ClusterRoutes />, {
      initialRouterEntries: ["/u1/default"],
    });

    expect(await screen.findByText("ClusterDetail component")).toBeVisible();
  });

  it("updates the path when the name has changed", async () => {
    server.use(validClustersResponse);
    renderComponent(
      <RenderWithPathname>
        <ClusterRoutes />
      </RenderWithPathname>,
      {
        initialRouterEntries: ["/u1/old_name"],
      },
    );

    expect(await screen.findByText("/u1/default")).toBeVisible();
    expect(screen.getByText("ClusterDetail component")).toBeVisible();
  });

  it("updates the path when the id has changed", async () => {
    server.use(validClustersResponse);
    renderComponent(
      <RenderWithPathname>
        <ClusterRoutes />
      </RenderWithPathname>,
      {
        initialRouterEntries: ["/u1/default"],
      },
    );

    expect(await screen.findByText("/u1/default")).toBeVisible();
    expect(screen.getByText("ClusterDetail component")).toBeVisible();
  });

  it("shows an error state when clusters fail to load on the details page", async () => {
    server.use(errorClustersResponse);
    renderComponent(<ClusterRoutes />, {
      initialRouterEntries: ["/u1/default"],
    });

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });
});
