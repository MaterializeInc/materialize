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

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { ErrorCode } from "~/api/materialize/types";
import { getStore } from "~/jotai";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "~/platform/clusters/constants";
import { allClusters } from "~/store/allClusters";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent } from "~/test/utils";

import { ClusterMetrics } from "./ClusterMetrics";

describe("ClusterMetrics", () => {
  it("shows an error when the clusters subscribe fails before its snapshot", async () => {
    getStore().set(
      allClusters,
      mockSubscribeState<Cluster>({
        snapshotComplete: false,
        error: {
          code: ErrorCode.INTERNAL_ERROR,
          message: "Something went wrong",
        },
      }),
    );
    await renderComponent(
      <Routes>
        <Route path=":clusterId/:clusterName" element={<ClusterMetrics />} />
      </Routes>,
      { initialRouterEntries: ["/u1/default"] },
    );

    expect(await screen.findByText(CLUSTERS_FETCH_ERROR_MESSAGE)).toBeVisible();
  });
});
