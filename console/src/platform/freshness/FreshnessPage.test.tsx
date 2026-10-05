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
import { describe, expect, it } from "vitest";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { SubscribeError } from "~/api/materialize/SubscribeManager";
import { allClusters } from "~/store/allClusters";
import { renderComponent } from "~/test/utils";

import FreshnessPage from "./FreshnessPage";

const renderWithClusters = async (state: {
  data: Cluster[];
  error?: SubscribeError;
  snapshotComplete: boolean;
}) =>
  renderComponent(<FreshnessPage />, {
    initializeState: (store) =>
      store.set(allClusters, { error: undefined, ...state }),
  });

describe("FreshnessPage cluster states", () => {
  it("waits rather than claiming there are no clusters", async () => {
    // An empty list before the subscribe lands is not the same as an empty
    // environment, and the page used to report both as the latter.
    await renderWithClusters({ data: [], snapshotComplete: false });

    expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    expect(
      screen.queryByText(/No clusters to show freshness for/),
    ).not.toBeInTheDocument();
  });

  it("says so when the subscribe fails", async () => {
    await renderWithClusters({
      data: [],
      error: { code: "INTERNAL_ERROR", message: "boom" },
      snapshotComplete: true,
    });

    expect(
      await screen.findByText(/An error occurred loading clusters/),
    ).toBeVisible();
    expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
  });

  it("reports an empty environment only once it knows", async () => {
    await renderWithClusters({ data: [], snapshotComplete: true });

    expect(
      await screen.findByText(/No clusters to show freshness for/),
    ).toBeVisible();
  });
});
