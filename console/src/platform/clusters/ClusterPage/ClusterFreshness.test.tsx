// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { delay, http } from "msw";
import React from "react";
import { Route, Routes } from "react-router-dom";
import { describe, expect, it } from "vitest";

import server from "~/api/mocks/server";
import { allObjects } from "~/store/allObjects";
import { buildDatabaseObject, renderComponent } from "~/test/utils";

import { ClusterFreshness } from "./ClusterFreshness";
import { ClusterPageHeader } from "./ClusterPage";

// The controls render while the readings load, so the queries never need an
// answer.
const neverAnswerSql = () =>
  server.use(
    http.post("*/api/sql", async () => {
      await delay("infinite");
    }),
  );

const OBJECTS = {
  data: [buildDatabaseObject({ id: "u10", clusterId: "u1" })],
  error: undefined,
  snapshotComplete: true,
};

describe("ClusterFreshness", () => {
  it("restores its controls from a bookmarked URL", async () => {
    neverAnswerSql();
    await renderComponent(
      <Routes>
        <Route
          path=":clusterId/:clusterName/freshness"
          element={<ClusterFreshness />}
        />
      </Routes>,
      {
        initializeState: (store) => store.set(allObjects, OBJECTS),
        initialRouterEntries: [
          "/u1/quickstart/freshness?timePeriod=360&type=index&threshold=5000&exceeded=p99",
        ],
      },
    );

    expect(await screen.findByLabelText("Time range")).toHaveValue("360");
    expect(screen.getByText("Indexes")).toBeVisible();
    expect(screen.getByLabelText("Freshness statistic")).toHaveValue("p99");
    expect(screen.getByLabelText("Freshness threshold in seconds")).toHaveValue(
      5,
    );
  });

  it("keeps its controls when the user leaves the tab and comes back", async () => {
    neverAnswerSql();
    // Nested as `ClusterRoutes` and `ClusterPage` nest them, because the tab
    // links are relative.
    await renderComponent(
      <Routes>
        <Route path=":clusterId/:clusterName">
          <Route
            index
            path="*"
            element={
              <>
                <ClusterPageHeader />
                <Routes>
                  <Route index element={<div>Metrics tab</div>} />
                  <Route path="freshness" element={<ClusterFreshness />} />
                </Routes>
              </>
            }
          />
        </Route>
      </Routes>,
      {
        initializeState: (store) => store.set(allObjects, OBJECTS),
        initialRouterEntries: ["/u1/quickstart/freshness?configure=overview"],
      },
    );

    const user = userEvent.setup();
    await user.selectOptions(await screen.findByLabelText("Time range"), "360");
    await user.click(screen.getByRole("link", { name: "Metrics" }));
    expect(await screen.findByText("Metrics tab")).toBeVisible();

    // Without the drawer's section, which would reopen the drawer.
    const freshnessTab = screen.getByRole("link", { name: "Freshness" });
    expect(freshnessTab).toHaveAttribute(
      "href",
      "/u1/quickstart/freshness?timePeriod=360",
    );
    await user.click(freshnessTab);
    expect(await screen.findByLabelText("Time range")).toHaveValue("360");
  });
});
