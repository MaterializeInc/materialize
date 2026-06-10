// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { Route, Routes } from "react-router-dom";

import server from "~/api/mocks/server";
import { validClustersResponse } from "~/test/clusterQueryBuilders";
import { renderComponent, RenderWithPathname } from "~/test/utils";

import ClusterDetail from "./ClusterDetail";

vi.mock("~/platform/clusters/ClusterOverview", () => ({
  default: function () {
    return <div>ClusterOverview component</div>;
  },
}));

describe("ClusterRoutes", () => {
  it("breadcrumb context menu allows switching clusters", async () => {
    server.use(validClustersResponse);
    renderComponent(
      <RenderWithPathname>
        <Routes>
          <Route path=":clusterId/:clusterName">
            <Route index path="*" element={<ClusterDetail />} />
          </Route>
        </Routes>
      </RenderWithPathname>,
      {
        initialRouterEntries: ["/u1/default"],
      },
    );

    await waitFor(() => {
      // The context menu Portal / Menu list combination is setting display: none on
      // elemnts outside the menu, which is really confusing.
      expect(screen.getByText("/u1/default")).toBeVisible();
    });
    expect(screen.getByText("ClusterOverview component")).toBeVisible();
    const user = userEvent.setup();
    user.click(screen.getByRole("button", { name: "Navigation actions" }));
    await waitFor(() => {
      expect(screen.getByText("quickstart")).toBeVisible();
    });
    user.click(screen.getByRole("menuitem", { name: "quickstart" }));

    expect(await screen.findByText("/u2/quickstart")).toBeVisible();
    expect(screen.getByText("ClusterOverview component")).toBeVisible();
  });
});
