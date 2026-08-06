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

import server from "~/api/mocks/server";
import {
  disabledEnvironment,
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";

import { NavBar } from "./NavBar";

describe("NavBar", () => {
  afterEach(() => {
    server.resetHandlers();
    vi.clearAllMocks();
  });

  it("renders the Admin group after the region-scoped items", async () => {
    await renderComponent(<NavBar isCollapsed={false} />, {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    });
    const clusters = await screen.findByText("Clusters");
    const admin = screen.getByText("Admin");
    // Admin is pinned to the bottom of the menu, so it must come after the
    // region-scoped items in document order.
    expect(
      clusters.compareDocumentPosition(admin) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(screen.getByText("Create New")).toBeInTheDocument();
  });

  it("hides the create button and region items without an enabled environment", async () => {
    await renderComponent(<NavBar isCollapsed={false} />, {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", disabledEnvironment),
    });
    expect(await screen.findByText("Admin")).toBeInTheDocument();
    expect(screen.queryByText("Create New")).not.toBeInTheDocument();
    expect(screen.queryByText("Clusters")).not.toBeInTheDocument();
    expect(screen.queryByText("SQL Shell")).not.toBeInTheDocument();
  });
});
