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
import { Outlet, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";

import { renderComponent } from "~/test/utils";

import { MaintainedObjectsOutletContext } from "./MaintainedObjectsLayout";
import { ObjectDetailDrawer } from "./ObjectDetailDrawer";

const allObjects = vi.hoisted(() => ({
  data: [] as { id: string; objectType: string }[],
}));

vi.mock("~/store/allObjects", () => ({
  useAllObjects: () => ({ data: allObjects.data }),
}));

const WRITABLE_TABLE = "No freshness data for this table";

const renderDrawer = (
  context: Partial<MaintainedObjectsOutletContext>,
  objectId = "u1",
) =>
  renderComponent(
    <Routes>
      <Route
        element={
          <Outlet
            context={{
              data: [],
              isLoading: false,
              lagReady: true,
              hydrationReady: true,
              isError: false,
              lookbackMinutes: 1,
              setLookbackMinutes: vi.fn(),
              ...context,
            }}
          />
        }
      >
        <Route
          path="/maintained-objects/:objectId"
          element={<ObjectDetailDrawer />}
        />
      </Route>
    </Routes>,
    { initialRouterEntries: [`/maintained-objects/${objectId}`] },
  );

describe("ObjectDetailDrawer", () => {
  beforeEach(() => {
    allObjects.data = [
      { id: "u1", objectType: "table" },
      { id: "u2", objectType: "view" },
      { id: "s1", objectType: "table" },
    ];
  });

  it("explains that a writable table has no freshness data", async () => {
    await renderDrawer({});

    expect(await screen.findByText(WRITABLE_TABLE)).toBeVisible();
  });

  it.each([
    ["a view", "u2"],
    ["a system table", "s1"],
  ])("says the page does not track %s", async (_label, objectId) => {
    await renderDrawer({}, objectId);

    expect(
      await screen.findByText("This page doesn't track this object"),
    ).toBeVisible();
  });

  it("says an object missing from the catalog was not found", async () => {
    await renderDrawer({}, "u99");

    expect(await screen.findByText("Object not found")).toBeVisible();
  });

  it("waits for the hydration snapshot before explaining a table", async () => {
    // A source-fed table is only listed once that snapshot names its source.
    await renderDrawer({ hydrationReady: false });

    expect(await screen.findByTestId("loading-spinner")).toBeInTheDocument();
    expect(screen.queryByText(WRITABLE_TABLE)).not.toBeInTheDocument();
  });
});
