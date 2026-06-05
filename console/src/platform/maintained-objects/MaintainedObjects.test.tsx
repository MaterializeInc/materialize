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

import { renderComponent } from "~/test/utils";

import MaintainedObjects, { MaintainedObjectsProps } from "./MaintainedObjects";
import { MaintainedObjectListItem } from "./queries";

const buildObject = (
  overrides: Partial<MaintainedObjectListItem>,
): MaintainedObjectListItem => ({
  id: "u1",
  name: "object",
  schemaName: "public",
  databaseName: "materialize",
  objectType: "materialized-view",
  sourceType: null,
  cluster: { id: "u1", name: "quickstart" },
  hydratedReplicas: 1,
  totalReplicas: 1,
  lag: null,
  ...overrides,
});

const ordersMv = buildObject({
  id: "u101",
  name: "orders_mv",
  objectType: "materialized-view",
  cluster: { id: "u101", name: "compute_cluster" },
  hydratedReplicas: 2,
  totalReplicas: 2,
});

const inventoryIdx = buildObject({
  id: "u102",
  name: "inventory_idx",
  objectType: "index",
  cluster: { id: "u101", name: "compute_cluster" },
  hydratedReplicas: 1,
  totalReplicas: 2,
});

const eventsSrc = buildObject({
  id: "u103",
  name: "events_src",
  objectType: "source",
  cluster: { id: "u200", name: "ingest_cluster" },
  hydratedReplicas: 0,
  totalReplicas: 1,
});

const buildProps = (
  overrides?: Partial<MaintainedObjectsProps>,
): MaintainedObjectsProps => ({
  objects: [ordersMv, inventoryIdx, eventsSrc],
  isLoading: false,
  lagReady: true,
  hydrationReady: true,
  lookbackMinutes: 15,
  setLookbackMinutes: vi.fn(),
  ...overrides,
});

const renderMaintainedObjects = (overrides?: Partial<MaintainedObjectsProps>) =>
  renderComponent(<MaintainedObjects {...buildProps(overrides)} />, {
    initialRouterEntries: ["/maintained-objects"],
  });

describe("MaintainedObjects", () => {
  it("renders the list of maintained objects", async () => {
    await renderMaintainedObjects();

    expect(await screen.findByText("orders_mv")).toBeVisible();
    expect(screen.getByText("inventory_idx")).toBeVisible();
    expect(screen.getByText("events_src")).toBeVisible();

    // Cluster links render with the cluster name.
    expect(
      screen.getAllByRole("link", { name: "compute_cluster" }),
    ).toHaveLength(2);
    expect(screen.getByRole("link", { name: "ingest_cluster" })).toBeVisible();
  });

  it("filters rows by object type", async () => {
    const user = userEvent.setup();
    await renderMaintainedObjects();

    await user.click(
      await screen.findByRole("button", { name: "Filter objectType" }),
    );
    await user.click(await screen.findByLabelText("index"));

    expect(screen.getByText("inventory_idx")).toBeVisible();
    expect(screen.queryByText("orders_mv")).not.toBeInTheDocument();
    expect(screen.queryByText("events_src")).not.toBeInTheDocument();

    // Filter chip summarizes the active filter.
    expect(screen.getByText("Type: index")).toBeVisible();
  });

  it("filters rows by search term", async () => {
    const user = userEvent.setup();
    await renderMaintainedObjects();

    await user.type(
      await screen.findByPlaceholderText("Search objects..."),
      "orders",
    );

    // TableSearch debounces input, so wait for the filter to settle.
    await waitFor(() => {
      expect(screen.getByText("orders_mv")).toBeVisible();
      expect(screen.queryByText("inventory_idx")).not.toBeInTheDocument();
      expect(screen.queryByText("events_src")).not.toBeInTheDocument();
    });
  });

  it("shows an empty state when filters match no rows", async () => {
    const user = userEvent.setup();
    await renderMaintainedObjects();

    await user.type(
      await screen.findByPlaceholderText("Search objects..."),
      "no_such_object",
    );

    expect(
      await screen.findByText("No objects match the selected filters"),
    ).toBeVisible();
  });

  it("handles empty list", async () => {
    await renderMaintainedObjects({ objects: [] });

    expect(
      await screen.findByText("No objects match the selected filters"),
    ).toBeVisible();
    expect(screen.queryByText("orders_mv")).not.toBeInTheDocument();
  });
});
