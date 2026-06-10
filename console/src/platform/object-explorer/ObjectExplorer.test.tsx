// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { DatabaseObject } from "~/api/materialize/objects";
import { AllNamespaceItem } from "~/api/materialize/schemaList";
import { getStore } from "~/jotai";
import { allNamespaceItems } from "~/platform/object-explorer/allNamespaces";
import { allObjects } from "~/store/allObjects";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { createProviderWrapper } from "~/test/utils";

import { ObjectExplorerRoutes } from "./ObjectExplorerRoutes";

// Mock the WebSocket subscription
vi.mock("~/platform/object-explorer/allNamespaces", async () => {
  const actual = await vi.importActual(
    "~/platform/object-explorer/allNamespaces",
  );
  return {
    ...actual,
    useSubscribeToAllNamespaces: vi.fn(() => {
      return {
        isLoading: false,
        isError: false,
        data: [],
        error: undefined,
        snapshotComplete: true,
      };
    }),

    allNamespaceItems: actual.allNamespaceItems,
  };
});

// Standard database with schema
const materializeDbPublicSchema = {
  schemaName: "public",
  databaseName: "materialize",
  databaseId: "u1",
  schemaId: "u1",
};

// Database with no schemas
const emptyDatabaseNoSchema = {
  schemaName: null,
  databaseName: "empty_db",
  databaseId: "db2",
  schemaId: null,
};

// A different database with a schema, for variety
const dbWithSchema = {
  schemaName: "analytics",
  databaseName: "metrics_db",
  databaseId: "db3",
  schemaId: "s2",
};

const schemaWithNoDB = {
  schemaName: "mz_catalog",
  databaseName: null,
  databaseId: null,
  schemaId: "s1",
};

const ordersTable: DatabaseObject = {
  databaseName: materializeDbPublicSchema.databaseName!,
  databaseId: materializeDbPublicSchema.databaseId!,
  name: "orders",
  schemaName: materializeDbPublicSchema.schemaName,
  schemaId: materializeDbPublicSchema.schemaId ?? "",
  id: "u123",
  objectType: "table",
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u1",
  clusterName: "quickstart",
};

const itemsTable: DatabaseObject = {
  databaseName: materializeDbPublicSchema.databaseName!,
  databaseId: materializeDbPublicSchema.databaseId!,
  name: "items",
  schemaName: materializeDbPublicSchema.schemaName ?? "",
  schemaId: materializeDbPublicSchema.schemaId ?? "",
  id: "u124",
  objectType: "table",
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u1",
  clusterName: "default",
};

const kafkaSource: DatabaseObject = {
  databaseName: materializeDbPublicSchema.databaseName!,
  databaseId: materializeDbPublicSchema.databaseId!,
  name: "kafka_source",
  schemaName: materializeDbPublicSchema.schemaName ?? "",
  schemaId: materializeDbPublicSchema.schemaId ?? "",
  id: "u125",
  objectType: "source",
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u1",
  clusterName: "default",
};

const Wrapper = await createProviderWrapper();

const renderComponent = () => {
  return render(
    <Wrapper>
      <Routes>
        <Route path="objects">
          <Route index path="*" element={<ObjectExplorerRoutes />} />
        </Route>
      </Routes>
    </Wrapper>,
  );
};

function getButton(name: string) {
  return screen.getByRole("button", { name });
}

function getLink(name: string) {
  return screen.getByRole("link", { name });
}

describe("ObjectExplorer", () => {
  beforeEach(() => {
    history.pushState(undefined, "", "/objects");
    const store = getStore();
    store.set(
      allObjects,
      mockSubscribeState({
        data: [ordersTable, itemsTable, kafkaSource],
        snapshotComplete: true,
        error: undefined,
      }),
    );
    store.set(
      allNamespaceItems,
      mockSubscribeState<AllNamespaceItem>({
        data: [
          materializeDbPublicSchema,
          emptyDatabaseNoSchema,
          dbWithSchema,
          schemaWithNoDB,
        ],
        snapshotComplete: true,
        error: undefined,
      }),
    );
  });

  it("Renders a tree with all nodes collapsed initially", async () => {
    const user = userEvent.setup();
    renderComponent();

    // 1. Check database nodes are visible initially
    const materializeLink = await screen.findByRole("link", {
      name: "materialize",
    });
    expect(materializeLink).toBeVisible();

    const emptyDbLink = await screen.findByRole("link", { name: "empty_db" });
    expect(emptyDbLink).toBeVisible();

    // Check another database with schema
    const metricsDbLink = await screen.findByRole("link", {
      name: "metrics_db",
    });
    expect(metricsDbLink).toBeVisible();

    const systemCatalogNodeText = await screen.findByText("System catalog");
    expect(systemCatalogNodeText).toBeVisible();

    let publicSchema = screen.queryByRole("link", { name: "public" });
    expect(publicSchema).toBeNull();
    expect(screen.queryByRole("link", { name: "analytics" })).toBeNull();
    expect(screen.queryByRole("link", { name: "mz_catalog" })).toBeNull();

    await user.click(materializeLink);

    publicSchema = await screen.findByRole("link", { name: "public" });
    expect(publicSchema).toBeVisible();

    let tables = screen.queryByRole("button", { name: "Tables" });
    expect(tables).toBeNull();

    await user.click(publicSchema);

    tables = await screen.findByRole("button", { name: "Tables" });
    expect(tables).toBeVisible();

    await user.click(tables);

    const ordersLink = await screen.findByRole("link", { name: "orders" });
    expect(ordersLink).toBeVisible();

    // check if system schemas (database -less schema) is visible
    await user.click(screen.getByRole("button", { name: "System catalog" }));
    const mzCatalogLink = await screen.findByRole("link", {
      name: "mz_catalog",
    });
    expect(mzCatalogLink).toBeVisible();

    await user.click(systemCatalogNodeText);
  });

  it("Collapses nodes on caret icon click", async () => {
    const user = userEvent.setup();
    renderComponent();

    const db = await screen.findByText("materialize");
    await user.click(db);
    let schema: HTMLElement | null = getLink("public");
    await user.click(schema);
    let tables: HTMLElement | null = getButton("Tables");
    await user.click(tables);
    expect(getLink("orders")).toBeVisible();

    await user.click(screen.getByRole("button", { name: "Collapse Tables" }));
    const ordersTableEl = screen.queryByRole("link", { name: "orders" });
    expect(ordersTableEl).toBeNull();

    await user.click(screen.getByRole("button", { name: "Collapse public" }));
    tables = screen.queryByRole("button", { name: "Tables" });
    expect(tables).toBeNull();

    await user.click(
      screen.getByRole("button", { name: "Collapse materialize" }),
    );
    schema = screen.queryByRole("link", { name: "public" });
    expect(schema).toBeNull();
  });

  it("Selection changes to nearest visible parent when collapsing nodes", async () => {
    const user = userEvent.setup();
    renderComponent();

    const db = await screen.findByText("materialize");
    await user.click(db);
    let schema: HTMLElement | null = getLink("public");
    await user.click(schema);
    const tables = getButton("Tables");
    await user.click(tables);
    await user.click(getLink("orders"));

    await user.click(getButton("Collapse Tables"));
    const ordersTableEl = screen.queryByRole("link", { name: "orders" });
    expect(ordersTableEl).toBeNull();
    // Since the object types are not selectable, the schema is selected
    expect(location.pathname).toEqual("/objects/materialize/schemas/public");

    await user.click(getButton("Collapse materialize"));
    schema = screen.queryByRole("link", { name: "public" });
    expect(schema).toBeNull();
    expect(location.pathname).toEqual("/objects/materialize");
  });

  it("Filtering by name", async () => {
    const user = userEvent.setup();
    renderComponent();

    await user.click(await screen.findByRole("link", { name: "materialize" }));
    await user.click(getLink("public"));
    await user.click(getButton("Tables"));
    const searchInput = screen.getByPlaceholderText("Search");
    await user.type(searchInput, "order");
    await waitFor(() => {
      expect(screen.getByRole("link", { name: "orders" })).toBeVisible();
      expect(screen.queryByRole("link", { name: "items" })).toBeNull();
    });

    await user.clear(searchInput);
    await waitFor(() => {
      expect(screen.getByRole("link", { name: "items" })).toBeVisible();
    });
  });

  it("Filtering by type", async () => {
    const user = userEvent.setup();
    renderComponent();

    const db = await screen.findByText("materialize");
    await user.click(db);
    await user.click(getLink("public"));

    // filter by type
    await user.click(screen.getByLabelText("Filter"));
    await user.click(screen.getByLabelText("Source"));
    // close the filter menu
    await user.click(screen.getByLabelText("Filter"));

    expect(screen.queryByRole("button", { name: "Connections" })).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Materialized Views" }),
    ).toBeNull();
    expect(screen.queryByRole("button", { name: "Sinks" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Tables" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Views" })).toBeNull();
    await user.click(getButton("Sources"));
    expect(screen.getByRole("link", { name: "kafka_source" })).toBeVisible();

    // remove type filter
    await user.click(screen.getByLabelText("Filter"));
    await user.click(screen.getByLabelText("Source"));
    // close the filter menu
    await user.click(screen.getByLabelText("Filter"));
    expect(
      await screen.findByRole("button", { name: "Connections" }),
    ).toBeVisible();
    expect(
      screen.getByRole("button", { name: "Materialized Views" }),
    ).toBeVisible();
    expect(screen.getByRole("button", { name: "Sinks" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Tables" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Views" })).toBeVisible();
  });
});
