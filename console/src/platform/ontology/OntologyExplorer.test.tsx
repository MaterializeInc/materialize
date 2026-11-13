// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen, waitFor } from "@testing-library/react";
import React from "react";
import { describe, expect, it } from "vitest";

import { renderComponent } from "~/test/utils";

import { buildDataProductGraph } from "./dataProductGraph";
import { OntologyExplorer } from "./OntologyExplorer";

const table = (id: string, name: string) => ({
  id,
  name,
  objectType: "table",
  schemaName: "shop",
  databaseName: "materialize",
  clusterName: null,
  sourceType: null,
});

const GRAPH = buildDataProductGraph({
  relations: [table("u1", "orders"), table("u2", "customers")],
  ontologyTables: [
    {
      id: "u9",
      name: "ontology",
      schemaName: "shop",
      databaseName: "materialize",
    },
  ],
  ontologyRows: [
    {
      ontologyId: "u9",
      tableName: "orders",
      columns: ["customer_id"],
      referencedTable: "customers",
      referencedColumns: ["id"],
    },
    {
      ontologyId: "u9",
      tableName: "payments",
      columns: ["order_id"],
      referencedTable: "orders",
      referencedColumns: ["id"],
    },
  ],
  lineage: [],
  columns: new Map([
    [
      "u1",
      [
        { name: "id", type: "bigint", description: null, semanticType: null },
        {
          name: "customer_id",
          type: "bigint",
          description: "Who placed the order.",
          semanticType: null,
        },
      ],
    ],
    [
      "u2",
      [{ name: "id", type: "bigint", description: null, semanticType: null }],
    ],
  ]),
  comments: new Map([["u1", "Every order placed."]]),
  dataProducts: new Set(['"materialize"."shop"."orders"']),
});

const renderExplorer = (initialEntry = "/") =>
  renderComponent(
    <OntologyExplorer
      name="test"
      graph={GRAPH}
      allGroups={["materialize.shop"]}
      groups={null}
      onGroupsChange={() => {}}
      defaultShowEdgeLabels
      renderOverview={({ graph }) => (
        <div>Overview with {graph.issues.length} issue(s)</div>
      )}
      previewCluster={() => null}
      isLive
      suggestComments
      isReady
    />,
    { initialRouterEntries: [initialEntry] },
  );

describe("OntologyExplorer", () => {
  it("draws entities, the unresolved reference and the summary", async () => {
    await renderExplorer();
    expect(await screen.findByTestId("ontology-node-orders")).toBeVisible();
    expect(screen.getByTestId("ontology-node-customers")).toBeVisible();
    expect(screen.getByTestId("ontology-node-payments")).toBeVisible();
    expect(screen.getByText("Overview with 1 issue(s)")).toBeVisible();
    expect(screen.getByText("Live")).toBeVisible();
  });

  it("shows an entity's details when it is selected", async () => {
    await renderExplorer();
    fireEvent.click(await screen.findByTestId("ontology-node-orders"));
    const panel = screen.getByTestId("ontology-side-panel");
    await waitFor(() => expect(panel).toHaveTextContent("Every order placed."));
    expect(panel).toHaveTextContent("MCP data product");
    expect(panel).toHaveTextContent("customer_id");
    expect(panel).toHaveTextContent("Who placed the order.");
  });

  it("opens a relationship from the URL with its join", async () => {
    const join = GRAPH.edges.find((edge) => edge.source === "u1");
    await renderExplorer(`/?edge=${encodeURIComponent(join?.id ?? "")}`);
    const panel = await screen.findByTestId("ontology-side-panel");
    await waitFor(() =>
      expect(panel).toHaveTextContent('ON a."customer_id" = b."id"'),
    );
    expect(panel).toHaveTextContent("Declared inmaterialize.shop.ontology");
  });

  it("filters entities by search", async () => {
    await renderExplorer();
    fireEvent.change(await screen.findByLabelText("Search entities"), {
      target: { value: "cust" },
    });
    fireEvent.keyDown(screen.getByLabelText("Search entities"), {
      key: "Enter",
    });
    await waitFor(() =>
      expect(screen.getByTestId("ontology-side-panel")).toHaveTextContent(
        "customers",
      ),
    );
  });

  it("hides the side panel until something is selected", async () => {
    await renderExplorer();
    fireEvent.click(await screen.findByRole("button", { name: "Hide panel" }));
    expect(screen.queryByTestId("ontology-side-panel")).toBeNull();
    fireEvent.click(screen.getByTestId("ontology-node-customers"));
    expect(await screen.findByTestId("ontology-side-panel")).toBeVisible();
    expect(screen.getByRole("button", { name: "Hide panel" })).toBeVisible();
  });
});
