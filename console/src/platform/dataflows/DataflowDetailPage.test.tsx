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
import { http, HttpResponse } from "msw";
import React from "react";
import { Route, Routes } from "react-router-dom";

import type { Cluster } from "~/api/materialize/cluster/clusterList";
import { ErrorCode } from "~/api/materialize/types";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent } from "~/test/utils";

import DataflowDetailPage from "./DataflowDetailPage";

// jsdom lacks ResizeObserver/DOMMatrixReadOnly, so replace @xyflow/react with a
// flat renderer that preserves the decision under test (whether the canvas and
// its nodes exist). This mirrors src/test/mockReactFlow.tsx but also stubs
// useReactFlow, which DataflowDetailPage's centering helper reads and the
// shared helper does not provide.
vi.mock("@xyflow/react", () => ({
  ReactFlow: ({
    nodes,
    children,
  }: {
    nodes: { id: string; data: { node: { label: string } } }[];
    children?: React.ReactNode;
  }) => (
    <div data-testid="react-flow">
      {nodes.map((n) => (
        <div key={n.id} data-testid={`node-${n.id}`}>
          {n.data.node.label}
        </div>
      ))}
      {children}
    </div>
  ),
  Background: () => null,
  Controls: () => null,
  MiniMap: () => null,
  Handle: () => null,
  Position: { Left: "left", Right: "right", Top: "top", Bottom: "bottom" },
  BaseEdge: () => null,
  EdgeLabelRenderer: ({ children }: { children: React.ReactNode }) => (
    <>{children}</>
  ),
  getBezierPath: () => ["", 0, 0],
  useReactFlow: () => ({
    getInternalNode: () => undefined,
    setCenter: () => {},
  }),
}));

// elkjs runs in a web worker that jsdom cannot load, so replace the layout hook
// with a synchronous stub. The Proxy resolves any node id to a fixed box, so
// every visible node lands on the mocked React Flow canvas.
vi.mock("./useElkLayout", () => ({
  useElkLayout: () => ({
    positions: new Proxy(
      {},
      { get: () => ({ x: 0, y: 0, width: 100, height: 40 }) },
    ),
    layouting: false,
    error: null,
  }),
}));

const FAILING_REPLICA = "r2";

const cluster = {
  id: "u5",
  name: "test_cluster",
  replicas: [{ name: "r1" }, { name: "r2" }],
} as unknown as Cluster;

const okResult = {
  desc: { columns: [] },
  rows: [],
};
// Two operators (a root and one child) so buildDataflowStructure yields a
// graph with more than one node and DataflowDetailPage renders the canvas.
const operatorsResult = {
  desc: {
    columns: [
      { name: "id" },
      { name: "address" },
      { name: "name" },
      { name: "arrangementRecords" },
      { name: "arrangementSize" },
      { name: "elapsedNs" },
    ],
  },
  rows: [
    ["10", ["7"], "Dataflow", "0", "0", "0"],
    ["11", ["7", "1"], "Map", "0", "0", "1"],
  ],
};

beforeEach(() => {
  const store = getStore();
  store.set(allClusters, mockSubscribeState({ data: [cluster] }));
  server.use(
    http.post("*/api/sql", async ({ request }) => {
      const options = JSON.parse(
        new URL(request.url).searchParams.get("options") ?? "{}",
      );
      if (options.cluster_replica === FAILING_REPLICA) {
        return HttpResponse.json({
          results: [
            {
              error: {
                message: "no such replica",
                code: ErrorCode.INTERNAL_ERROR,
              },
            },
          ],
        });
      }
      return HttpResponse.json({
        results: [operatorsResult, okResult, okResult],
      });
    }),
  );
});

describe("DataflowDetailPage", () => {
  // CNS-109: switching to a replica whose query fails must surface the error
  // state and drop the graph from the previously successful replica.
  it("shows the error state and hides the stale graph when the new replica fails", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/7"],
      },
    );

    // r1 succeeds, so the graph renders first.
    expect(await screen.findByTestId("react-flow")).toBeVisible();

    // Switch to r2, whose query fails. The toolbar renders its own selects, so
    // pick the replica select by its options rather than the ambiguous role.
    const replicaSelect = screen
      .getAllByRole("combobox")
      .find((select) =>
        Array.from((select as HTMLSelectElement).options).some(
          (option) => option.value === FAILING_REPLICA,
        ),
      );
    expect(replicaSelect).toBeDefined();
    await userEvent.selectOptions(replicaSelect!, FAILING_REPLICA);

    expect(
      await screen.findByText("There was an error visualizing your dataflow"),
    ).toBeVisible();
    // The stale graph from r1 must be gone.
    expect(screen.queryByTestId("react-flow")).toBeNull();
  });
});
