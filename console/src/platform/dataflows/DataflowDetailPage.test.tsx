// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";
import {
  Route,
  Routes,
  useLocation,
  useNavigationType,
} from "react-router-dom";

import type { Cluster } from "~/api/materialize/cluster/clusterList";
import { ErrorCode } from "~/api/materialize/types";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent } from "~/test/utils";

import DataflowDetailPage from "./DataflowDetailPage";
import { nodeIdOf } from "./dataflowGraph";

// Declared via vi.hoisted so the vi.mock factory below (itself hoisted
// above regular imports) can close over it.
const fitViewSpy = vi.hoisted(() => vi.fn());

// jsdom lacks ResizeObserver/DOMMatrixReadOnly, so replace @xyflow/react with a
// flat renderer that preserves the decision under test (whether the canvas and
// its nodes exist). This mirrors src/test/mockReactFlow.tsx but also stubs
// useReactFlow, which DataflowDetailPage's centering helper reads and the
// shared helper does not provide. Click/double-click are wired through so
// selection and drill-down navigation can be driven from tests too.
vi.mock("@xyflow/react", () => ({
  ReactFlow: ({
    nodes,
    children,
    onNodeClick,
    onNodeDoubleClick,
  }: {
    nodes: {
      id: string;
      type?: string;
      data: {
        node?: { label: string };
        label?: string;
        onToggleExpand?: (id: string) => void;
      };
    }[];
    children?: React.ReactNode;
    onNodeClick?: (e: unknown, node: unknown) => void;
    onNodeDoubleClick?: (e: unknown, node: unknown) => void;
  }) => (
    <div data-testid="react-flow">
      {nodes.map((n) => {
        const node = n.data.node;
        return (
          <div
            key={n.id}
            data-testid={`node-${n.id}`}
            onClick={() => onNodeClick?.(null, n)}
            onDoubleClick={() => onNodeDoubleClick?.(null, n)}
          >
            {/* Mirrors RegionNode's own disclosure triangle (nodes.tsx),
                just enough to drive the toggle from a test: the flat
                renderer above never instantiates the real node components. */}
            {n.type === "region" && node && (
              <button
                type="button"
                data-testid="region-toggle"
                onClick={(e) => {
                  e.stopPropagation();
                  n.data.onToggleExpand?.(n.id);
                }}
              />
            )}
            {node ? node.label : n.data.label}
          </div>
        );
      })}
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
    fitView: fitViewSpy,
    getViewport: () => ({ x: 0, y: 0, zoom: 1 }),
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
// The page also renders a dataflow switcher (useDataflowList), a separate
// request from the graph data's three queries above.
const dataflowListResult = {
  desc: {
    columns: [
      { name: "id" },
      { name: "name" },
      { name: "records" },
      { name: "size" },
      { name: "elapsedNs" },
    ],
  },
  rows: [["7", "Dataflow: mv", "0", "0", "0"]],
};

// Dataflow 8: root [8], sibling regions RegionA [8,1] (leaf LeafA [8,1,1]) and
// RegionB [8,2] (leaf LeafB [8,2,1]), with RegionA's output feeding RegionB's
// input directly (a plain sibling-to-sibling crossing, not nested).
const portJumpOperatorsResult = {
  ...operatorsResult,
  rows: [
    ["20", ["8"], "Dataflow", "0", "0", "0"],
    ["21", ["8", "1"], "RegionA", "0", "0", "0"],
    ["22", ["8", "1", "1"], "LeafA", "0", "0", "1"],
    ["23", ["8", "2"], "RegionB", "0", "0", "0"],
    ["24", ["8", "2", "1"], "LeafB", "0", "0", "1"],
  ],
};
const portJumpChannelsResult = {
  desc: {
    columns: [
      { name: "id" },
      { name: "fromOperatorAddress" },
      { name: "fromPort" },
      { name: "toOperatorAddress" },
      { name: "toPort" },
      { name: "messagesSent" },
      { name: "batchesSent" },
      { name: "channelType" },
    ],
  },
  rows: [["1", ["8", "1"], "0", ["8", "2"], "0", "5", "2", "rows"]],
};

// Dataflow 9: RegionA [9,1]'s single output port (0) fans out to two
// siblings, RegionB [9,2] and RegionC [9,3] (1:n, not 1:1).
const fanOutOperatorsResult = {
  ...operatorsResult,
  rows: [
    ["30", ["9"], "Dataflow", "0", "0", "0"],
    ["31", ["9", "1"], "RegionA", "0", "0", "0"],
    ["32", ["9", "1", "1"], "LeafA", "0", "0", "1"],
    ["33", ["9", "2"], "RegionB", "0", "0", "0"],
    ["34", ["9", "2", "1"], "LeafB", "0", "0", "1"],
    ["35", ["9", "3"], "RegionC", "0", "0", "0"],
    ["36", ["9", "3", "1"], "LeafC", "0", "0", "1"],
  ],
};
const fanOutChannelsResult = {
  ...portJumpChannelsResult,
  rows: [
    ["1", ["9", "1"], "0", ["9", "2"], "0", "5", "2", "rows"],
    ["2", ["9", "1"], "0", ["9", "3"], "0", "3", "1", "rows"],
  ],
};

// Dataflow 40: root [40], child [40,1] "Join" (operator id 41), covered by
// one LIR span, to exercise the "Show LIR groups" toggle in isolation from
// every other test's dataflow 7 fixture, which has no LIR spans at all.
const lirOperatorsResult = {
  ...operatorsResult,
  rows: [
    ["40", ["40"], "Dataflow", "0", "0", "0"],
    ["41", ["40", "1"], "Join", "0", "0", "0"],
  ],
};
const lirSpansResult = {
  desc: {
    columns: [
      { name: "exportId" },
      { name: "lirId" },
      { name: "parentLirId" },
      { name: "nesting" },
      { name: "operator" },
      { name: "operatorIdStart" },
      { name: "operatorIdEnd" },
    ],
  },
  rows: [["u7", "1", null, "0", "Join::Differential", "41", "42"]],
};

// Dataflow 50: root [50], leaf Join [50,1] (op 51) covered by a LIR span at
// root scope, and unrelated sibling region RegionA [50,2] (op 52) with child
// LeafA [50,2,1] (op 53). Exercises the LIR-grouping/in-place-expansion
// interlock: Join's group box must disappear the instant RegionA is
// expanded, even though the two aren't nested under each other.
const expandInteractionOperatorsResult = {
  ...operatorsResult,
  rows: [
    ["50", ["50"], "Dataflow", "0", "0", "0"],
    ["51", ["50", "1"], "Join", "0", "0", "0"],
    ["52", ["50", "2"], "RegionA", "0", "0", "0"],
    ["53", ["50", "2", "1"], "LeafA", "0", "0", "1"],
  ],
};
const expandInteractionSpansResult = {
  ...lirSpansResult,
  rows: [["u8", "1", null, "0", "Join::Differential", "51", "52"]],
};

beforeEach(() => {
  fitViewSpy.mockClear();
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
      const body = await request.text();
      // useDataflowList's query targets mz_dataflows directly; none of
      // useDataflowGraphData's three queries reference that exact table name.
      if (body.includes("mz_dataflows")) {
        return HttpResponse.json({ results: [dataflowListResult] });
      }
      // The dataflow id is interpolated as a SQL literal, so it shows up
      // verbatim in the compiled query text.
      if (body.includes("'8'")) {
        return HttpResponse.json({
          results: [
            portJumpOperatorsResult,
            portJumpChannelsResult,
            okResult,
            okResult,
            okResult,
          ],
        });
      }
      if (body.includes("'9'")) {
        return HttpResponse.json({
          results: [
            fanOutOperatorsResult,
            fanOutChannelsResult,
            okResult,
            okResult,
            okResult,
          ],
        });
      }
      if (body.includes("'40'")) {
        return HttpResponse.json({
          results: [
            lirOperatorsResult,
            okResult,
            lirSpansResult,
            okResult,
            okResult,
          ],
        });
      }
      if (body.includes("'50'")) {
        return HttpResponse.json({
          results: [
            expandInteractionOperatorsResult,
            okResult,
            expandInteractionSpansResult,
            okResult,
            okResult,
          ],
        });
      }
      return HttpResponse.json({
        results: [operatorsResult, okResult, okResult, okResult, okResult],
      });
    }),
  );
});

// Exposes the current URL search string so a test can assert the
// drill-down scope actually landed in the URL, not just in the rendered
// graph (which would still look right if it were only in component state).
const LocationProbe = () => (
  <div data-testid="location-search">{useLocation().search}</div>
);

// Exposes the type of the most recent navigation, so a test can tell a
// `replace` (no new history entry) apart from a `push` without any direct
// access to the router's own history stack (MemoryRouter doesn't expose one).
const NavigationProbe = () => (
  <div data-testid="navigation-type">{useNavigationType()}</div>
);

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

  // A port's peer jump link should work the same regardless of which
  // direction it represents: this drills into RegionA, whose only channel
  // is its own output feeding sibling RegionB's input, and checks that
  // jumping from the resulting "out" port's peer actually navigates.
  it("jumps to an output port's peer, same as an input port's peer", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/8"],
      },
    );

    const regionAId = nodeIdOf([8, 1]);
    const regionBId = nodeIdOf([8, 2]);
    expect(await screen.findByTestId(`node-${regionAId}`)).toHaveTextContent(
      "RegionA",
    );

    // Drill into RegionA: its only channel is its own output (port 0)
    // reaching sibling RegionB, which isn't part of this view, so it
    // should surface as a dangling "out" port.
    await userEvent.dblClick(screen.getByTestId(`node-${regionAId}`));
    const outPortId = `${regionAId}:out:0`;
    const outPortNode = await screen.findByTestId(`node-${outPortId}`);

    // Select the port to open its detail panel, then jump its peer.
    await userEvent.click(outPortNode);
    expect(await screen.findByText(/RegionB/)).toBeVisible();
    const jumpLink = await screen.findByRole("button", { name: /RegionB/ });
    await userEvent.click(jumpLink);

    // RegionB is itself a region, so jumping drills straight into it and
    // selects the matching "in" port there (port 0, matching the channel's
    // toPort), rather than just landing outside it as an unlabeled box.
    const regionBInPortId = `${regionBId}:in:0`;
    expect(await screen.findByTestId(`node-${regionBInPortId}`)).toBeVisible();
    // Its detail panel is open, selected: a second "input 0" text node (the
    // panel title) alongside the port box's own label.
    expect(await screen.findAllByText("input 0")).toHaveLength(2);
  });

  // A 1:1 port doesn't need the detail panel detour: double-clicking it
  // jumps directly, the same destination as clicking through to its Jump
  // button.
  it("double-clicking a port with exactly one peer jumps directly", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/8"],
      },
    );

    const regionAId = nodeIdOf([8, 1]);
    const regionBId = nodeIdOf([8, 2]);
    await screen.findByTestId(`node-${regionAId}`);
    await userEvent.dblClick(screen.getByTestId(`node-${regionAId}`));

    const outPortId = `${regionAId}:out:0`;
    const outPortNode = await screen.findByTestId(`node-${outPortId}`);
    await userEvent.dblClick(outPortNode);

    const regionBInPortId = `${regionBId}:in:0`;
    expect(await screen.findByTestId(`node-${regionBInPortId}`)).toBeVisible();
    expect(await screen.findAllByText("input 0")).toHaveLength(2);
  });

  // The drill-down scope has to actually be in the URL, not just reflected
  // in the rendered graph, for a copied link or the back button to work.
  it("puts the drill-down scope in the URL, so a link into it reopens there directly", async () => {
    const { unmount } = await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={
            <>
              <DataflowDetailPage />
              <LocationProbe />
            </>
          }
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/8"],
      },
    );

    const regionAId = nodeIdOf([8, 1]);
    await screen.findByTestId(`node-${regionAId}`);
    // A fresh, undrilled link carries no scope param at all.
    expect(screen.getByTestId("location-search").textContent).toBe("");

    await userEvent.dblClick(screen.getByTestId(`node-${regionAId}`));
    await screen.findByTestId(`node-${nodeIdOf([8, 1, 1])}`);
    const search = screen.getByTestId("location-search").textContent;
    expect(search).toBe("?scope=8.1");

    unmount();

    // Reopening at that exact URL, with no clicks, lands drilled in already.
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: [
          `/clusters/u5/test_cluster/dataflows/8${search}`,
        ],
      },
    );
    expect(
      await screen.findByTestId(`node-${nodeIdOf([8, 1, 1])}`),
    ).toBeVisible();
  });

  // Selection lives in the URL too, the same as scope: opening a link
  // straight at `select=...` (indistinguishable, from react-router's
  // perspective, from landing there via the back/forward buttons) must
  // restore the panel without any click, and fit the viewport to it since
  // nothing guarantees it's already onscreen.
  it("restores selection from a direct link and fits the viewport to it", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: [
          `/clusters/u5/test_cluster/dataflows/8?scope=8.1&select=${encodeURIComponent(`node:${nodeIdOf([8, 1, 1])}`)}`,
        ],
      },
    );

    // The panel title and the node's own label both render "LeafA".
    expect(await screen.findAllByText("LeafA")).toHaveLength(2);
    // Present only in the detail panel's own rendering, confirming the
    // panel (not just the node) is open.
    expect(screen.getByText("Kind")).toBeInTheDocument();
    expect(fitViewSpy).toHaveBeenCalledTimes(1);
  });

  // elk lays each scope out from scratch, so a viewport left over from
  // wherever the user was in the previous scope can easily miss the new
  // one entirely. Navigating into a scope should recenter for that reason,
  // but an unrelated re-render within the same scope (e.g. selecting a
  // node) must not refit again, or it would fight the user's own panning.
  it("recenters after navigating into a scope, but not again within it", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/8"],
      },
    );

    const regionAId = nodeIdOf([8, 1]);
    await screen.findByTestId(`node-${regionAId}`);
    // The initial scope is skipped: React Flow's own `fitView` prop already
    // covers first load.
    expect(fitViewSpy).not.toHaveBeenCalled();

    await userEvent.dblClick(screen.getByTestId(`node-${regionAId}`));
    const leafAId = nodeIdOf([8, 1, 1]);
    await screen.findByTestId(`node-${leafAId}`);
    expect(fitViewSpy).toHaveBeenCalledTimes(1);

    await userEvent.click(screen.getByTestId(`node-${leafAId}`));
    expect(fitViewSpy).toHaveBeenCalledTimes(1);
  });

  // A 1:n port (RegionA's single output feeding both RegionB and RegionC)
  // can't pick a jump target on its own; double-clicking it should behave
  // like a plain click (open its own detail panel) rather than guess.
  it("double-clicking a fanned-out port selects it instead of guessing a peer", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/9"],
      },
    );

    const regionAId = nodeIdOf([9, 1]);
    await screen.findByTestId(`node-${regionAId}`);
    await userEvent.dblClick(screen.getByTestId(`node-${regionAId}`));

    const outPortId = `${regionAId}:out:0`;
    const outPortNode = await screen.findByTestId(`node-${outPortId}`);
    await userEvent.dblClick(outPortNode);

    // Still inside RegionA: neither RegionB nor RegionC drilled into.
    expect(screen.queryByTestId(`node-${nodeIdOf([9, 2])}:in:0`)).toBeNull();
    expect(screen.queryByTestId(`node-${nodeIdOf([9, 3])}:in:0`)).toBeNull();
    // The port itself is selected, showing both peers to choose from.
    expect(await screen.findAllByText("output 0")).toHaveLength(2);
    expect(screen.getByText(/RegionB/)).toBeVisible();
    expect(screen.getByText(/RegionC/)).toBeVisible();
  });

  // A scope drilled into on one dataflow is a node id that (almost
  // certainly) doesn't exist in a different dataflow's structure. Switching
  // must fall back to the new structure's root instead of crashing on a
  // `nodes.get(focusedScope)!` lookup for an id the new data doesn't have.
  it("falls back to the new root when switching dataflows while drilled into a scope", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
        {/* The dataflow switcher navigates through absoluteClusterPath,
            which prefixes the region slug; the initial entry below doesn't
            (matching every other test in this file), so both shapes need a
            matching route. */}
        <Route
          path="/regions/:regionSlug/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/8"],
      },
    );

    const regionAId = nodeIdOf([8, 1]);
    await screen.findByTestId(`node-${regionAId}`);
    await userEvent.dblClick(screen.getByTestId(`node-${regionAId}`));
    // Drilled in: RegionA's own id no longer names anything in dataflow 7.
    await screen.findByTestId(`node-${regionAId}:out:0`);

    const dataflowSelect = screen
      .getAllByRole("combobox")
      .find((select) =>
        Array.from((select as HTMLSelectElement).options).some(
          (option) => option.value === "7",
        ),
      );
    expect(dataflowSelect).toBeDefined();
    await userEvent.selectOptions(dataflowSelect!, "7");

    // No crash, and the new dataflow's own root renders.
    expect(
      await screen.findByTestId(`node-${nodeIdOf([7, 1])}`),
    ).toHaveTextContent("Map");
  });

  it("shows a LIR group box by default, and hides it when the toggle is turned off", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/40"],
      },
    );

    expect(
      await screen.findByTestId(`node-${nodeIdOf([40, 1])}`),
    ).toBeInTheDocument();
    // On by default.
    const groupNode = screen.getByTestId("node-u7/1");
    expect(groupNode).toHaveTextContent("Join::Differential");

    // Regression check for the onNodeDoubleClick guard added in Step 4:
    // double-clicking a group must not throw (it read `.data.node.kind`
    // unconditionally before the guard, which crashes on a group node) and
    // must not navigate anywhere, since groups aren't scopes.
    await userEvent.dblClick(groupNode);
    expect(screen.getByTestId("node-u7/1")).toBeInTheDocument();

    await userEvent.click(screen.getByLabelText("Show LIR groups"));
    expect(
      within(screen.getByTestId("react-flow")).queryByText(
        /Join::Differential/,
      ),
    ).not.toBeInTheDocument();
  });

  it("opens the detail panel with LIR summary info when a group header is clicked", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/40"],
      },
    );

    await screen.findByTestId(`node-${nodeIdOf([40, 1])}`);
    await userEvent.click(screen.getByTestId("node-u7/1"));

    // The panel title and the LIR sidebar's own row both render "LIR 1:
    // Join::Differential", so assert at least one match rather than a
    // single getByText, which throws on more than one hit.
    expect(
      screen.getAllByText("LIR 1: Join::Differential").length,
    ).toBeGreaterThan(0);
    // Rendered the same Row-shaped way as a regular node's details, not a
    // visually distinct card.
    expect(screen.getByText("Export")).toBeInTheDocument();
    expect(screen.getByText("Members")).toBeInTheDocument();
    expect(screen.getByText("Records")).toBeInTheDocument();
    expect(screen.getByText("Memory")).toBeInTheDocument();
  });

  describe("region in-place expansion", () => {
    it("toggling a region writes the expand param without a history push", async () => {
      await renderComponent(
        <Routes>
          <Route
            path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
            element={
              <>
                <DataflowDetailPage />
                <LocationProbe />
                <NavigationProbe />
              </>
            }
          />
        </Routes>,
        {
          initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/8"],
        },
      );

      const regionAId = nodeIdOf([8, 1]);
      const regionANode = await screen.findByTestId(`node-${regionAId}`);
      await userEvent.click(within(regionANode).getByTestId("region-toggle"));

      expect(screen.getByTestId("location-search").textContent).toBe(
        "?expand=8.1",
      );
      // Toggling replaces the current history entry rather than pushing a
      // new one: expand/collapse clicks would otherwise spam the back
      // button, unlike an actual scope navigation.
      expect(screen.getByTestId("navigation-type").textContent).toBe("REPLACE");
    });

    it("navigating into a scope clears the expand param", async () => {
      await renderComponent(
        <Routes>
          <Route
            path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
            element={
              <>
                <DataflowDetailPage />
                <LocationProbe />
              </>
            }
          />
        </Routes>,
        {
          initialRouterEntries: [
            "/clusters/u5/test_cluster/dataflows/8?expand=8.1",
          ],
        },
      );

      const regionAId = nodeIdOf([8, 1]);
      await screen.findByTestId(`node-${regionAId}`);
      await userEvent.dblClick(screen.getByTestId(`node-${regionAId}`));

      await screen.findByTestId(`node-${nodeIdOf([8, 1, 1])}`);
      expect(screen.getByTestId("location-search").textContent).toBe(
        "?scope=8.1",
      );
    });

    it("does not group by LIR while a region is expanded", async () => {
      await renderComponent(
        <Routes>
          <Route
            path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
            element={<DataflowDetailPage />}
          />
        </Routes>,
        {
          initialRouterEntries: [
            "/clusters/u5/test_cluster/dataflows/50?expand=50.2",
          ],
        },
      );

      await screen.findByTestId(`node-${nodeIdOf([50, 2])}`);
      // The LIR group box that would otherwise wrap Join is suppressed while
      // RegionA is expanded, even though the two are unrelated siblings.
      expect(screen.queryByTestId("node-u8/1")).not.toBeInTheDocument();
      // Join itself still renders, just ungrouped.
      expect(
        screen.getByTestId(`node-${nodeIdOf([50, 1])}`),
      ).toBeInTheDocument();
    });
  });
});
