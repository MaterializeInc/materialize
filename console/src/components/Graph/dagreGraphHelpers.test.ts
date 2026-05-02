// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  clampEdgePoints,
  computeNodePositions,
  createDagreGraph,
  createWorkflowNodeBottomFn,
  DagreGraphNode,
  getGraphDimensions,
  MIN_EDGE_MARGIN,
  orderEdgesBySelection,
} from "./dagreGraphHelpers";

const createTestNodes = (): DagreGraphNode[] => [
  { id: "a", width: 100, height: 50 },
  { id: "b", width: 100, height: 50 },
  { id: "c", width: 100, height: 50 },
];

describe("createDagreGraph", () => {
  it("creates graph with nodes and edges using parentId/childId", () => {
    const nodes = createTestNodes();
    const edges = [{ parentId: "a", childId: "b" }];

    const graph = createDagreGraph(nodes, edges, 100);

    expect(graph.nodes()).toHaveLength(3);
    expect(graph.edges()).toHaveLength(1);
  });

  it("creates graph with nodes and edges using parentName/childName", () => {
    const nodes = createTestNodes();
    const edges = [{ parentName: "a", childName: "b" }];

    const graph = createDagreGraph(nodes, edges, 100);

    expect(graph.nodes()).toHaveLength(3);
    expect(graph.edges()).toHaveLength(1);
  });

  it("handles undefined nodes and edges", () => {
    const graph = createDagreGraph(undefined, undefined, 100);

    expect(graph.nodes()).toHaveLength(0);
    expect(graph.edges()).toHaveLength(0);
  });

  it("skips edges with null parent or child", () => {
    const nodes = createTestNodes();
    const edges = [{ parentId: null, childId: "b" }];

    const graph = createDagreGraph(nodes, edges, 100);

    expect(graph.edges()).toHaveLength(0);
  });
});

describe("computeNodePositions", () => {
  it("returns empty map for null graph", () => {
    const result = computeNodePositions(null, createTestNodes());
    expect(result.size).toBe(0);
  });

  it("computes positions for all nodes", () => {
    const nodes = createTestNodes();
    const graph = createDagreGraph(nodes, [], 100);

    const positions = computeNodePositions(graph, nodes);

    expect(positions.size).toBe(3);
    expect(positions.get("a")).toHaveProperty("top");
    expect(positions.get("a")).toHaveProperty("left");
    expect(positions.get("a")).toHaveProperty("right");
    expect(positions.get("a")).toHaveProperty("bottom");
  });

  it("uses custom getNodeBottom when provided", () => {
    const nodes = createTestNodes();
    const graph = createDagreGraph(nodes, [], 100);
    const customBottom = (_node: DagreGraphNode, pos: { top: number }) =>
      pos.top + 999;

    const positions = computeNodePositions(graph, nodes, customBottom);

    const nodeA = positions.get("a");
    expect(nodeA?.bottom).toBe(nodeA!.top + 999);
  });
});

describe("getGraphDimensions", () => {
  it("returns undefined for null graph", () => {
    const result = getGraphDimensions(null);
    expect(result).toEqual({ height: undefined, width: undefined });
  });

  it("returns dimensions for valid graph", () => {
    const graph = createDagreGraph(createTestNodes(), [], 100);

    const result = getGraphDimensions(graph);

    expect(result.height).toBeDefined();
    expect(result.width).toBeDefined();
  });
});

describe("orderEdgesBySelection", () => {
  it("returns empty array for null graph", () => {
    const result = orderEdgesBySelection(null, "a");
    expect(result).toEqual([]);
  });

  it("places selected node edges last", () => {
    const nodes = createTestNodes();
    const edges = [
      { parentId: "a", childId: "b" },
      { parentId: "b", childId: "c" },
    ];
    const graph = createDagreGraph(nodes, edges, 100);

    const result = orderEdgesBySelection(graph, "b");

    // Edges adjacent to "b" should be at the end
    const lastTwo = result.slice(-2);
    expect(lastTwo.every((e) => e.v === "b" || e.w === "b")).toBe(true);
  });

  it("returns all edges when no selection", () => {
    const nodes = createTestNodes();
    const edges = [{ parentId: "a", childId: "b" }];
    const graph = createDagreGraph(nodes, edges, 100);

    const result = orderEdgesBySelection(graph, undefined);

    expect(result).toHaveLength(1);
  });
});

describe("clampEdgePoints", () => {
  it("clamps points within bounds", () => {
    const points = [
      { x: -10, y: -10 },
      { x: 500, y: 500 },
    ];

    const result = clampEdgePoints(points, 100, 100);

    expect(result[0].x).toBe(MIN_EDGE_MARGIN);
    expect(result[0].y).toBe(MIN_EDGE_MARGIN);
    expect(result[1].x).toBe(100 - MIN_EDGE_MARGIN);
    expect(result[1].y).toBe(100 - MIN_EDGE_MARGIN);
  });

  it("preserves points already within bounds", () => {
    const points = [{ x: 50, y: 50 }];

    const result = clampEdgePoints(points, 100, 100);

    expect(result[0]).toEqual({ x: 50, y: 50 });
  });
});

describe("createWorkflowNodeBottomFn", () => {
  const baseHeight = 50;
  const heightWithStatus = 80;

  it("returns base height when no status", () => {
    const lagMap = new Map<string, unknown>();
    const nodeMap = new Map<string, { sourceStatus?: unknown }>();
    const fn = createWorkflowNodeBottomFn(
      lagMap,
      nodeMap,
      baseHeight,
      heightWithStatus,
    );

    const result = fn(
      { id: "a", width: 100, height: 50 },
      { left: 0, top: 10 },
    );

    expect(result).toBe(10 + baseHeight);
  });

  it("returns height with status when lag exists", () => {
    const lagMap = new Map([["a", { lag: 100 }]]);
    const nodeMap = new Map<string, { sourceStatus?: unknown }>();
    const fn = createWorkflowNodeBottomFn(
      lagMap,
      nodeMap,
      baseHeight,
      heightWithStatus,
    );

    const result = fn(
      { id: "a", width: 100, height: 50 },
      { left: 0, top: 10 },
    );

    expect(result).toBe(10 + heightWithStatus);
  });

  it("returns height with status when sourceStatus exists", () => {
    const lagMap = new Map<string, unknown>();
    const nodeMap = new Map([["a", { sourceStatus: "running" }]]);
    const fn = createWorkflowNodeBottomFn(
      lagMap,
      nodeMap,
      baseHeight,
      heightWithStatus,
    );

    const result = fn(
      { id: "a", width: 100, height: 50 },
      { left: 0, top: 10 },
    );

    expect(result).toBe(10 + heightWithStatus);
  });
});
