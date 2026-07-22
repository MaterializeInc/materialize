// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import ELK from "elkjs";
import { describe, expect, it } from "vitest";

import {
  buildDataflowStructure,
  deriveVisibleGraph,
  groupByLir,
  nodeIdOf,
  type VisibleNode,
} from "./dataflowGraph";
import { CHANNELS, LIR_SPANS, OPS } from "./dataflowGraph.test";
import {
  extractPositions,
  NODE_DIMENSIONS,
  type Positions,
  resolveAbsolutePositions,
  toElkGraph,
} from "./elkGraph";

describe("toElkGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);

  it("lists every visible node flat, at its kind's dimensions", () => {
    const elk = toElkGraph(deriveVisibleGraph(s, nodeIdOf([5, 1])));
    const ids = elk.children!.map((c) => c.id);
    expect(ids).toEqual(
      expect.arrayContaining([
        nodeIdOf([5, 1, 1]),
        nodeIdOf([5, 1, 2]),
        `${nodeIdOf([5, 1])}:in:0`,
      ]),
    );
    const leaf = elk.children!.find((c) => c.id === nodeIdOf([5, 1, 1]))!;
    expect(leaf.width).toEqual(NODE_DIMENSIONS.operator.width);
    // Flat: no node carries children of its own.
    for (const c of elk.children!) expect(c.children).toBeUndefined();
  });

  it("round-trips positions", () => {
    const elk = toElkGraph(deriveVisibleGraph(s, s.root));
    // simulate a layout result
    elk.children![0].x = 10;
    elk.children![0].y = 20;
    const positions = extractPositions(elk);
    expect(positions[elk.children![0].id]).toMatchObject({ x: 10, y: 20 });
  });
});

describe("toElkGraph with a LIR grouping", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);

  it("nests a group's members as elk children, sized only by content", () => {
    const visible = deriveVisibleGraph(s, s.root);
    const grouping = groupByLir(visible.nodes);
    const elk = toElkGraph(visible, grouping);
    const group = elk.children!.find((c) => c.id === grouping.groups[0].id)!;
    expect(group.children!.map((c) => c.id)).toEqual(
      expect.arrayContaining([nodeIdOf([5, 1])]),
    );
    // Auto-sized by elk from its content, not one of NODE_DIMENSIONS's fixed sizes.
    expect(group.width).toBeUndefined();
    expect(group.height).toBeUndefined();
    // Flat sibling stays a direct child of the root, not absorbed into the group.
    expect(elk.children!.map((c) => c.id)).toEqual(
      expect.arrayContaining([nodeIdOf([5, 2])]),
    );
  });

  it("keeps every edge at the root level regardless of nesting depth", () => {
    const visible = deriveVisibleGraph(s, s.root);
    const grouping = groupByLir(visible.nodes);
    const elk = toElkGraph(visible, grouping);
    expect(elk.edges!.map((e) => e.id)).toEqual(visible.edges.map((e) => e.id));
    for (const child of elk.children!) expect(child.edges).toBeUndefined();
  });

  it("round-trips nested positions unchanged (parent-relative, no conversion)", () => {
    const visible = deriveVisibleGraph(s, s.root);
    const grouping = groupByLir(visible.nodes);
    const elk = toElkGraph(visible, grouping);
    const group = elk.children!.find((c) => c.id === grouping.groups[0].id)!;
    group.x = 12;
    group.y = 87;
    group.width = 132;
    group.height = 259;
    const member = group.children!.find((c) => c.id === nodeIdOf([5, 1]))!;
    member.x = 16;
    member.y = 24;
    const positions = extractPositions(elk);
    // The member's extracted position is exactly its own x/y, not offset by
    // the group's: elk already reports it relative to the group.
    expect(positions[member.id]).toMatchObject({ x: 16, y: 24 });
    expect(positions[group.id]).toMatchObject({
      x: 12,
      y: 87,
      width: 132,
      height: 259,
    });
  });

  it("real elk lays out nested groups with auto-sized bounds and positions relative to parent", async () => {
    // Empirical check: elk auto-sizes a compound node from its children,
    // reports child positions relative to their own parent (matching React
    // Flow's parentId convention with no coordinate conversion needed), and
    // processes edges declared at the root even with nested containers present,
    // given hierarchyHandling: INCLUDE_CHILDREN.
    const elk = new ELK();
    const graph = {
      id: "root",
      layoutOptions: {
        "elk.algorithm": "layered",
        "elk.direction": "DOWN",
        "elk.hierarchyHandling": "INCLUDE_CHILDREN",
      },
      children: [
        { id: "ungrouped", width: 100, height: 50 },
        {
          id: "groupA",
          layoutOptions: { "elk.padding": "[top=24,left=8,bottom=8,right=8]" },
          children: [
            { id: "a1", width: 100, height: 50 },
            {
              id: "groupA_inner",
              layoutOptions: {
                "elk.padding": "[top=24,left=8,bottom=8,right=8]",
              },
              children: [
                { id: "a2", width: 100, height: 50 },
                { id: "a3", width: 100, height: 50 },
              ],
            },
          ],
        },
      ],
      edges: [
        { id: "e1", sources: ["ungrouped"], targets: ["a1"] },
        { id: "e2", sources: ["a1"], targets: ["a2"] },
        { id: "e3", sources: ["a2"], targets: ["a3"] },
      ],
    };
    const result = await elk.layout(graph);
    const groupA = result.children!.find((c) => c.id === "groupA")!;
    const groupAInner = groupA.children!.find((c) => c.id === "groupA_inner")!;
    // Auto-sized larger than any single member: real content-driven sizing.
    expect(groupA.width!).toBeGreaterThan(100);
    expect(groupA.height!).toBeGreaterThan(50);
    // All edges remain at root even though nested containers are present;
    // they layout correctly with hierarchyHandling: INCLUDE_CHILDREN.
    expect((result.edges ?? []).map((e) => e.id)).toEqual(
      expect.arrayContaining(["e1", "e2", "e3"]),
    );
    // Nested child positions are relative to their immediate parent (groupA_inner).
    // hierarchyHandling: INCLUDE_CHILDREN is the discriminating option that changes
    // elk's nested layout: without it, a3 lands at x:128, y:24 instead.
    // Pinning this exact position discriminates the option's presence in production.
    const a3 = groupAInner.children!.find((c) => c.id === "a3")!;
    expect(a3).toMatchObject({ x: 8, y: 94 });
  });
});

// Minimal VisibleNode fixture: fills every required field with a null/zero
// placeholder, and lets a test override only the fields it cares about
// (kind, expanded, parentId, ...).
const makeVisibleNode = (
  id: string,
  kind: VisibleNode["kind"],
  overrides: Partial<VisibleNode> = {},
): VisibleNode => ({
  id,
  kind,
  label: id,
  stats: null,
  transitive: null,
  own: null,
  ownSkew: null,
  transitiveSkew: null,
  overheadNs: null,
  childCount: 0,
  lir: [],
  address: null,
  operatorId: null,
  peers: [],
  direction: null,
  ...overrides,
});

describe("toElkGraph with an expanded region", () => {
  it("nests an expanded region's children under a sized container", () => {
    const region = makeVisibleNode(nodeIdOf([5, 1]), "region", {
      expanded: true,
    });
    const child = makeVisibleNode(nodeIdOf([5, 1, 1]), "operator", {
      parentId: nodeIdOf([5, 1]),
    });
    const elk = toElkGraph({ nodes: [region, child], edges: [] });
    const container = elk.children!.find((c) => c.id === nodeIdOf([5, 1]));
    expect(container).toBeDefined();
    // A container has children and no fixed width (elk auto-sizes it), unlike a
    // collapsed leaf which carries NODE_DIMENSIONS.
    expect(container!.children!.map((c) => c.id)).toEqual([
      nodeIdOf([5, 1, 1]),
    ]);
    expect(container!.width).toBeUndefined();
    expect(container!.layoutOptions).toBeDefined();
  });
});

// The fix for ViewportGuard reading elk's group-relative positions as if
// they were already absolute canvas coordinates (DataflowGraphView.tsx).
// Exercised directly here, rather than through DataflowGraphView or
// DataflowDetailPage, because the mocked useElkLayout those component tests
// rely on returns the same fixed {x: 0, y: 0, ...} box for every node id via
// a Proxy, so a group and its member always land on the same position
// regardless of whether the offset gets resolved, which can't discriminate
// this bug from a fix.
describe("resolveAbsolutePositions", () => {
  it("returns positions unchanged when there is no grouping", () => {
    const positions: Positions = {
      a: { x: 5, y: 5, width: 10, height: 10 },
    };
    expect(resolveAbsolutePositions(positions, undefined)).toBe(positions);
    expect(resolveAbsolutePositions(positions, new Map())).toBe(positions);
  });

  it("offsets a group member by its group's own position", () => {
    const positions: Positions = {
      group: { x: 100, y: 200, width: 50, height: 50 },
      member: { x: 8, y: 24, width: 10, height: 10 },
    };
    const parentOf = new Map([["member", "group"]]);
    const resolved = resolveAbsolutePositions(positions, parentOf);
    // The group itself has no parent, so its own position is already absolute.
    expect(resolved.group).toMatchObject({ x: 100, y: 200 });
    // The member's elk-reported position (8, 24) is relative to the group;
    // its absolute position is the group's origin plus that offset.
    expect(resolved.member).toMatchObject({ x: 108, y: 224 });
  });

  it("sums offsets across arbitrarily many levels of nesting", () => {
    const positions: Positions = {
      outer: { x: 100, y: 100, width: 300, height: 300 },
      inner: { x: 20, y: 30, width: 200, height: 200 },
      leaf: { x: 5, y: 7, width: 10, height: 10 },
    };
    const parentOf = new Map([
      ["inner", "outer"],
      ["leaf", "inner"],
    ]);
    const resolved = resolveAbsolutePositions(positions, parentOf);
    expect(resolved.outer).toMatchObject({ x: 100, y: 100 });
    // inner: outer's origin plus inner's own offset.
    expect(resolved.inner).toMatchObject({ x: 120, y: 130 });
    // leaf: inner's *absolute* origin (120, 130), not inner's raw
    // group-relative offset (20, 30), plus leaf's own offset. Summing
    // against the wrong (non-recursive) base would give (25, 37) instead
    // of (125, 137), so this is the case that would fail if the resolution
    // only walked up a single level.
    expect(resolved.leaf).toMatchObject({ x: 125, y: 137 });
  });

  it("leaves a node with no resolvable parent position undefined", () => {
    const positions: Positions = {
      lone: { x: 42, y: 43, width: 1, height: 1 },
      member: { x: 1, y: 1, width: 1, height: 1 },
    };
    const parentOf = new Map([["member", "group-not-in-positions"]]);
    const resolved = resolveAbsolutePositions(positions, parentOf);
    expect(resolved.lone).toMatchObject({ x: 42, y: 43 });
    // The member's parent has no known position (e.g. hidden or not yet
    // laid out), so its absolute position can't be resolved either, rather
    // than silently treating the unresolved offset as already absolute.
    expect(resolved.member).toBeUndefined();
  });
});
