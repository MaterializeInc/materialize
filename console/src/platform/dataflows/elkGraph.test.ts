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
} from "./dataflowGraph";
import { CHANNELS, LIR_SPANS, OPS } from "./dataflowGraph.test";
import { extractPositions, NODE_DIMENSIONS, toElkGraph } from "./elkGraph";

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
