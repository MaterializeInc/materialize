// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { ElkNode } from "elkjs/lib/elk-api";

import type { LirGrouping, VisibleGraph, VisibleNode } from "./dataflowGraph";

export interface NodePosition {
  x: number;
  y: number;
  width: number;
  height: number;
}

export type Positions = Record<string, NodePosition>;

export const NODE_DIMENSIONS: Record<
  VisibleNode["kind"],
  { width: number; height: number }
> = {
  operator: { width: 240, height: 72 },
  region: { width: 260, height: 96 },
  port: { width: 90, height: 24 },
};

const LAYOUT_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "DOWN",
  "elk.padding": "[top=48,left=16,bottom=16,right=16]",
  "elk.spacing.nodeNode": "24",
  "elk.layered.spacing.nodeNodeBetweenLayers": "48",
  // Lets an edge crossing a LIR group's boundary be declared uniformly at
  // the root: elk relocates it to the correct container itself. Harmless
  // when there's no grouping at all (today's flat views).
  "elk.hierarchyHandling": "INCLUDE_CHILDREN",
};

// A LIR group is a label-only wrapper, sized purely from its content (no
// fixed width/height), with top padding reserved for the header strip
// LirGroupNode renders.
const GROUP_LAYOUT_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "DOWN",
  "elk.padding": "[top=24,left=8,bottom=8,right=8]",
  "elk.spacing.nodeNode": "24",
  "elk.layered.spacing.nodeNodeBetweenLayers": "48",
};

// A view is always one scope's direct children, so without a grouping this
// is flat: nothing is nested inside anything else. With a grouping, a
// group's members (VisibleNodes or other groups) become its elk children;
// everything else stays a direct child of the root, exactly like today.
export function toElkGraph(
  graph: VisibleGraph,
  grouping?: LirGrouping,
): ElkNode {
  const nodeById = new Map(graph.nodes.map((n) => [n.id, n]));
  const groupById = new Map((grouping?.groups ?? []).map((g) => [g.id, g]));
  const childrenOf = new Map<string, string[]>(); // parent id ("" = root) -> ordered child ids
  const addChild = (parentKey: string, id: string) => {
    const list = childrenOf.get(parentKey);
    if (list) list.push(id);
    else childrenOf.set(parentKey, [id]);
  };
  for (const n of graph.nodes)
    addChild(grouping?.parentOf.get(n.id) ?? "", n.id);
  for (const g of groupById.values())
    addChild(grouping?.parentOf.get(g.id) ?? "", g.id);

  const buildNode = (id: string): ElkNode => {
    const group = groupById.get(id);
    if (group) {
      return {
        id,
        layoutOptions: GROUP_LAYOUT_OPTIONS,
        children: (childrenOf.get(id) ?? []).map(buildNode),
      };
    }
    const node = nodeById.get(id);
    if (!node) {
      throw new Error(`missing visible node for id ${id} building elk graph`);
    }
    return { id, ...NODE_DIMENSIONS[node.kind] };
  };

  return {
    id: "__root__",
    layoutOptions: LAYOUT_OPTIONS,
    children: (childrenOf.get("") ?? []).map(buildNode),
    edges: graph.edges.map((e) => ({
      id: e.id,
      sources: [e.source],
      targets: [e.target],
    })),
  };
}

// Recurses into nested children: elk reports every node's x/y already
// relative to its own immediate parent, the same convention React Flow uses
// for a node with parentId set, so no coordinate conversion happens here —
// this only flattens the tree into one lookup map, keyed by id at any depth.
export function extractPositions(layouted: ElkNode): Positions {
  const positions: Positions = {};
  const visit = (node: ElkNode) => {
    for (const child of node.children ?? []) {
      positions[child.id] = {
        x: child.x ?? 0,
        y: child.y ?? 0,
        width: child.width ?? 0,
        height: child.height ?? 0,
      };
      visit(child);
    }
  };
  visit(layouted);
  return positions;
}

// extractPositions above deliberately leaves nested positions parent-relative
// (elk's convention, matching React Flow's parentId semantics), which is
// exactly right for feeding React Flow but wrong for any consumer that
// compares positions against absolute canvas coordinates instead (e.g. a
// viewport-visibility check reading reactFlow.getViewport()). This resolves
// each node up its parent chain, summing offsets, before such a comparison.
// Nesting can go arbitrarily deep, so it recurses rather than assuming one
// level.
export function resolveAbsolutePositions(
  positions: Positions,
  parentOf: LirGrouping["parentOf"] | undefined,
): Positions {
  if (!parentOf || parentOf.size === 0) return positions;
  const resolved: Positions = {};
  const resolve = (id: string): NodePosition | undefined => {
    const cached = resolved[id];
    if (cached) return cached;
    const p = positions[id];
    if (!p) return undefined;
    const parentId = parentOf.get(id);
    if (!parentId) {
      resolved[id] = p;
      return p;
    }
    const parentPos = resolve(parentId);
    if (!parentPos) return undefined;
    const abs = { ...p, x: p.x + parentPos.x, y: p.y + parentPos.y };
    resolved[id] = abs;
    return abs;
  };
  for (const id of Object.keys(positions)) resolve(id);
  return resolved;
}
