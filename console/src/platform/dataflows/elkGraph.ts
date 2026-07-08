// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { ElkNode } from "elkjs/lib/elk-api";

import type { VisibleGraph, VisibleNode } from "./dataflowGraph";

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
  collapsedRegion: { width: 260, height: 88 },
  region: { width: 0, height: 0 },
  port: { width: 90, height: 24 },
};

const LAYOUT_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "DOWN",
  "elk.hierarchyHandling": "INCLUDE_CHILDREN",
  "elk.padding": "[top=48,left=16,bottom=16,right=16]",
  "elk.spacing.nodeNode": "24",
  "elk.layered.spacing.nodeNodeBetweenLayers": "48",
};

export function toElkGraph(graph: VisibleGraph): ElkNode {
  const elkNodes = new Map<string, ElkNode>();
  const root: ElkNode = {
    id: "__root__",
    layoutOptions: LAYOUT_OPTIONS,
    children: [],
    edges: [],
  };
  for (const node of graph.nodes) {
    const dims = NODE_DIMENSIONS[node.kind];
    const elkNode: ElkNode = {
      id: node.id,
      children: [],
      ...(node.kind === "region" ? { layoutOptions: LAYOUT_OPTIONS } : dims),
    };
    elkNodes.set(node.id, elkNode);
    const parent = node.parent ? elkNodes.get(node.parent)! : root;
    parent.children!.push(elkNode);
  }
  root.edges = graph.edges.map((e) => ({
    id: e.id,
    sources: [e.source],
    targets: [e.target],
  }));
  return root;
}

export function extractPositions(layouted: ElkNode): Positions {
  const positions: Positions = {};
  const walk = (node: ElkNode) => {
    for (const child of node.children ?? []) {
      positions[child.id] = {
        x: child.x ?? 0,
        y: child.y ?? 0,
        width: child.width ?? 0,
        height: child.height ?? 0,
      };
      walk(child);
    }
  };
  walk(layouted);
  return positions;
}
