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
  region: { width: 260, height: 96 },
  port: { width: 90, height: 24 },
};

const LAYOUT_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "DOWN",
  "elk.padding": "[top=48,left=16,bottom=16,right=16]",
  "elk.spacing.nodeNode": "24",
  "elk.layered.spacing.nodeNodeBetweenLayers": "48",
};

// A view is always one scope's direct children, so this is always a flat,
// single-level graph: nothing is ever nested inside anything else.
export function toElkGraph(graph: VisibleGraph): ElkNode {
  return {
    id: "__root__",
    layoutOptions: LAYOUT_OPTIONS,
    children: graph.nodes.map((n) => ({
      id: n.id,
      ...NODE_DIMENSIONS[n.kind],
    })),
    edges: graph.edges.map((e) => ({
      id: e.id,
      sources: [e.source],
      targets: [e.target],
    })),
  };
}

export function extractPositions(layouted: ElkNode): Positions {
  const positions: Positions = {};
  for (const child of layouted.children ?? []) {
    positions[child.id] = {
      x: child.x ?? 0,
      y: child.y ?? 0,
      width: child.width ?? 0,
      height: child.height ?? 0,
    };
  }
  return positions;
}
