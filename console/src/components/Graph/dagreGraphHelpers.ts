// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import dagre from "@dagrejs/dagre";

import { STROKE_WIDTH } from "~/components/Graph";
import { clamp } from "~/util";

export const MIN_EDGE_MARGIN = STROKE_WIDTH / 2;

export interface DagreGraphNode {
  id: string;
  width: number;
  height: number;
}

export interface DagreGraphEdge {
  parentId?: string | null;
  childId?: string | null;
  parentName?: string | null;
  childName?: string | null;
}

export interface NodePosition {
  top: number;
  right: number;
  left: number;
  bottom: number;
}

/**
 * Creates and configures a dagre graph with nodes and edges
 */
export function createDagreGraph<TNode extends DagreGraphNode>(
  nodes: TNode[] | undefined,
  edges: DagreGraphEdge[] | undefined,
  ranksep: number,
): dagre.graphlib.Graph {
  const g = new dagre.graphlib.Graph();
  g.setGraph({ ranksep });
  g.setDefaultEdgeLabel(() => ({}));

  for (const node of nodes ?? []) {
    g.setNode(node.id, {
      width: node.width,
      height: node.height,
    });
  }

  for (const edge of edges ?? []) {
    const parent = edge.parentId ?? edge.parentName;
    const child = edge.childId ?? edge.childName;
    if (parent && child) {
      g.setEdge(parent, child);
    }
  }

  dagre.layout(g);

  return g;
}

/**
 * Computes position map for all nodes in the graph
 */
export function computeNodePositions<TNode extends DagreGraphNode>(
  graph: dagre.graphlib.Graph | null,
  nodes: TNode[] | undefined,
  getNodeBottom?: (
    node: TNode,
    position: { left: number; top: number },
  ) => number,
): Map<string, NodePosition> {
  const map = new Map<string, NodePosition>();
  if (!graph || !nodes) return map;

  for (const node of nodes) {
    const dagreNode = graph.node(node.id);
    if (!dagreNode) continue;

    const left = dagreNode.x - dagreNode.width / 2;
    const right = left + dagreNode.width;
    const top = dagreNode.y - dagreNode.height / 2;
    const bottom = getNodeBottom
      ? getNodeBottom(node, { left, top })
      : top + dagreNode.height;

    map.set(node.id, { top, right, left, bottom });
  }
  return map;
}

/**
 * Gets graph dimensions (height and width)
 */
export function getGraphDimensions(graph: dagre.graphlib.Graph | null): {
  height: number | undefined;
  width: number | undefined;
} {
  if (!graph) return { height: undefined, width: undefined };
  const graphInfo = graph.graph();
  return { height: graphInfo.height, width: graphInfo.width };
}

/**
 * Orders graph edges so edges adjacent to the selected node render on top
 */
export function orderEdgesBySelection(
  graph: dagre.graphlib.Graph | null,
  selectedNodeId: string | undefined,
): dagre.Edge[] {
  const graphEdges = graph?.edges() ?? [];

  const edgesAdjacentToSelected = graphEdges.filter(
    (edge) => edge.v === selectedNodeId || edge.w === selectedNodeId,
  );

  const otherEdges = graphEdges.filter(
    (edge) => !(edge.v === selectedNodeId || edge.w === selectedNodeId),
  );

  return [...otherEdges, ...edgesAdjacentToSelected];
}

/**
 * Clamps edge points to stay within graph bounds
 * Dagre has a bug where point values can go outside the graph bounds
 * https://github.com/dagrejs/dagre/issues/291
 */
export function clampEdgePoints(
  points: { x: number; y: number }[],
  width: number,
  height: number,
): { x: number; y: number }[] {
  return points.map((p) => ({
    x: clamp(p.x, MIN_EDGE_MARGIN, width - MIN_EDGE_MARGIN),
    y: clamp(p.y, MIN_EDGE_MARGIN, height - MIN_EDGE_MARGIN),
  }));
}

/**
 * Creates a getNodeBottom function for workflow graph nodes.
 * Workflow graph nodes have variable heights based on whether they have lag or source status.
 */
export function createWorkflowNodeBottomFn(
  lagMap: Map<string, unknown> | undefined,
  nodeMap: Map<string, { sourceStatus?: unknown }>,
  baseHeight: number,
  heightWithStatus: number,
): (node: DagreGraphNode, position: { left: number; top: number }) => number {
  return (node, position) => {
    const hasStatus =
      Boolean(lagMap?.get(node.id)) ||
      Boolean(nodeMap.get(node.id)?.sourceStatus);
    return hasStatus
      ? position.top + heightWithStatus
      : position.top + baseHeight;
  };
}
