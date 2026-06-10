// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import {
  clampEdgePoints,
  computeNodePositions,
  createDagreGraph,
  DagreGraphEdge,
  DagreGraphNode,
  getGraphDimensions,
  NodePosition,
  orderEdgesBySelection,
} from "~/components/Graph/dagreGraphHelpers";

interface UseDagreGraphOptions<TNode extends DagreGraphNode> {
  nodes: TNode[] | undefined;
  edges: DagreGraphEdge[] | undefined;
  selectedNodeId: string | undefined;
  ranksep: number;
  getNodeBottom?: (
    node: TNode,
    position: { left: number; top: number },
  ) => number;
}

/**
 * Hook for building and managing dagre graph layouts.
 * Provides computed positions, dimensions, and ordering for graph visualization.
 */
export function useDagreGraph<TNode extends DagreGraphNode>({
  nodes,
  edges,
  selectedNodeId,
  ranksep,
  getNodeBottom,
}: UseDagreGraphOptions<TNode>) {
  const graph = React.useMemo(
    () => createDagreGraph(nodes, edges, ranksep),
    [nodes, edges, ranksep],
  );

  const nodePositionMap = React.useMemo(
    () => computeNodePositions(graph, nodes, getNodeBottom),
    [graph, nodes, getNodeBottom],
  );

  const { height, width } = getGraphDimensions(graph);
  const selectedNode =
    graph && selectedNodeId ? graph.node(selectedNodeId) : null;
  const orderedGraphEdges = orderEdgesBySelection(graph, selectedNodeId);

  const clampPoints = (points: { x: number; y: number }[]) => {
    if (!width || !height) return points;
    return clampEdgePoints(points, width, height);
  };

  return {
    graph,
    nodePositionMap,
    height,
    width,
    selectedNode,
    orderedGraphEdges,
    clampPoints,
  };
}

export type { DagreGraphEdge, DagreGraphNode, NodePosition };
