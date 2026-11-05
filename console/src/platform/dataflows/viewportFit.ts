// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { NodeId } from "./dataflowGraph";

export interface ViewportFitState {
  scope: NodeId;
  // Sorted ids of the nodes laid out, so equal sets compare equal.
  nodeSet: string;
  expansion: string;
}

// Whether a newly landed layout deserves a fit. A new scope always does: elk
// lays each scope out independently, starting near its own origin, so the
// viewport left over from the previous scope is sized for a different graph.
// Within a scope, a changed node set does too (hiding unused paths can drop
// most of the graph, leaving the rest off-screen at the old zoom), except
// when the change comes from expanding or collapsing a region, which has its
// own targeted fit. A refresh that keeps the node set never refits, so
// panning away deliberately is not fought.
export const shouldRefit = (
  prev: ViewportFitState,
  next: ViewportFitState,
): boolean =>
  prev.scope !== next.scope ||
  (prev.nodeSet !== next.nodeSet && prev.expansion === next.expansion);

interface Box2D {
  x: number;
  y: number;
  width: number;
  height: number;
}

// Where to center the viewport so a node stays visible beside an overlay, or
// null when it already is. `node` is in flow coordinates, `viewport` is React
// Flow's transform, and `canvas` is the pane's pixel size. Zoom is never
// changed, so the result is a flow-space point for setCenter.
export const revealCenter = (
  node: Box2D,
  viewport: { x: number; y: number; zoom: number },
  canvas: { width: number; height: number },
  occludedRight: number,
): { x: number; y: number } | null => {
  const left = node.x * viewport.zoom + viewport.x;
  const top = node.y * viewport.zoom + viewport.y;
  const right = left + node.width * viewport.zoom;
  const bottom = top + node.height * viewport.zoom;
  const usableWidth = canvas.width - occludedRight;
  if (left >= 0 && top >= 0 && right <= usableWidth && bottom <= canvas.height)
    return null;
  // setCenter puts its point at the middle of the whole pane, so aim half the
  // overlay's width past the node to land it in the middle of the uncovered
  // part instead.
  return {
    x: node.x + node.width / 2 + occludedRight / 2 / viewport.zoom,
    y: node.y + node.height / 2,
  };
};
