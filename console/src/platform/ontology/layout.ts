// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import dagre from "@dagrejs/dagre";
import { curveBasis, line } from "d3";

import { edgeLabel, OntologyEdge, OntologyNode } from "./model";

export const NODE_WIDTH = 208;
export const NODE_HEIGHT = 60;
const GRAPH_MARGIN = 48;
/** Room above a group's first row for its name. */
const GROUP_LABEL_HEIGHT = 28;
const GROUP_PADDING = 16;
const LABEL_CHAR_WIDTH = 6.4;
const LABEL_HEIGHT = 18;

export type LayoutMode = "hierarchical" | "circular";

export interface PositionedNode {
  /** Center point. */
  x: number;
  y: number;
}

export interface PositionedGroup {
  name: string;
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface PositionedEdge {
  path: string;
  /** Where the edge's label is centered. */
  labelX: number;
  labelY: number;
}

export interface GraphLayout {
  width: number;
  height: number;
  nodes: Map<string, PositionedNode>;
  groups: PositionedGroup[];
  edges: Map<string, PositionedEdge>;
}

export const labelWidth = (text: string) =>
  Math.ceil(text.length * LABEL_CHAR_WIDTH) + 12;

const curvedPath = line<{ x: number; y: number }>()
  .x((p) => p.x)
  .y((p) => p.y)
  .curve(curveBasis);

const groupNodeId = (group: string) => `group|${group}`;

/**
 * Lays out the graph in ranks, with what is referenced above what references
 * it, and each group drawn as a box around its entities. Ranks run top to
 * bottom because the cards are wide: side by side ranks overflow a landscape
 * viewport well before stacked ones do.
 */
function hierarchicalLayout(
  nodes: OntologyNode[],
  edges: OntologyEdge[],
  showEdgeLabels: boolean,
): GraphLayout {
  const g = new dagre.graphlib.Graph({ compound: true, multigraph: true });
  g.setGraph({
    rankdir: "BT",
    nodesep: 32,
    ranksep: showEdgeLabels ? 96 : 72,
    edgesep: 16,
    marginx: GRAPH_MARGIN,
    marginy: GRAPH_MARGIN,
  });
  g.setDefaultEdgeLabel(() => ({}));

  const groups = [...new Set(nodes.map((node) => node.group))].sort();
  // A lone group gains nothing from a box, and its padding only shifts the
  // graph off center.
  const drawGroups = groups.length > 1;
  if (drawGroups) {
    for (const group of groups) {
      g.setNode(groupNodeId(group), { label: group });
    }
  }
  for (const node of nodes) {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
    if (drawGroups) g.setParent(node.id, groupNodeId(node.group));
  }
  for (const edge of edges) {
    const text = showEdgeLabels ? edgeLabel(edge) : "";
    g.setEdge(
      edge.source,
      edge.target,
      {
        width: text ? labelWidth(text) : 0,
        height: text ? LABEL_HEIGHT : 0,
        labelpos: "c",
      },
      edge.id,
    );
  }

  dagre.layout(g);

  const centers = new Map(
    nodes.map((node) => {
      const { x, y } = g.node(node.id);
      return [node.id, { x, y }];
    }),
  );
  const wrapped = wrapWideRanks(centers, edges, showEdgeLabels);
  if (wrapped) return wrapped;

  const positionedGroups: PositionedGroup[] = [];
  if (drawGroups) {
    for (const group of groups) {
      // Dagre sizes clusters without room for a label, so the box is drawn
      // from the members' bounds plus a label row.
      const members = nodes.filter((node) => node.group === group);
      const xs = members.map((node) => centers.get(node.id)?.x ?? 0);
      const ys = members.map((node) => centers.get(node.id)?.y ?? 0);
      const left = Math.min(...xs) - NODE_WIDTH / 2 - GROUP_PADDING;
      const right = Math.max(...xs) + NODE_WIDTH / 2 + GROUP_PADDING;
      const top =
        Math.min(...ys) - NODE_HEIGHT / 2 - GROUP_PADDING - GROUP_LABEL_HEIGHT;
      const bottom = Math.max(...ys) + NODE_HEIGHT / 2 + GROUP_PADDING;
      positionedGroups.push({
        name: group,
        x: left,
        y: top,
        width: right - left,
        height: bottom - top,
      });
    }
  }

  const rawEdges = edges.flatMap((edge) => {
    const label = g.edge({ v: edge.source, w: edge.target, name: edge.id });
    const points: { x: number; y: number }[] = label?.points ?? [];
    if (points.length < 2) return [];
    const middle = points[Math.floor(points.length / 2)];
    const text = showEdgeLabels ? edgeLabel(edge) : "";
    return [
      {
        id: edge.id,
        points,
        label: { x: label.x ?? middle.x, y: label.y ?? middle.y },
        labelHalfWidth: text ? labelWidth(text) / 2 : 0,
      },
    ];
  });

  // Dagre's reported size counts its cluster nodes, which are not drawn, and
  // not the group label rows, which are. Bound what is drawn instead.
  const xs: number[] = [];
  const ys: number[] = [];
  for (const { x, y } of centers.values()) {
    xs.push(x - NODE_WIDTH / 2, x + NODE_WIDTH / 2);
    ys.push(y - NODE_HEIGHT / 2, y + NODE_HEIGHT / 2);
  }
  for (const group of positionedGroups) {
    xs.push(group.x, group.x + group.width);
    ys.push(group.y, group.y + group.height);
  }
  for (const edge of rawEdges) {
    for (const point of edge.points) {
      xs.push(point.x);
      ys.push(point.y);
    }
    xs.push(
      edge.label.x - edge.labelHalfWidth,
      edge.label.x + edge.labelHalfWidth,
    );
  }
  const dx = GRAPH_MARGIN - Math.min(...xs);
  const dy = GRAPH_MARGIN - Math.min(...ys);
  const shift = (p: { x: number; y: number }) => ({ x: p.x + dx, y: p.y + dy });

  const positioned = new Map<string, PositionedNode>();
  for (const [id, center] of centers) positioned.set(id, shift(center));
  const positionedEdges = new Map<string, PositionedEdge>();
  for (const edge of rawEdges) {
    const label = shift(edge.label);
    positionedEdges.set(edge.id, {
      path: curvedPath(edge.points.map(shift)) ?? "",
      labelX: label.x,
      labelY: label.y,
    });
  }
  return {
    width: Math.max(...xs) + dx + GRAPH_MARGIN,
    height: Math.max(...ys) + dy + GRAPH_MARGIN,
    nodes: positioned,
    groups: positionedGroups.map((group) => ({ ...group, ...shift(group) })),
    edges: positionedEdges,
  };
}

const COLUMN_GAP = 32;
const ROW_GAP = 36;

/**
 * Cards per row once ranks wrap, chosen so a wrapped graph comes out about as
 * wide as it is tall in screen terms. Below the minimum, dagre's own layout
 * is kept.
 */
export function maxCardsPerRow(nodeCount: number) {
  return Math.max(6, Math.ceil(Math.sqrt(0.6 * nodeCount)));
}

/**
 * Re-flows a dagre layout whose ranks are wider than `maxCardsPerRow`, which
 * happens when many entities reference one hub, such as every catalog object
 * referencing its owner role. Each rank keeps dagre's order but wraps into
 * centered rows, and edges are redrawn as curves between card borders since
 * dagre's routes no longer apply. Group boxes are dropped, because wrapped
 * groups are not contiguous. Returns null when no rank needs wrapping.
 */
function wrapWideRanks(
  centers: Map<string, { x: number; y: number }>,
  edges: OntologyEdge[],
  showEdgeLabels: boolean,
): GraphLayout | null {
  const perRow = maxCardsPerRow(centers.size);
  const ranks = new Map<number, string[]>();
  for (const [id, { y }] of centers) {
    const key = Math.round(y);
    ranks.set(key, [...(ranks.get(key) ?? []), id]);
  }
  if ([...ranks.values()].every((rank) => rank.length <= perRow)) return null;

  const rankGap = showEdgeLabels ? 96 : 72;
  const rowWidth = perRow * NODE_WIDTH + (perRow - 1) * COLUMN_GAP;
  const width = rowWidth + 2 * GRAPH_MARGIN;
  const positioned = new Map<string, PositionedNode>();
  let y = GRAPH_MARGIN + NODE_HEIGHT / 2;
  const orderedRanks = [...ranks.entries()].sort((a, b) => a[0] - b[0]);
  orderedRanks.forEach(([, ids], rankIndex) => {
    if (rankIndex > 0) y += rankGap;
    const ordered = [...ids].sort(
      (a, b) => (centers.get(a)?.x ?? 0) - (centers.get(b)?.x ?? 0),
    );
    for (let start = 0; start < ordered.length; start += perRow) {
      if (start > 0) y += NODE_HEIGHT + ROW_GAP;
      const row = ordered.slice(start, start + perRow);
      const used = row.length * NODE_WIDTH + (row.length - 1) * COLUMN_GAP;
      let x = (width - used) / 2 + NODE_WIDTH / 2;
      for (const id of row) {
        positioned.set(id, { x, y });
        x += NODE_WIDTH + COLUMN_GAP;
      }
    }
    y += NODE_HEIGHT;
  });

  const positionedEdges = new Map<string, PositionedEdge>();
  for (const edge of edges) {
    const from = positioned.get(edge.source);
    const to = positioned.get(edge.target);
    if (!from || !to) continue;
    const goesUp = to.y < from.y - NODE_HEIGHT / 2;
    const goesDown = to.y > from.y + NODE_HEIGHT / 2;
    let p0: { x: number; y: number };
    let p3: { x: number; y: number };
    let p1: { x: number; y: number };
    let p2: { x: number; y: number };
    if (goesUp || goesDown) {
      const sign = goesUp ? -1 : 1;
      p0 = { x: from.x, y: from.y + (sign * NODE_HEIGHT) / 2 };
      p3 = { x: to.x, y: to.y - (sign * NODE_HEIGHT) / 2 };
      const bend = Math.max(40, Math.abs(p3.y - p0.y) / 2);
      p1 = { x: p0.x, y: p0.y + sign * bend };
      p2 = { x: p3.x, y: p3.y - sign * bend };
    } else {
      // Same row: arc over the cards in between.
      p0 = { x: from.x, y: from.y - NODE_HEIGHT / 2 };
      p3 = { x: to.x, y: to.y - NODE_HEIGHT / 2 };
      const lift = Math.min(120, 30 + Math.abs(p3.x - p0.x) / 6);
      p1 = { x: p0.x, y: p0.y - lift };
      p2 = { x: p3.x, y: p3.y - lift };
    }
    positionedEdges.set(edge.id, {
      path: `M${p0.x},${p0.y} C${p1.x},${p1.y} ${p2.x},${p2.y} ${p3.x},${p3.y}`,
      // The midpoint of a cubic Bézier curve.
      labelX: (p0.x + 3 * p1.x + 3 * p2.x + p3.x) / 8,
      labelY: (p0.y + 3 * p1.y + 3 * p2.y + p3.y) / 8,
    });
  }
  return {
    width,
    // `y` ends one card height below the last row's center.
    height: y - NODE_HEIGHT / 2 + GRAPH_MARGIN,
    nodes: positioned,
    groups: [],
    edges: positionedEdges,
  };
}

/** Where the line from a card's center toward `toward` crosses its border. */
export function boundaryPoint(
  center: { x: number; y: number },
  toward: { x: number; y: number },
) {
  const dx = toward.x - center.x;
  const dy = toward.y - center.y;
  if (dx === 0 && dy === 0) return center;
  const scale = Math.min(
    dx === 0 ? Infinity : NODE_WIDTH / 2 / Math.abs(dx),
    dy === 0 ? Infinity : NODE_HEIGHT / 2 / Math.abs(dy),
  );
  return { x: center.x + dx * scale, y: center.y + dy * scale };
}

/**
 * Places every entity on one circle, ordered by group so each group occupies
 * an arc, with relationships bowing toward the center.
 */
function circularLayout(
  nodes: OntologyNode[],
  edges: OntologyEdge[],
): GraphLayout {
  const ordered = [...nodes].sort(
    (a, b) => a.group.localeCompare(b.group) || a.label.localeCompare(b.label),
  );
  // Cards sit side by side near the top and bottom of the circle and stacked
  // near its sides, so space them by the mean of their width and height.
  const spacing = (NODE_WIDTH + NODE_HEIGHT) / 2 + 24;
  const radius = Math.max(220, (ordered.length * spacing) / (2 * Math.PI));
  const cx = radius + NODE_WIDTH / 2 + GRAPH_MARGIN;
  const cy = radius + NODE_HEIGHT / 2 + GRAPH_MARGIN;
  const positioned = new Map<string, PositionedNode>();
  ordered.forEach((node, i) => {
    const angle = (2 * Math.PI * i) / ordered.length - Math.PI / 2;
    positioned.set(node.id, {
      x: cx + radius * Math.cos(angle),
      y: cy + radius * Math.sin(angle),
    });
  });
  const positionedEdges = new Map<string, PositionedEdge>();
  for (const edge of edges) {
    const from = positioned.get(edge.source);
    const to = positioned.get(edge.target);
    if (!from || !to) continue;
    if (edge.source === edge.target) {
      const loop = `M${from.x},${from.y - NODE_HEIGHT / 2} c-40,-60 40,-60 0,0`;
      positionedEdges.set(edge.id, {
        path: loop,
        labelX: from.x,
        labelY: from.y - NODE_HEIGHT / 2 - 48,
      });
      continue;
    }
    const control = {
      x: (from.x + to.x) / 2 + (cx - (from.x + to.x) / 2) * 0.6,
      y: (from.y + to.y) / 2 + (cy - (from.y + to.y) / 2) * 0.6,
    };
    // End the curve at the card borders, so the arrowhead is not hidden
    // under the card it points at.
    const start = boundaryPoint(from, control);
    const end = boundaryPoint(to, control);
    positionedEdges.set(edge.id, {
      path: `M${start.x},${start.y} Q${control.x},${control.y} ${end.x},${end.y}`,
      // The midpoint of a quadratic Bézier curve.
      labelX: 0.25 * start.x + 0.5 * control.x + 0.25 * end.x,
      labelY: 0.25 * start.y + 0.5 * control.y + 0.25 * end.y,
    });
  }
  return {
    width: 2 * cx,
    height: 2 * cy,
    nodes: positioned,
    groups: [],
    edges: positionedEdges,
  };
}

export function layoutGraph(
  nodes: OntologyNode[],
  edges: OntologyEdge[],
  { mode, showEdgeLabels }: { mode: LayoutMode; showEdgeLabels: boolean },
): GraphLayout {
  if (nodes.length === 0) {
    return {
      width: 0,
      height: 0,
      nodes: new Map(),
      groups: [],
      edges: new Map(),
    };
  }
  return mode === "circular"
    ? circularLayout(nodes, edges)
    : hierarchicalLayout(nodes, edges, showEdgeLabels);
}
