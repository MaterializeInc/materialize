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

import {
  cardHeight,
  COMPACT_CARD_HEIGHT,
  KeyColumn,
  keyRowOffset,
  pairsColumns,
} from "./keyColumns";
import { edgeLabel, OntologyEdge, OntologyNode } from "./model";

export const NODE_WIDTH = 224;
const GRAPH_MARGIN = 48;
/** Room above a group's first row for its name. */
const GROUP_LABEL_HEIGHT = 28;
const GROUP_PADDING = 16;
const LABEL_CHAR_WIDTH = 6.4;
const LABEL_HEIGHT = 18;

export type LayoutMode = "hierarchical" | "circular";

export interface LayoutOptions {
  mode: LayoutMode;
  showEdgeLabels: boolean;
  /**
   * Key columns by entity, to draw on the cards, or null for compact cards.
   * With them, relationships that pair columns run from row to row.
   */
  keyColumns: Map<string, KeyColumn[]> | null;
}

export interface PositionedNode {
  /** Center point. */
  x: number;
  y: number;
  height: number;
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
  /** Runs between key column rows, which already say what it joins. */
  anchored: boolean;
}

export interface GraphLayout {
  width: number;
  height: number;
  nodes: Map<string, PositionedNode>;
  groups: PositionedGroup[];
  edges: Map<string, PositionedEdge>;
}

type Point = { x: number; y: number };

export const labelWidth = (text: string) =>
  Math.ceil(text.length * LABEL_CHAR_WIDTH) + 12;

const curvedPath = line<Point>()
  .x((p) => p.x)
  .y((p) => p.y)
  .curve(curveBasis);

const groupNodeId = (group: string) => `group|${group}`;

/** The midpoint of a cubic Bézier curve. */
const cubicMidpoint = (p0: Point, p1: Point, p2: Point, p3: Point) => ({
  x: (p0.x + 3 * p1.x + 3 * p2.x + p3.x) / 8,
  y: (p0.y + 3 * p1.y + 3 * p2.y + p3.y) / 8,
});

const cubicPath = (p0: Point, p1: Point, p2: Point, p3: Point) =>
  `M${p0.x},${p0.y} C${p1.x},${p1.y} ${p2.x},${p2.y} ${p3.x},${p3.y}`;

/**
 * Lays out the graph in ranks, with what is referenced above what references
 * it, and each group drawn as a box around its entities. Ranks run top to
 * bottom because the cards are wide: side by side ranks overflow a landscape
 * viewport well before stacked ones do.
 */
function hierarchicalLayout(
  nodes: OntologyNode[],
  edges: OntologyEdge[],
  heights: Map<string, number>,
  { showEdgeLabels, keyColumns }: LayoutOptions,
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
    g.setNode(node.id, {
      width: NODE_WIDTH,
      height: heights.get(node.id) ?? COMPACT_CARD_HEIGHT,
    });
    if (drawGroups) g.setParent(node.id, groupNodeId(node.group));
  }
  for (const edge of edges) {
    const text = labelText(edge, showEdgeLabels, keyColumns);
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
  const wrapped = wrapWideRanks(centers, heights, edges, showEdgeLabels);
  if (wrapped) return wrapped;

  const height = (id: string) => heights.get(id) ?? COMPACT_CARD_HEIGHT;
  const positionedGroups: PositionedGroup[] = [];
  if (drawGroups) {
    for (const group of groups) {
      // Dagre sizes clusters without room for a label, so the box is drawn
      // from the members' bounds plus a label row.
      const members = nodes.filter((node) => node.group === group);
      const lefts = members.map(
        (n) => (centers.get(n.id)?.x ?? 0) - NODE_WIDTH / 2,
      );
      const rights = members.map(
        (n) => (centers.get(n.id)?.x ?? 0) + NODE_WIDTH / 2,
      );
      const tops = members.map(
        (n) => (centers.get(n.id)?.y ?? 0) - height(n.id) / 2,
      );
      const bottoms = members.map(
        (n) => (centers.get(n.id)?.y ?? 0) + height(n.id) / 2,
      );
      const left = Math.min(...lefts) - GROUP_PADDING;
      const right = Math.max(...rights) + GROUP_PADDING;
      const top = Math.min(...tops) - GROUP_PADDING - GROUP_LABEL_HEIGHT;
      const bottom = Math.max(...bottoms) + GROUP_PADDING;
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
    const points: Point[] = label?.points ?? [];
    if (points.length < 2) return [];
    const middle = points[Math.floor(points.length / 2)];
    const text = labelText(edge, showEdgeLabels, keyColumns);
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
  for (const [id, { x, y }] of centers) {
    xs.push(x - NODE_WIDTH / 2, x + NODE_WIDTH / 2);
    ys.push(y - height(id) / 2, y + height(id) / 2);
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
  const shift = (p: Point) => ({ x: p.x + dx, y: p.y + dy });

  const positioned = new Map<string, PositionedNode>();
  for (const [id, center] of centers) {
    positioned.set(id, { ...shift(center), height: height(id) });
  }
  const positionedEdges = new Map<string, PositionedEdge>();
  for (const edge of rawEdges) {
    const label = shift(edge.label);
    positionedEdges.set(edge.id, {
      path: curvedPath(edge.points.map(shift)) ?? "",
      labelX: label.x,
      labelY: label.y,
      anchored: false,
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

/** The label dagre makes room for. Row-to-row joins are drawn without one. */
function labelText(
  edge: OntologyEdge,
  showEdgeLabels: boolean,
  keyColumns: Map<string, KeyColumn[]> | null,
) {
  if (!showEdgeLabels || (keyColumns && pairsColumns(edge))) return "";
  return edgeLabel(edge);
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
  centers: Map<string, Point>,
  heights: Map<string, number>,
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

  const height = (id: string) => heights.get(id) ?? COMPACT_CARD_HEIGHT;
  const rankGap = showEdgeLabels ? 96 : 72;
  const rowWidth = perRow * NODE_WIDTH + (perRow - 1) * COLUMN_GAP;
  const width = rowWidth + 2 * GRAPH_MARGIN;
  const positioned = new Map<string, PositionedNode>();
  // The top of the next row of cards.
  let top = GRAPH_MARGIN;
  const orderedRanks = [...ranks.entries()].sort((a, b) => a[0] - b[0]);
  orderedRanks.forEach(([, ids], rankIndex) => {
    if (rankIndex > 0) top += rankGap - ROW_GAP;
    const ordered = [...ids].sort(
      (a, b) => (centers.get(a)?.x ?? 0) - (centers.get(b)?.x ?? 0),
    );
    for (let start = 0; start < ordered.length; start += perRow) {
      const row = ordered.slice(start, start + perRow);
      const rowHeight = Math.max(...row.map(height));
      const used = row.length * NODE_WIDTH + (row.length - 1) * COLUMN_GAP;
      let x = (width - used) / 2 + NODE_WIDTH / 2;
      for (const id of row) {
        // Cards in a row share a top edge, so their headers line up.
        positioned.set(id, { x, y: top + height(id) / 2, height: height(id) });
        x += NODE_WIDTH + COLUMN_GAP;
      }
      top += rowHeight + ROW_GAP;
    }
  });

  const positionedEdges = new Map<string, PositionedEdge>();
  for (const edge of edges) {
    const from = positioned.get(edge.source);
    const to = positioned.get(edge.target);
    if (!from || !to) continue;
    const goesUp = to.y + to.height / 2 < from.y - from.height / 2;
    const goesDown = to.y - to.height / 2 > from.y + from.height / 2;
    let p0: Point;
    let p1: Point;
    let p2: Point;
    let p3: Point;
    if (goesUp || goesDown) {
      const sign = goesUp ? -1 : 1;
      p0 = { x: from.x, y: from.y + (sign * from.height) / 2 };
      p3 = { x: to.x, y: to.y - (sign * to.height) / 2 };
      const bend = Math.max(40, Math.abs(p3.y - p0.y) / 2);
      p1 = { x: p0.x, y: p0.y + sign * bend };
      p2 = { x: p3.x, y: p3.y - sign * bend };
    } else {
      // Same row: arc over the cards in between.
      p0 = { x: from.x, y: from.y - from.height / 2 };
      p3 = { x: to.x, y: to.y - to.height / 2 };
      const lift = Math.min(120, 30 + Math.abs(p3.x - p0.x) / 6);
      p1 = { x: p0.x, y: p0.y - lift };
      p2 = { x: p3.x, y: p3.y - lift };
    }
    const label = cubicMidpoint(p0, p1, p2, p3);
    positionedEdges.set(edge.id, {
      path: cubicPath(p0, p1, p2, p3),
      labelX: label.x,
      labelY: label.y,
      anchored: false,
    });
  }
  return {
    width,
    height: top - ROW_GAP + GRAPH_MARGIN,
    nodes: positioned,
    groups: [],
    edges: positionedEdges,
  };
}

/** Where the line from a card's center toward `toward` crosses its border. */
export function boundaryPoint(
  center: Point,
  toward: Point,
  height = COMPACT_CARD_HEIGHT,
) {
  const dx = toward.x - center.x;
  const dy = toward.y - center.y;
  if (dx === 0 && dy === 0) return center;
  const scale = Math.min(
    dx === 0 ? Infinity : NODE_WIDTH / 2 / Math.abs(dx),
    dy === 0 ? Infinity : height / 2 / Math.abs(dy),
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
  heights: Map<string, number>,
): GraphLayout {
  const ordered = [...nodes].sort(
    (a, b) => a.group.localeCompare(b.group) || a.label.localeCompare(b.label),
  );
  const height = (id: string) => heights.get(id) ?? COMPACT_CARD_HEIGHT;
  const tallest = Math.max(...ordered.map((node) => height(node.id)));
  // Cards sit side by side near the top and bottom of the circle and stacked
  // near its sides, so space them by the mean of their width and height.
  const spacing = (NODE_WIDTH + tallest) / 2 + 24;
  const radius = Math.max(220, (ordered.length * spacing) / (2 * Math.PI));
  const cx = radius + NODE_WIDTH / 2 + GRAPH_MARGIN;
  const cy = radius + tallest / 2 + GRAPH_MARGIN;
  const positioned = new Map<string, PositionedNode>();
  ordered.forEach((node, i) => {
    const angle = (2 * Math.PI * i) / ordered.length - Math.PI / 2;
    positioned.set(node.id, {
      x: cx + radius * Math.cos(angle),
      y: cy + radius * Math.sin(angle),
      height: height(node.id),
    });
  });
  const positionedEdges = new Map<string, PositionedEdge>();
  for (const edge of edges) {
    const from = positioned.get(edge.source);
    const to = positioned.get(edge.target);
    if (!from || !to) continue;
    if (edge.source === edge.target) {
      const top = from.y - from.height / 2;
      positionedEdges.set(edge.id, {
        path: `M${from.x},${top} c-40,-60 40,-60 0,0`,
        labelX: from.x,
        labelY: top - 48,
        anchored: false,
      });
      continue;
    }
    const control = {
      x: (from.x + to.x) / 2 + (cx - (from.x + to.x) / 2) * 0.6,
      y: (from.y + to.y) / 2 + (cy - (from.y + to.y) / 2) * 0.6,
    };
    // End the curve at the card borders, so the arrowhead is not hidden
    // under the card it points at.
    const start = boundaryPoint(from, control, from.height);
    const end = boundaryPoint(to, control, to.height);
    positionedEdges.set(edge.id, {
      path: `M${start.x},${start.y} Q${control.x},${control.y} ${end.x},${end.y}`,
      // The midpoint of a quadratic Bézier curve.
      labelX: 0.25 * start.x + 0.5 * control.x + 0.25 * end.x,
      labelY: 0.25 * start.y + 0.5 * control.y + 0.25 * end.y,
      anchored: false,
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

/** How far a row-to-row edge between vertically stacked cards bows out. */
const SIDE_LOOP = 40;

/**
 * Redraws each relationship that pairs columns from its first column's row on
 * one card to the referenced row on the other, leaving the side of each card
 * that faces the other. Cards stacked above each other have no facing sides,
 * so the edge leaves and enters on their right and loops outside them.
 * Returns how far right the loops reach, which the canvas must include.
 */
function anchorEdges(
  layout: GraphLayout,
  edges: OntologyEdge[],
  keyColumns: Map<string, KeyColumn[]>,
) {
  let rightmost = 0;
  for (const edge of edges) {
    if (!pairsColumns(edge)) continue;
    const from = layout.nodes.get(edge.source);
    const to = layout.nodes.get(edge.target);
    if (!from || !to) continue;
    const rowY = (node: PositionedNode, id: string, column: string) =>
      node.y - node.height / 2 + keyRowOffset(keyColumns.get(id), column);
    const fromY = rowY(from, edge.source, edge.sourceColumns[0]);
    const toY = rowY(to, edge.target, edge.targetColumns[0]);
    const half = NODE_WIDTH / 2;
    let p0: Point;
    let p1: Point;
    let p2: Point;
    let p3: Point;
    if (Math.abs(to.x - from.x) > NODE_WIDTH * 0.75) {
      const dir = to.x > from.x ? 1 : -1;
      p0 = { x: from.x + dir * half, y: fromY };
      p3 = { x: to.x - dir * half, y: toY };
      const reach = Math.max(40, Math.abs(p3.x - p0.x) / 2);
      p1 = { x: p0.x + dir * reach, y: p0.y };
      p2 = { x: p3.x - dir * reach, y: p3.y };
    } else {
      p0 = { x: from.x + half, y: fromY };
      p3 = { x: to.x + half, y: toY };
      const loopX =
        Math.max(p0.x, p3.x) +
        SIDE_LOOP +
        Math.min(120, Math.abs(p3.y - p0.y) * 0.15);
      p1 = { x: loopX, y: p0.y };
      p2 = { x: loopX, y: p3.y };
      rightmost = Math.max(rightmost, loopX);
    }
    const label = cubicMidpoint(p0, p1, p2, p3);
    layout.edges.set(edge.id, {
      path: cubicPath(p0, p1, p2, p3),
      labelX: label.x,
      labelY: label.y,
      anchored: true,
    });
  }
  return rightmost;
}

export function layoutGraph(
  nodes: OntologyNode[],
  edges: OntologyEdge[],
  options: LayoutOptions,
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
  const { keyColumns } = options;
  const heights = new Map(
    nodes.map((node) => [
      node.id,
      keyColumns ? cardHeight(keyColumns.get(node.id)) : COMPACT_CARD_HEIGHT,
    ]),
  );
  const layout =
    options.mode === "circular"
      ? circularLayout(nodes, edges, heights)
      : hierarchicalLayout(nodes, edges, heights, options);
  if (keyColumns) {
    const rightmost = anchorEdges(layout, edges, keyColumns);
    layout.width = Math.max(layout.width, rightmost + GRAPH_MARGIN);
  }
  return layout;
}
