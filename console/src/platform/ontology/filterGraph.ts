// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { OntologyEdge, OntologyEdgeKind, OntologyGraph } from "./model";

export interface GraphFilters {
  /** Groups to draw. Null draws every group. */
  groups: ReadonlySet<string> | null;
  hiddenEdgeKinds: ReadonlySet<OntologyEdgeKind>;
  showIsolated: boolean;
  /**
   * Draw only the entities within `depth` relationships of `nodeId`, in any
   * direction and across groups.
   */
  focus: { nodeId: string; depth: number } | null;
}

export interface FilteredGraph extends OntologyGraph {
  /** Entities in the drawn groups that no drawn relationship touches. */
  isolatedCount: number;
}

/** Applies the explorer's filters. Node and edge order is preserved. */
export function filterGraph(
  graph: OntologyGraph,
  filters: GraphFilters,
): FilteredGraph {
  const edges = graph.edges.filter(
    (edge) => !filters.hiddenEdgeKinds.has(edge.kind),
  );
  const nodeIds = new Set(graph.nodes.map((node) => node.id));

  let included: Set<string>;
  if (filters.focus && nodeIds.has(filters.focus.nodeId)) {
    included = neighborhood(edges, filters.focus.nodeId, filters.focus.depth);
  } else {
    included = new Set(
      graph.nodes
        .filter((node) => !filters.groups || filters.groups.has(node.group))
        .map((node) => node.id),
    );
  }

  const drawnEdges = edges.filter(
    (edge) => included.has(edge.source) && included.has(edge.target),
  );
  const connected = new Set<string>();
  for (const edge of drawnEdges) {
    connected.add(edge.source);
    connected.add(edge.target);
  }
  const isolatedCount = [...included].filter((id) => !connected.has(id)).length;
  const focusId = filters.focus?.nodeId;
  const nodes = graph.nodes.filter(
    (node) =>
      included.has(node.id) &&
      (filters.showIsolated || connected.has(node.id) || node.id === focusId),
  );
  const drawnIds = new Set(nodes.map((node) => node.id));
  const edgesById = new Map(graph.edges.map((edge) => [edge.id, edge]));
  const issues = graph.issues.filter((issue) => {
    if (!issue.edgeId) return true;
    const edge = edgesById.get(issue.edgeId);
    return !edge || (drawnIds.has(edge.source) && drawnIds.has(edge.target));
  });

  return { nodes, edges: drawnEdges, issues, isolatedCount };
}

/** Node ids reachable from `start` within `depth` hops, ignoring direction. */
export function neighborhood(
  edges: OntologyEdge[],
  start: string,
  depth: number,
): Set<string> {
  const adjacent = new Map<string, string[]>();
  for (const edge of edges) {
    adjacent.set(edge.source, [
      ...(adjacent.get(edge.source) ?? []),
      edge.target,
    ]);
    adjacent.set(edge.target, [
      ...(adjacent.get(edge.target) ?? []),
      edge.source,
    ]);
  }
  const seen = new Set([start]);
  let frontier = [start];
  for (let hop = 0; hop < depth && frontier.length > 0; hop++) {
    const next: string[] = [];
    for (const id of frontier) {
      for (const neighbor of adjacent.get(id) ?? []) {
        if (!seen.has(neighbor)) {
          seen.add(neighbor);
          next.push(neighbor);
        }
      }
    }
    frontier = next;
  }
  return seen;
}

/** Whether a node matches a search, by name, relation or description. */
export function matchesSearch(
  node: { label: string; relation: string | null; description: string | null },
  search: string,
) {
  const needle = search.trim().toLowerCase();
  if (needle === "") return false;
  return [node.label, node.relation, node.description].some((text) =>
    text?.toLowerCase().includes(needle),
  );
}
