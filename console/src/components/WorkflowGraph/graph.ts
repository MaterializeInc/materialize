// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import dagre from "@dagrejs/dagre";

import { assert } from "~/util";

export type GraphlibRelations = (id: string) => string[] | undefined;

/**
 * Given a starting node and a function for finding connected nodes, walks the graph and
 * finds all connected nodes.
 */
function walkGraph(startingId: string, walkFn: GraphlibRelations) {
  const results = new Set<string>();
  const visited = new Set<string>();
  const toVisit = [startingId];
  // We iterate until we don't find any more nodes, then return
  while (toVisit.length > 0) {
    const next = toVisit.pop();
    // we know this will be defined because of the length check in the while condition
    assert(next);
    // because there might be mutliple edges that lead to the same node, we guard against
    // walking the same node more than once.
    if (visited.has(next)) continue;
    visited.add(next);

    const found = walkFn(next);
    if (!found) continue;

    for (const id of found) {
      toVisit.push(id);
      results.add(id);
    }
  }
  return Array.from(results.values());
}

export const getUpstreamNodes = (id: string, graph: dagre.graphlib.Graph) =>
  walkGraph(id, graph.predecessors.bind(graph) as unknown as GraphlibRelations);

export const getDownstreamNodes = (id: string, graph: dagre.graphlib.Graph) =>
  walkGraph(id, graph.successors.bind(graph) as unknown as GraphlibRelations);
