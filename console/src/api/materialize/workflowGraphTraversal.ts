// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  workflowGraphEdgeKey,
  WorkflowGraphEdgeRow,
} from "./workflowGraphEdges";

function index(
  edges: WorkflowGraphEdgeRow[],
  keyOf: (e: WorkflowGraphEdgeRow) => string,
): Map<string, WorkflowGraphEdgeRow[]> {
  const map = new Map<string, WorkflowGraphEdgeRow[]>();
  for (const e of edges) {
    const bucket = map.get(keyOf(e));
    if (bucket) {
      bucket.push(e);
    } else {
      map.set(keyOf(e), [e]);
    }
  }
  return map;
}

/**
 * Breadth-first walk from `root` collecting every reachable edge. `adjacency`
 * maps a node to the edges incident on it in the walk direction, and `step`
 * yields the node to continue from for a given edge. The visited set guards
 * against cycles even though the catalog dependency graph is acyclic.
 */
function collect(
  root: string,
  adjacency: Map<string, WorkflowGraphEdgeRow[]>,
  step: (e: WorkflowGraphEdgeRow) => string,
  out: Map<string, WorkflowGraphEdgeRow>,
): void {
  const visited = new Set<string>([root]);
  const queue = [root];
  while (queue.length > 0) {
    const node = queue.shift();
    if (node === undefined) break;
    const edges = adjacency.get(node);
    if (!edges) continue;
    for (const e of edges) {
      out.set(workflowGraphEdgeKey(e), e);
      const next = step(e);
      if (!visited.has(next)) {
        visited.add(next);
        queue.push(next);
      }
    }
  }
}

/**
 * Client-side equivalent of the upstream/downstream recursion in
 * `buildWorkflowGraphQuery`. Given the full edge set and a root object id, returns
 * every edge reachable by walking parents (upstream) and children (downstream)
 * from the root, deduped, matching the SQL result shape.
 */
export function traverseWorkflowGraph(
  edges: WorkflowGraphEdgeRow[],
  objectId: string,
): WorkflowGraphEdgeRow[] {
  const byChild = index(edges, (e) => e.childId);
  const byParent = index(edges, (e) => e.parentId);
  const result = new Map<string, WorkflowGraphEdgeRow>();
  collect(objectId, byChild, (e) => e.parentId, result);
  collect(objectId, byParent, (e) => e.childId, result);
  return [...result.values()];
}
