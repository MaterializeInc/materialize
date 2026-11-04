// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { RawBuilder, sql } from "kysely";

/** The first version with `mz_internal.mz_object_graph_edges`. */
export const OBJECT_GRAPH_EDGES_MIN_VERSION = "26.39.0";

/** A dependency edge: `childId` depends on `parentId`. */
export interface WorkflowGraphEdgeRow {
  childId: string;
  parentId: string;
}

/** Identity of an edge. Catalog ids contain no spaces or commas. */
export function workflowGraphEdgeKey(edge: WorkflowGraphEdgeRow): string {
  return `${edge.childId} ${edge.parentId}`;
}

/** The inverse of `workflowGraphEdgeKey`. */
export function parseWorkflowGraphEdgeKey(key: string): WorkflowGraphEdgeRow {
  const [childId, parentId] = key.split(" ");
  return { childId, parentId };
}

/**
 * Every dependency edge in the environment. `mz_object_graph_edges` is indexed
 * on `mz_catalog_server`, so a SUBSCRIBE to this reads that index.
 */
export function buildWorkflowGraphEdgesQuery(): RawBuilder<WorkflowGraphEdgeRow> {
  return sql<WorkflowGraphEdgeRow>`
SELECT object_id AS "childId", dependency_id AS "parentId"
FROM mz_internal.mz_object_graph_edges`;
}
