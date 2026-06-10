// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { escapedLiteral as lit, executeSqlV2 } from "~/api/materialize";

import { queryBuilder } from "./db";

export interface WorkflowGraphEdge {
  parentId: string | null;
  childId: string | null;
}

export const ALLOWED_OBJECT_TYPES = [
  "index" as const,
  "materialized-view" as const,
  "sink" as const,
  "source" as const,
  "table" as const,
];

export type AllowedObjects = (typeof ALLOWED_OBJECT_TYPES)[0];

/**
 * Given an object ID, looks for all compute dependencies up and downstream. Returns
 * pairs of IDs that represent edges in the graph.
 *
 * 1) Filters materialization dependencies for allowed types
 * 2) Unions sources that have subsources and sinks
 * 3) Walk upstream from the given object
 * 4) Walk downstream from the given object
 * 5) Union those two sets
 */
export function buildWorkflowGraphQuery({ objectId }: { objectId: string }) {
  return sql`
WITH MUTUALLY RECURSIVE
  allowed_compute_dependencies(child_id text, parent_id text) AS (
    SELECT object_id, dependency_id FROM mz_materialization_dependencies
    JOIN mz_objects AS parent_o ON parent_o.id = dependency_id
    JOIN mz_objects AS child_o ON child_o.id = object_id
    AND parent_o.type in (${sql.raw(
      ALLOWED_OBJECT_TYPES.map((t) => `'${t}'`).join(", "),
    )})
    AND child_o.type in (${sql.raw(
      ALLOWED_OBJECT_TYPES.map((t) => `'${t}'`).join(", "),
    )})
  ),
  -- TODO: (#3400) Remove once all subsources are tables
  sources_with_subsources_object_dependencies(child_id text, parent_id text) AS (
    SELECT od.object_id as child_id, referenced_object_id as parent_id
    FROM mz_object_dependencies od
    JOIN mz_sources AS parent_sources ON parent_sources.id = od.referenced_object_id
    JOIN mz_sources AS child_sources ON child_sources.id = od.object_id
    WHERE parent_sources.id LIKE 'u%'
    AND child_sources.type IS NOT NULL AND child_sources.type = 'subsource'
    UNION ALL
    SELECT t.id as child_id, source_id as parent_id
    FROM mz_tables t
    JOIN mz_sources AS parent_sources ON parent_sources.id = t.source_id
  ),
  allowed_dependencies(child_id text, parent_id text) AS (
    SELECT * from allowed_compute_dependencies
    UNION
    -- Sources with subsources (i.e. loadgen and Postgres sources) don't exist in mz_compute_dependencies,
    -- but their subsources do since dataflows exist in subsources. Thus we union sources_with_subsources_object_dependencies
    -- to create edges and nodes for these type of sources.
    SELECT * from sources_with_subsources_object_dependencies
  ),
  upstream(child_id text, parent_id text) AS (
    SELECT *
    FROM allowed_dependencies
    WHERE child_id = ${lit(objectId)}
    UNION
    SELECT r2.*
    FROM upstream r1
    JOIN allowed_dependencies r2(child_id, parent_id) ON r1.parent_id = r2.child_id
  ),
  downstream(child_id text, parent_id text) AS (
    SELECT *
    FROM allowed_dependencies
    WHERE parent_id = ${lit(objectId)}
    UNION
    SELECT r2.*
    FROM downstream r1
    JOIN allowed_dependencies r2(child_id, parent_id) ON r2.parent_id = r1.child_id
  )
SELECT child_id as "childId", parent_id as "parentId" FROM upstream
UNION
SELECT child_id as "childId", parent_id as "parentId" FROM downstream
`.$castTo<WorkflowGraphEdge>();
}

export type WorkflowGraphParams = {
  objectId: string;
};

/**
 * Fetches all upstream and downstream compute dependencies of an object.
 */
export async function fetchWorkflowGraph({
  params,
  queryKey,
  requestOptions,
}: {
  params: WorkflowGraphParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildWorkflowGraphQuery(params).compile(queryBuilder);

  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
