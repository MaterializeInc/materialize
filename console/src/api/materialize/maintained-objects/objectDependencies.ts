// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult, sql } from "kysely";

import {
  escapedLiteral as lit,
  executeSqlV2,
  queryBuilder,
} from "~/api/materialize";
import { queryBuilder as rawQueryBuilder } from "~/api/materialize/db";

/**
 * Fetches direct upstream dependencies for an object with freshness info.
 * Uses Frank McSherry's crit_input pattern: identifies which inputs are
 * at the minimum frontier (the actual bottlenecks) vs inputs that are caught up.
 *
 * Returns all direct inputs with:
 * - Their current lag (from mz_wallclock_global_lag)
 * - Whether they are a bottleneck (at the minimum frontier among all inputs)
 * - The delay they introduce (source frontier - target frontier)
 */
export function buildUpstreamDependenciesQuery(objectId: string) {
  return queryBuilder
    .with("input_of", (cte) =>
      cte
        .selectFrom("mz_compute_dependencies")
        .select([
          "dependency_id as source",
          "object_id as target",
        ]),
    )
    .with("input_frontier", (cte) =>
      cte
        .selectFrom("input_of as i")
        .innerJoin(
          "mz_frontiers as f",
          "f.object_id",
          "i.source",
        )
        .select([
          "i.target",
          sql<string>`min(f.write_frontier)`.as("min_frontier"),
        ])
        .groupBy("i.target"),
    )
    .selectFrom("input_of as i")
    .innerJoin("mz_objects as o", "o.id", "i.source")
    .innerJoin(
      "mz_object_fully_qualified_names as fqn",
      "fqn.id",
      "i.source",
    )
    .innerJoin("mz_frontiers as fs", "fs.object_id", "i.source")
    .innerJoin("mz_frontiers as ft", "ft.object_id", "i.target")
    .leftJoin("mz_clusters as c", "c.id", "o.cluster_id")
    .leftJoin("input_frontier as i_f", "i_f.target", "i.target")
    .leftJoin(
      queryBuilder
        .selectFrom("mz_wallclock_global_lag_recent_history")
        .select(["object_id", "lag"])
        .distinctOn(["object_id"])
        .orderBy("object_id")
        .orderBy("occurred_at", "desc")
        .where(
          (eb) => sql`${eb.ref("occurred_at")} + INTERVAL '5 MINUTES'`,
          ">=",
          sql<Date>`mz_now()`,
        )
        .as("ll"),
      "ll.object_id",
      "i.source",
    )
    .select([
      "i.source as id",
      "fqn.name",
      sql<string>`o.type`.as("objectType"),
      "fqn.schema_name as schemaName",
      "fqn.database_name as databaseName",
      "c.name as clusterName",
      "ll.lag",
      sql<string>`fs.write_frontier::text::numeric - ft.write_frontier::text::numeric`.as(
        "delay",
      ),
      sql<boolean>`fs.write_frontier = i_f.min_frontier`.as("isBottleneck"),
    ])
    .where("i.target", "=", objectId)
    .orderBy(sql`fs.write_frontier::text::numeric - ft.write_frontier::text::numeric`, "desc");
}

export type UpstreamDependencyRow = InferResult<
  ReturnType<typeof buildUpstreamDependenciesQuery>
>[0];

export async function fetchUpstreamDependencies({
  objectId,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildUpstreamDependenciesQuery(objectId).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
    sessionVariables: {
      transaction_isolation: "serializable",
    },
  });
}

/**
 * Fetches direct downstream dependents — objects that depend on this object.
 * Simpler than upstream: no bottleneck detection needed, just "who reads from me?"
 */
export function buildDownstreamDependentsQuery(objectId: string) {
  return queryBuilder
    .selectFrom("mz_compute_dependencies as cd")
    .innerJoin("mz_objects as o", "o.id", "cd.object_id")
    .innerJoin(
      "mz_object_fully_qualified_names as fqn",
      "fqn.id",
      "cd.object_id",
    )
    .leftJoin("mz_clusters as c", "c.id", "o.cluster_id")
    .leftJoin(
      queryBuilder
        .selectFrom("mz_wallclock_global_lag_recent_history")
        .select(["object_id", "lag"])
        .distinctOn(["object_id"])
        .orderBy("object_id")
        .orderBy("occurred_at", "desc")
        .where(
          (eb) => sql`${eb.ref("occurred_at")} + INTERVAL '5 MINUTES'`,
          ">=",
          sql<Date>`mz_now()`,
        )
        .as("ll"),
      "ll.object_id",
      "cd.object_id",
    )
    .select([
      "cd.object_id as id",
      "fqn.name",
      sql<string>`o.type`.as("objectType"),
      "fqn.schema_name as schemaName",
      "fqn.database_name as databaseName",
      "c.name as clusterName",
      "ll.lag",
    ])
    .where("cd.dependency_id", "=", objectId)
    .orderBy("fqn.name");
}

export type DownstreamDependentRow = InferResult<
  ReturnType<typeof buildDownstreamDependentsQuery>
>[0];

export async function fetchDownstreamDependents({
  objectId,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildDownstreamDependentsQuery(objectId).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
    sessionVariables: {
      transaction_isolation: "serializable",
    },
  });
}

/**
 * Full critical path query — Frank McSherry's crit_path + crit_edge pattern.
 * Uses WITH MUTUALLY RECURSIVE to trace ALL blocking objects from the probe
 * back to root sources, following only critical edges (inputs at the minimum frontier).
 * Returns every edge on the critical path with delay per edge.
 *
 * This is expensive (~1-5s) — only run on user action, not on panel open.
 */
export interface CriticalPathEdge {
  sourceId: string;
  targetId: string;
  sourceName: string;
  sourceType: string;
  targetName: string;
  targetType: string;
  /** Frontier gap: source.frontier - target.frontier */
  edgeDelay: number;
  /** Self delay for target: min(input_frontiers) - target.frontier. Positive = target is bottleneck */
  targetSelfDelay: number;
  /** Wallclock distance for source node */
  sourceFrontierLag: number;
  /** Wallclock distance for target node */
  targetFrontierLag: number;
}

export async function fetchCriticalPath({
  objectId,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const query = sql`
WITH MUTUALLY RECURSIVE
  -- All dependency edges (compute + object deps for sources/tables)
  input_of(source text, target text) AS (
    SELECT dependency_id, object_id
    FROM mz_internal.mz_materialization_dependencies
    UNION
    SELECT referenced_object_id, object_id
    FROM mz_internal.mz_object_dependencies
    WHERE referenced_object_id LIKE 'u%'
      AND object_id LIKE 'u%'
  ),
  -- Per target: the minimum frontier across all inputs (Frank's input_frontier)
  input_frontier(target text, frontier mz_timestamp) AS (
    SELECT i.target, min(f.write_frontier)
    FROM input_of i
    JOIN mz_internal.mz_frontiers f ON i.source = f.object_id
    GROUP BY i.target
  ),
  -- Only follow critical edges: inputs at the minimum frontier (Frank's crit_input)
  crit_input(source text, target text) AS (
    SELECT i.source, i.target
    FROM input_of i
    JOIN mz_internal.mz_frontiers f ON i.source = f.object_id
    JOIN input_frontier i_f ON i.target = i_f.target
    WHERE f.write_frontier = i_f.frontier
  ),
  -- Walk backwards from probe, following only critical edges (Frank's crit_path)
  delayed_by(probe_id text, source text, target text) AS (
    SELECT ${lit(objectId)}, ${lit(objectId)}, ${lit(objectId)}
    UNION
    SELECT db.probe_id, ci.source, ci.target
    FROM delayed_by db
    JOIN crit_input ci ON db.source = ci.target
  )
SELECT DISTINCT
  db.source AS "sourceId",
  db.target AS "targetId",
  os.name AS "sourceName",
  os.type AS "sourceType",
  ot.name AS "targetName",
  ot.type AS "targetType",
  -- Edge delay: frontier gap between source and target
  fs.write_frontier::text::numeric - ft.write_frontier::text::numeric AS "edgeDelay",
  -- Self delay for the TARGET node: min(input frontiers) - output frontier
  -- Positive means the target node itself is the bottleneck
  COALESCE(i_f_t.frontier::text::numeric - ft.write_frontier::text::numeric, 0) AS "targetSelfDelay",
  -- Frontier lag for source: wallclock distance
  mz_now()::text::numeric - fs.write_frontier::text::numeric AS "sourceFrontierLag",
  -- Frontier lag for target: wallclock distance
  mz_now()::text::numeric - ft.write_frontier::text::numeric AS "targetFrontierLag"
FROM delayed_by db
JOIN mz_internal.mz_frontiers fs ON db.source = fs.object_id
JOIN mz_internal.mz_frontiers ft ON db.target = ft.object_id
JOIN mz_catalog.mz_objects os ON db.source = os.id
JOIN mz_catalog.mz_objects ot ON db.target = ot.id
LEFT JOIN input_frontier i_f_t ON i_f_t.target = db.target
WHERE db.source != db.target
ORDER BY fs.write_frontier::text::numeric - ft.write_frontier::text::numeric DESC
  `.compile(rawQueryBuilder);

  return executeSqlV2({
    queries: query,
    queryKey,
    requestOptions,
    sessionVariables: {
      transaction_isolation: "serializable",
    },
  });
}
