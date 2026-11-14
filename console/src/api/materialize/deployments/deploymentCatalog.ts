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

import { executeSqlV2, queryBuilder } from "~/api/materialize";
import {
  ClusterHealth,
  OOM_LOOP_THRESHOLD,
  SinkCutoverRow,
} from "~/platform/deployments/deploymentModel";

export interface QualifiedSchema {
  database: string;
  schema: string;
}

export interface DeployCatalogObjectRow {
  id: string;
  database: string;
  schema: string;
  name: string;
  type: string;
  clusterName: string | null;
  /** Hydrated on at least one replica. Null for objects with no dataflow. */
  hydrated: boolean | null;
  lagMs: number | null;
}

export interface QualifiedObject {
  database: string;
  schema: string;
  name: string;
}

const NO_MATCH = sql<boolean>`false`;

function schemaFilter(schemas: QualifiedSchema[]) {
  if (schemas.length === 0) return NO_MATCH;
  return sql<boolean>`(${sql.join(
    schemas.map((s) => sql`(d.name = ${s.database} AND s.name = ${s.schema})`),
    sql` OR `,
  )})`;
}

/**
 * The checks `mz-deploy wait` runs per cluster, restricted to user objects:
 * replicas, OOM-looping replicas, hydration on the best replica and the
 * largest wallclock lag. Memory is the busiest replica's.
 */
export function buildClusterHealthQuery(clusterNames: string[]) {
  const names = clusterNames.length > 0 ? sql.join(clusterNames) : sql`NULL`;
  return sql<ClusterHealth>`
    WITH
    cl AS (
      SELECT id, name, size, replication_factor
      FROM mz_catalog.mz_clusters
      WHERE name IN (${names})
    ),
    reps AS (
      SELECT r.id, r.cluster_id
      FROM mz_catalog.mz_cluster_replicas r
      JOIN cl ON cl.id = r.cluster_id
    ),
    ooms AS (
      SELECT h.replica_id
      FROM mz_internal.mz_cluster_replica_status_history h
      JOIN reps ON reps.id = h.replica_id
      WHERE h.reason = 'oom-killed'
        AND h.occurred_at + INTERVAL '24 hours' > mz_now()
      GROUP BY h.replica_id
      HAVING count(*) >= ${OOM_LOOP_THRESHOLD}
    ),
    hyd AS (
      SELECT reps.cluster_id,
        count(h.object_id) AS reported,
        count(*) FILTER (WHERE h.object_id LIKE 'u%' AND h.hydrated) AS hydrated,
        count(*) FILTER (WHERE h.object_id LIKE 'u%') AS total
      FROM reps
      LEFT JOIN mz_internal.mz_hydration_statuses h ON h.replica_id = reps.id
      GROUP BY reps.cluster_id, reps.id
    ),
    util AS (
      SELECT reps.cluster_id, max(u.memory_percent) AS memory_percent
      FROM reps
      JOIN mz_internal.mz_cluster_replica_utilization u ON u.replica_id = reps.id
      GROUP BY reps.cluster_id
    ),
    lag AS (
      SELECT o.cluster_id, max(l.lag) AS max_lag
      FROM mz_catalog.mz_objects o
      JOIN cl ON cl.id = o.cluster_id
      JOIN mz_internal.mz_wallclock_global_lag l ON l.object_id = o.id
      WHERE o.id LIKE 'u%'
      GROUP BY o.cluster_id
    )
    SELECT
      cl.id,
      cl.name,
      cl.size,
      cl.replication_factor::int AS "replicationFactor",
      (SELECT count(*) FROM reps WHERE reps.cluster_id = cl.id)::int AS replicas,
      (SELECT count(*) FROM reps JOIN ooms ON ooms.replica_id = reps.id
        WHERE reps.cluster_id = cl.id)::int AS "oomLoopingReplicas",
      COALESCE((SELECT max(reported) FROM hyd WHERE hyd.cluster_id = cl.id), 0) > 0
        AS "hydrationReported",
      COALESCE((SELECT max(hydrated) FROM hyd WHERE hyd.cluster_id = cl.id), 0)::int
        AS hydrated,
      COALESCE((SELECT max(total) FROM hyd WHERE hyd.cluster_id = cl.id), 0)::int
        AS total,
      util.memory_percent::float8 AS "memoryPercent",
      (EXTRACT(EPOCH FROM lag.max_lag) * 1000)::float8 AS "maxLagMs"
    FROM cl
    LEFT JOIN util ON util.cluster_id = cl.id
    LEFT JOIN lag ON lag.cluster_id = cl.id`.compile(queryBuilder);
}

/** User objects in the given schemas, with hydration and lag. */
export function buildDeployObjectsQuery(schemas: QualifiedSchema[]) {
  return sql<DeployCatalogObjectRow>`
    SELECT
      o.id,
      d.name AS database,
      s.name AS schema,
      o.name,
      o.type,
      c.name AS "clusterName",
      bool_or(h.hydrated) AS hydrated,
      (EXTRACT(EPOCH FROM max(l.lag)) * 1000)::float8 AS "lagMs"
    FROM mz_catalog.mz_objects o
    JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
    JOIN mz_catalog.mz_databases d ON d.id = s.database_id
    LEFT JOIN mz_catalog.mz_clusters c ON c.id = o.cluster_id
    LEFT JOIN mz_internal.mz_hydration_statuses h ON h.object_id = o.id
    LEFT JOIN mz_internal.mz_wallclock_global_lag l ON l.object_id = o.id
    WHERE ${schemaFilter(schemas)}
    GROUP BY o.id, d.name, s.name, o.name, o.type, c.name`.compile(
    queryBuilder,
  );
}

/**
 * Sinks outside the deploy that read from relations in its swapped schemas.
 * Joining `mz_relations` drops the sink's connection, so each sink yields one
 * row, for its input.
 * `promote` repoints each with `ALTER SINK ... SET FROM` to the staged object
 * of the same name, so this pairs the sink's write frontier with that object's
 * read frontier.
 */
export function buildSinkCutoverQuery(
  liveSchemas: QualifiedSchema[],
  deployId: string,
) {
  const suffix = `_${deployId}`;
  return sql<SinkCutoverRow>`
    SELECT
      k.id AS "sinkId",
      kd.name AS "sinkDatabase",
      ks.name AS "sinkSchema",
      k.name AS "sinkName",
      st.status,
      st.error,
      s.name AS "dependencySchema",
      dep.name AS "dependencyName",
      (nobj.id IS NOT NULL) AS "hasReplacement",
      kf.write_frontier::text::float8 AS "sinkWriteFrontierMs",
      nf.read_frontier::text::float8 AS "replacementReadFrontierMs",
      (EXTRACT(EPOCH FROM kl.lag) * 1000)::float8 AS "sinkLagMs"
    FROM mz_catalog.mz_sinks k
    JOIN mz_catalog.mz_schemas ks ON ks.id = k.schema_id
    JOIN mz_catalog.mz_databases kd ON kd.id = ks.database_id
    JOIN mz_internal.mz_object_dependencies od ON od.object_id = k.id
    JOIN mz_catalog.mz_relations dep ON dep.id = od.referenced_object_id
    JOIN mz_catalog.mz_schemas s ON s.id = dep.schema_id
    JOIN mz_catalog.mz_databases d ON d.id = s.database_id
    LEFT JOIN mz_internal.mz_sink_statuses st ON st.id = k.id
    LEFT JOIN mz_internal.mz_frontiers kf ON kf.object_id = k.id
    LEFT JOIN mz_catalog.mz_schemas ns
      ON ns.database_id = d.id AND ns.name = s.name || ${suffix}
    LEFT JOIN mz_catalog.mz_objects nobj
      ON nobj.schema_id = ns.id AND nobj.name = dep.name
    LEFT JOIN mz_internal.mz_frontiers nf ON nf.object_id = nobj.id
    LEFT JOIN mz_internal.mz_wallclock_global_lag kl ON kl.object_id = k.id
    WHERE ${schemaFilter(liveSchemas)}`.compile(queryBuilder);
}

/** Which of the given sinks already exist, so `promote` will skip creating them. */
export function buildExistingSinksQuery(sinks: QualifiedObject[]) {
  const filter =
    sinks.length === 0
      ? NO_MATCH
      : sql<boolean>`(${sql.join(
          sinks.map(
            (k) =>
              sql`(d.name = ${k.database} AND s.name = ${k.schema} AND k.name = ${k.name})`,
          ),
          sql` OR `,
        )})`;
  return sql<QualifiedObject>`
    SELECT d.name AS database, s.name AS schema, k.name
    FROM mz_catalog.mz_sinks k
    JOIN mz_catalog.mz_schemas s ON s.id = k.schema_id
    JOIN mz_catalog.mz_databases d ON d.id = s.database_id
    WHERE ${filter}`.compile(queryBuilder);
}

export async function fetchDeploymentCatalog({
  clusterNames,
  schemas,
  liveSchemas,
  deployId,
  pendingSinks,
  queryKey,
  requestOptions,
}: {
  clusterNames: string[];
  schemas: QualifiedSchema[];
  liveSchemas: QualifiedSchema[];
  deployId: string;
  pendingSinks: QualifiedObject[];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const [clusters, objects, sinks, existingSinks] = await executeSqlV2({
    queries: [
      buildClusterHealthQuery(clusterNames),
      buildDeployObjectsQuery(schemas),
      buildSinkCutoverQuery(liveSchemas, deployId),
      buildExistingSinksQuery(pendingSinks),
    ] as const,
    queryKey,
    requestOptions,
  });
  return {
    clusters: clusters.rows,
    objects: objects.rows,
    sinks: sinks.rows,
    existingSinks: existingSinks.rows,
  };
}

/** Cluster health only, for the list page's per-deploy status. */
export async function fetchClusterHealth({
  clusterNames,
  queryKey,
  requestOptions,
}: {
  clusterNames: string[];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const { rows } = await executeSqlV2({
    queries: buildClusterHealthQuery(clusterNames),
    queryKey,
    requestOptions,
  });
  return rows;
}

/** How long after promote the "after the swap" summary covers. */
export const AFTER_SWAP_WINDOW_MINUTES = 30;

export interface AfterSwapRow {
  clusterId: string;
  stagedName: string;
  /** Null once the cluster has been dropped. */
  currentName: string | null;
  peakMemoryFraction: number | null;
  peakCpuFraction: number | null;
  samples: number;
  restarts: number;
  oomKills: number;
}

/**
 * Load on the clusters a deploy promoted, in the window right after promote.
 *
 * `_mz_deploy` keeps only the IDs of the clusters a deploy replaced, and
 * `promote` drops those. The clusters that took over are the ones `stage`
 * created as `<name>_<deploy id>`, so they are found by that name in the audit
 * log, which outlives the rename.
 */
export function buildAfterSwapQuery(deployId: string, promotedAt: Date) {
  return sql<AfterSwapRow>`
    WITH
    staged AS (
      SELECT DISTINCT details->>'id' AS cluster_id, details->>'name' AS staged_name
      FROM mz_catalog.mz_audit_events
      WHERE object_type = 'cluster' AND event_type = 'create'
        AND details->>'name' LIKE ${`%\\_${deployId}`}
    ),
    reps AS (
      SELECT rh.replica_id, rh.cluster_id, rh.size
      FROM mz_internal.mz_cluster_replica_history rh
      JOIN staged ON staged.cluster_id = rh.cluster_id
    ),
    metrics AS (
      SELECT r.cluster_id,
        sum(m.memory_bytes::float8)
          / NULLIF(max(s.memory_bytes::float8 * s.processes), 0) AS mem_frac,
        sum(m.cpu_nano_cores::float8)
          / NULLIF(max(s.cpu_nano_cores::float8 * s.processes), 0) AS cpu_frac
      FROM reps r
      JOIN mz_catalog.mz_cluster_replica_sizes s ON s.size = r.size
      JOIN mz_internal.mz_cluster_replica_metrics_history m
        ON m.replica_id = r.replica_id
      WHERE m.occurred_at >= ${promotedAt.toISOString()}::timestamptz
        AND m.occurred_at < ${promotedAt.toISOString()}::timestamptz
          + INTERVAL '${sql.raw(String(AFTER_SWAP_WINDOW_MINUTES))} minutes'
      GROUP BY r.cluster_id, r.replica_id, m.occurred_at
    ),
    restarts AS (
      SELECT r.cluster_id,
        count(*) AS restarts,
        count(*) FILTER (WHERE h.reason = 'oom-killed') AS ooms
      FROM reps r
      JOIN mz_internal.mz_cluster_replica_status_history h
        ON h.replica_id = r.replica_id
      WHERE h.status = 'offline'
        AND h.occurred_at >= ${promotedAt.toISOString()}::timestamptz
        AND h.occurred_at < ${promotedAt.toISOString()}::timestamptz
          + INTERVAL '${sql.raw(String(AFTER_SWAP_WINDOW_MINUTES))} minutes'
      GROUP BY r.cluster_id
    )
    SELECT
      staged.cluster_id AS "clusterId",
      staged.staged_name AS "stagedName",
      c.name AS "currentName",
      max(metrics.mem_frac) AS "peakMemoryFraction",
      max(metrics.cpu_frac) AS "peakCpuFraction",
      count(metrics.mem_frac)::int AS samples,
      COALESCE(max(restarts.restarts), 0)::int AS restarts,
      COALESCE(max(restarts.ooms), 0)::int AS "oomKills"
    FROM staged
    LEFT JOIN mz_catalog.mz_clusters c ON c.id = staged.cluster_id
    LEFT JOIN metrics ON metrics.cluster_id = staged.cluster_id
    LEFT JOIN restarts ON restarts.cluster_id = staged.cluster_id
    GROUP BY staged.cluster_id, staged.staged_name, c.name`.compile(
    queryBuilder,
  );
}

export async function fetchAfterSwap({
  deployId,
  promotedAt,
  queryKey,
  requestOptions,
}: {
  deployId: string;
  promotedAt: Date;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const { rows } = await executeSqlV2({
    queries: buildAfterSwapQuery(deployId, promotedAt),
    queryKey,
    requestOptions,
  });
  return rows;
}
