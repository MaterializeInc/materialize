// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { CompiledQuery, RawBuilder, sql } from "kysely";

import {
  escapedLiteral as lit,
  executeSqlV2,
  IPostgresInterval,
  queryBuilder,
} from "~/api/materialize";

import { MAINTAINED_OBJECT_TYPES } from "./constants";

const MAINTAINED_OBJECT_TYPES_SQL = sql.raw(
  MAINTAINED_OBJECT_TYPES.map((t) => `'${t}'`).join(", "),
);

/**
 * `source_exports` CTE definition for `WITH MUTUALLY RECURSIVE`: each
 * subsource and source-fed table, its source, and the cluster that source
 * ingests on. Exports have no cluster of their own.
 */
const SOURCE_EXPORTS_CTE = sql`source_exports(
    export_id text,
    source_id text,
    cluster_id text
  ) AS (
    SELECT od.object_id, ps.id, ps.cluster_id
    FROM mz_object_dependencies od
    JOIN mz_sources cs ON cs.id = od.object_id AND cs.type = 'subsource'
    JOIN mz_sources ps ON ps.id = od.referenced_object_id
    UNION ALL
    SELECT t.id, ps.id, ps.cluster_id
    FROM mz_tables t
    JOIN mz_sources ps ON ps.id = t.source_id
  )`;

/** `mz_objects` reports a subsource as `source`. Needs `o` and `se` joined. */
const EXPORT_OBJECT_TYPE_SQL = sql`CASE
    WHEN se.export_id IS NOT NULL AND o.type = 'source' THEN 'subsource'
    ELSE o.type
  END`;

export interface CriticalPathRow {
  id: string;
  childId: string;
  name: string;
  objectType: string;
  schemaName: string;
  databaseName: string | null;
  /** For a subsource or source-fed table, the cluster its source ingests on. */
  clusterId: string | null;
  clusterName: string | null;
  /** This node's lag at the snapshot moment used to draw the chain. */
  lag: IPostgresInterval | null;
  /** The consumer's lag at the same snapshot moment. */
  targetLag: IPostgresInterval | null;
  isBottleneck: boolean;
  /** This node's max lag in the lookback window, independent of the chain snapshot. */
  peakLag: IPostgresInterval | null;
  /** Observation time of `peakLag`. */
  peakLagAt: Date | null;
}

/**
 * `per_object_lag` CTE definition for when the user has not selected a
 * timestamp on the freshness chart. `buildCriticalPathQuery` splices this
 * fragment into the critical path query via its `${perObjectLag}` placeholder;
 * the fragment also declares the `probe_peak_bucket` CTE it depends on.
 *
 * Gets each object's lag observation inside the chart bucket where the
 * queried object's lag peaked over the lookback window.
 */
const perObjectLagCteAtProbePeak = (
  objectId: string,
  lookbackMinutes: number,
  bucketSizeMs: number,
) => {
  const lookback = sql.raw(`${lookbackMinutes}`);
  const bucketMs = sql.raw(`${bucketSizeMs}`);
  // `DISTINCT ON (object_id)` picks the probe's peak-lag bucket;
  // `LIMIT` isn't reliable inside `WITH MUTUALLY RECURSIVE`. The
  // `::timestamptz` cast pins the CTE column type.
  return sql`probe_peak_bucket(bucket_start timestamptz) AS (
    SELECT DISTINCT ON (object_id) date_bin(
        INTERVAL '${bucketMs} MILLISECONDS',
        occurred_at,
        TIMESTAMP '1970-01-01'
      )::timestamptz
    FROM mz_wallclock_global_lag_recent_history
    WHERE object_id = ${lit(objectId)}
      AND occurred_at + INTERVAL '${lookback} MINUTES' >= mz_now()
    ORDER BY object_id, lag DESC NULLS LAST
  ),
  per_object_lag(object_id text, lag interval) AS (
    SELECT DISTINCT ON (h.object_id) h.object_id, h.lag
    FROM mz_wallclock_global_lag_recent_history h, probe_peak_bucket b
    WHERE h.occurred_at >= b.bucket_start
      AND h.occurred_at < b.bucket_start + INTERVAL '${bucketMs} MILLISECONDS'
    ORDER BY h.object_id, h.lag DESC
  )`;
};

/**
 * `per_object_lag` CTE definition for when the user has selected (locked) a
 * timestamp on the freshness chart. `buildCriticalPathQuery` splices this
 * fragment into the critical path query via its `${perObjectLag}` placeholder.
 *
 * Gets each object's most recent lag observation inside the chart bucket
 * containing the selected timestamp.
 */
const perObjectLagCteAtTime = (timestamp: Date, bucketSizeMs: number) => {
  const tsLit = sql.lit(timestamp.toISOString());
  const bucketMs = sql.raw(`${bucketSizeMs}`);
  return sql`per_object_lag(object_id text, lag interval) AS (
    SELECT DISTINCT ON (object_id) object_id, lag
    FROM mz_wallclock_global_lag_recent_history
    WHERE occurred_at >= date_bin(
        INTERVAL '${bucketMs} MILLISECONDS',
        ${tsLit}::timestamptz,
        TIMESTAMP '1970-01-01'
      )
      AND occurred_at < date_bin(
        INTERVAL '${bucketMs} MILLISECONDS',
        ${tsLit}::timestamptz,
        TIMESTAMP '1970-01-01'
      ) + INTERVAL '${bucketMs} MILLISECONDS'
    ORDER BY object_id, lag DESC
  )`;
};

/**
 * `per_object_peak` CTE definition: each object's max lag in the lookback
 * window and when it occurred, independent of the chain snapshot. Joined onto
 * the result rows as per-node badge metadata.
 */
const perObjectPeakCte = (lookbackMinutes: number) => {
  const lookback = sql.raw(`${lookbackMinutes}`);
  return sql`per_object_peak(object_id text, peak_lag interval, peak_at timestamptz) AS (
    SELECT DISTINCT ON (object_id)
      object_id, lag, occurred_at::timestamptz
    FROM mz_wallclock_global_lag_recent_history
    WHERE occurred_at + INTERVAL '${lookback} MINUTES' >= mz_now()
    ORDER BY object_id, lag DESC NULLS LAST, occurred_at DESC
  )`;
};

/**
 * Given an object ID, walks the critical path back to its sources and returns
 * one row per chain edge plus the immediate off-path siblings of each chain
 * node. The two callers — `buildCriticalPathLiveQuery` and
 * `buildCriticalPathAtTimeQuery` — parameterize this template by passing a
 * different `per_object_lag` CTE definition.
 *
 * 1) For each child, pick its slowest input(s) — the bottlenecks
 * 2) Walk backwards from the queried object along bottleneck edges
 * 3) Restrict to inputs of nodes on the chain (queried object + ancestors)
 * 4) Join object metadata, cluster name, and per-node window-pMAX
 *    (independent of the chain snapshot)
 */
const buildCriticalPathQuery = (
  objectId: string,
  lookbackMinutes: number,
  perObjectLag: RawBuilder<unknown>,
): CompiledQuery<CriticalPathRow> => {
  return sql<CriticalPathRow>`
WITH MUTUALLY RECURSIVE
  -- mz_materialization_dependencies covers sinks (which read from MVs/sources)
  -- as well as compute objects. Restrict both ends of the edge to maintained
  -- object types so tables written with INSERT drop out of the chain.
  -- Source-fed tables stay: like subsources, their lag is ingestion lag. The
  -- second branch adds the edge from each of those back to its source.
  input_of(source text, target text) AS (
    SELECT md.dependency_id, md.object_id
    FROM mz_materialization_dependencies md
    JOIN mz_objects src ON src.id = md.dependency_id
    LEFT JOIN mz_tables t ON t.id = md.dependency_id
    JOIN mz_objects tgt ON tgt.id = md.object_id
      AND tgt.type IN (${MAINTAINED_OBJECT_TYPES_SQL})
    WHERE src.type IN (${MAINTAINED_OBJECT_TYPES_SQL})
      OR t.source_id IS NOT NULL
    UNION ALL
    SELECT source_id, export_id FROM source_exports
  ),
  ${SOURCE_EXPORTS_CTE},
  ${perObjectLag},
  ${perObjectPeakCte(lookbackMinutes)},
  max_input_lag(target text, max_lag interval) AS (
    SELECT i.target, max(p.lag)
    FROM input_of i
    JOIN per_object_lag p ON p.object_id = i.source
    GROUP BY i.target
  ),
  crit_input(source text, target text) AS (
    SELECT i.source, i.target
    FROM input_of i
    JOIN per_object_lag p ON p.object_id = i.source
    JOIN max_input_lag m ON m.target = i.target
    WHERE p.lag IS NOT DISTINCT FROM m.max_lag
  ),
  -- Seed with (objectId, objectId) so its direct inputs land in the result.
  delayed_by(source text, target text) AS (
    SELECT ${lit(objectId)}, ${lit(objectId)}
    UNION
    SELECT ci.source, ci.target
    FROM delayed_by db
    JOIN crit_input ci ON db.source = ci.target
  ),
  chain_targets(target text) AS (
    SELECT DISTINCT target FROM delayed_by
  )
SELECT
  i.source AS "id",
  i.target AS "childId",
  fqn.name AS "name",
  ${EXPORT_OBJECT_TYPE_SQL} AS "objectType",
  fqn.schema_name AS "schemaName",
  fqn.database_name AS "databaseName",
  c.id AS "clusterId",
  c.name AS "clusterName",
  ls.lag AS "lag",
  lt.lag AS "targetLag",
  EXISTS (
    SELECT 1 FROM crit_input ci
    WHERE ci.source = i.source AND ci.target = i.target
  ) AS "isBottleneck",
  pk.peak_lag AS "peakLag",
  pk.peak_at AS "peakLagAt"
FROM input_of i
JOIN chain_targets ct ON ct.target = i.target
JOIN mz_objects o ON o.id = i.source
JOIN mz_object_fully_qualified_names fqn ON fqn.id = i.source
LEFT JOIN source_exports se ON se.export_id = i.source
LEFT JOIN mz_clusters c ON c.id = coalesce(o.cluster_id, se.cluster_id)
LEFT JOIN per_object_lag ls ON ls.object_id = i.source
LEFT JOIN per_object_lag lt ON lt.object_id = i.target
LEFT JOIN per_object_peak pk ON pk.object_id = i.source
ORDER BY ls.lag DESC NULLS LAST
  `.compile(queryBuilder);
};

/**
 * Compiles the critical-path query for default mode (the user has not
 * locked a timestamp on the freshness chart). The chain is snapshotted
 * from the bucket where the probe's lag peaked in the lookback window.
 */
export const buildCriticalPathLiveQuery = (
  objectId: string,
  lookbackMinutes: number,
  bucketSizeMs: number,
): CompiledQuery<CriticalPathRow> =>
  buildCriticalPathQuery(
    objectId,
    lookbackMinutes,
    perObjectLagCteAtProbePeak(objectId, lookbackMinutes, bucketSizeMs),
  );

/**
 * Compiles the critical-path query for the user's selected `timestamp`. Each
 * node's lag is read from the bucket aligned to that timestamp, so the chain
 * matches what the freshness chart drew at that point. `lookbackMinutes` is
 * only used to compute the per-node `peakLag` badge.
 */
export const buildCriticalPathAtTimeQuery = (
  objectId: string,
  timestamp: Date,
  bucketSizeMs: number,
  lookbackMinutes: number,
): CompiledQuery<CriticalPathRow> =>
  buildCriticalPathQuery(
    objectId,
    lookbackMinutes,
    perObjectLagCteAtTime(timestamp, bucketSizeMs),
  );

export type SourceExportLagRow = Omit<
  CriticalPathRow,
  "childId" | "targetLag" | "isBottleneck"
>;

/**
 * One row per subsource and source-fed table of `sourceId`. Each row's lag is
 * read from the same bucket the critical path snapshots: the bucket where the
 * source's lag peaked over the lookback window, or the bucket containing
 * `timestamp` when one is selected.
 */
export const buildSourceExportLagsQuery = (
  sourceId: string,
  lookbackMinutes: number,
  bucketSizeMs: number,
  timestamp: Date | null,
): CompiledQuery<SourceExportLagRow> => {
  const perObjectLag = timestamp
    ? perObjectLagCteAtTime(timestamp, bucketSizeMs)
    : perObjectLagCteAtProbePeak(sourceId, lookbackMinutes, bucketSizeMs);
  return sql<SourceExportLagRow>`
WITH MUTUALLY RECURSIVE
  ${SOURCE_EXPORTS_CTE},
  ${perObjectLag},
  ${perObjectPeakCte(lookbackMinutes)}
SELECT
  se.export_id AS "id",
  fqn.name AS "name",
  ${EXPORT_OBJECT_TYPE_SQL} AS "objectType",
  fqn.schema_name AS "schemaName",
  fqn.database_name AS "databaseName",
  c.id AS "clusterId",
  c.name AS "clusterName",
  ls.lag AS "lag",
  pk.peak_lag AS "peakLag",
  pk.peak_at AS "peakLagAt"
FROM source_exports se
JOIN mz_objects o ON o.id = se.export_id
JOIN mz_object_fully_qualified_names fqn ON fqn.id = se.export_id
LEFT JOIN mz_clusters c ON c.id = se.cluster_id
LEFT JOIN per_object_lag ls ON ls.object_id = se.export_id
LEFT JOIN per_object_peak pk ON pk.object_id = se.export_id
WHERE se.source_id = ${lit(sourceId)}
ORDER BY ls.lag DESC NULLS LAST
  `.compile(queryBuilder);
};

export const fetchSourceExportLags = ({
  sourceId,
  lookbackMinutes,
  bucketSizeMs,
  timestamp,
  queryKey,
  requestOptions,
}: {
  sourceId: string;
  lookbackMinutes: number;
  bucketSizeMs: number;
  timestamp: Date | null;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) =>
  executeSqlV2({
    queries: buildSourceExportLagsQuery(
      sourceId,
      lookbackMinutes,
      bucketSizeMs,
      timestamp,
    ),
    queryKey,
    requestOptions,
    requestTimeoutMs: 30_000,
    sessionVariables: { transaction_isolation: "serializable" },
  });

/**
 * Fetches the critical path for default mode (no user-selected timestamp).
 * The chain is snapshotted from the bucket where the probe's lag peaked in
 * the lookback window.
 */
export const fetchCriticalPathLive = ({
  objectId,
  lookbackMinutes,
  bucketSizeMs,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  lookbackMinutes: number;
  bucketSizeMs: number;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) =>
  executeSqlV2({
    queries: buildCriticalPathLiveQuery(
      objectId,
      lookbackMinutes,
      bucketSizeMs,
    ),
    queryKey,
    requestOptions,
    requestTimeoutMs: 30_000,
    sessionVariables: { transaction_isolation: "serializable" },
  });

/**
 * Fetches the critical path at the user's selected `timestamp` — each node's
 * lag is read from the chart bucket containing it, so the chain matches what
 * the chart drew at that point. `lookbackMinutes` is only used to compute
 * the per-node `peakLag` badge.
 */
export const fetchCriticalPathAtTime = ({
  objectId,
  timestamp,
  bucketSizeMs,
  lookbackMinutes,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  timestamp: Date;
  bucketSizeMs: number;
  lookbackMinutes: number;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) =>
  executeSqlV2({
    queries: buildCriticalPathAtTimeQuery(
      objectId,
      timestamp,
      bucketSizeMs,
      lookbackMinutes,
    ),
    queryKey,
    requestOptions,
    requestTimeoutMs: 30_000,
    sessionVariables: { transaction_isolation: "serializable" },
  });
