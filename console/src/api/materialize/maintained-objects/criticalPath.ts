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

export interface CriticalPathRow {
  id: string;
  childId: string;
  name: string;
  objectType: string;
  schemaName: string;
  databaseName: string | null;
  clusterId: string | null;
  clusterName: string | null;
  lag: IPostgresInterval | null;
  targetLag: IPostgresInterval | null;
  isBottleneck: boolean;
  parentSourceId: string | null;
}

/**
 * `per_object_lag` CTE definition for default mode when the user hasn't
 * selected a timestamp. `buildCriticalPathQuery` splices this fragment into
 * the critical path query via its `${perObjectLag}` placeholder.
 *
 * Gets each object's single MAX lag value over the lookback window that the
 * user selected in the maintained objects page.
 */
const perObjectLagCtePMaxInWindow = (lookbackMinutes: number) => {
  const lookback = sql.raw(`${lookbackMinutes}`);
  return sql`per_object_lag(object_id text, lag interval) AS (
    SELECT object_id, max(lag) AS lag
    FROM mz_wallclock_global_lag_recent_history
    WHERE occurred_at + INTERVAL '${lookback} MINUTES' >= mz_now()
    GROUP BY object_id
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
 * Given an object ID, walks the critical path back to its sources and returns
 * one row per chain edge plus the immediate off-path siblings of each chain
 * node. The mode-specific bit is just `per_object_lag`.
 *
 * 1) For each child, pick its slowest input(s) — the bottlenecks
 * 2) Walk backwards from the queried object along bottleneck edges
 * 3) Restrict to inputs of nodes on the chain (queried object + ancestors)
 * 4) Join object metadata, cluster name, and subsource → parent redirection
 */
const buildCriticalPathQuery = (
  objectId: string,
  perObjectLag: RawBuilder<unknown>,
): CompiledQuery<CriticalPathRow> =>
  sql<CriticalPathRow>`
WITH MUTUALLY RECURSIVE
  input_of(source text, target text) AS (
    SELECT dependency_id, object_id
    FROM mz_compute_dependencies
  ),
  ${perObjectLag},
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
  ),
  -- Subsources are filtered out of the maintained-objects list, so the panel
  -- redirects drawer links to the parent source.
  subsource_parents(subsource_id text, parent_id text) AS (
    SELECT od.object_id, od.referenced_object_id
    FROM mz_object_dependencies od
    JOIN mz_sources cs ON cs.id = od.object_id AND cs.type = 'subsource'
    JOIN mz_sources ps ON ps.id = od.referenced_object_id
      AND ps.id LIKE 'u%'
  )
SELECT
  i.source AS "id",
  i.target AS "childId",
  fqn.name AS "name",
  o.type AS "objectType",
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
  sp.parent_id AS "parentSourceId"
FROM input_of i
JOIN chain_targets ct ON ct.target = i.target
JOIN mz_objects o ON o.id = i.source
JOIN mz_object_fully_qualified_names fqn ON fqn.id = i.source
LEFT JOIN mz_clusters c ON c.id = o.cluster_id
LEFT JOIN per_object_lag ls ON ls.object_id = i.source
LEFT JOIN per_object_lag lt ON lt.object_id = i.target
LEFT JOIN subsource_parents sp ON sp.subsource_id = i.source
ORDER BY ls.lag DESC NULLS LAST
  `.compile(queryBuilder);

/**
 * Compiles the critical-path query when no timestamp is selected. Each
 * upstream's lag is the pMAX over the lookback window, so the chain reflects
 * the same window the maintained objects list page uses.
 */
export const buildCriticalPathPMaxInWindowQuery = (
  objectId: string,
  lookbackMinutes: number,
): CompiledQuery<CriticalPathRow> =>
  buildCriticalPathQuery(
    objectId,
    perObjectLagCtePMaxInWindow(lookbackMinutes),
  );

/**
 * Compiles the critical-path query for the user's selected `timestamp`. Each
 * upstream's lag is read from the bucket aligned to that timestamp, so the
 * chain values match what the freshness chart drew at that point.
 */
export const buildCriticalPathAtTimeQuery = (
  objectId: string,
  timestamp: Date,
  bucketSizeMs: number,
): CompiledQuery<CriticalPathRow> =>
  buildCriticalPathQuery(
    objectId,
    perObjectLagCteAtTime(timestamp, bucketSizeMs),
  );

/**
 * Fetches the critical path when no timestamp is selected — each upstream's
 * lag is the pMAX over the lookback window, matching the list page's lag
 * column.
 */
export const fetchCriticalPathPMaxInWindow = ({
  objectId,
  lookbackMinutes,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  lookbackMinutes: number;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) =>
  executeSqlV2({
    queries: buildCriticalPathPMaxInWindowQuery(objectId, lookbackMinutes),
    queryKey,
    requestOptions,
    requestTimeoutMs: 30_000,
    sessionVariables: { transaction_isolation: "serializable" },
  });

/**
 * Fetches the critical path at the user's selected `timestamp` — each
 * upstream's lag is read from the chart bucket containing it, so the chain
 * matches what the chart drew at that point.
 */
export const fetchCriticalPathAtTime = ({
  objectId,
  timestamp,
  bucketSizeMs,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  timestamp: Date;
  bucketSizeMs: number;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) =>
  executeSqlV2({
    queries: buildCriticalPathAtTimeQuery(objectId, timestamp, bucketSizeMs),
    queryKey,
    requestOptions,
    requestTimeoutMs: 30_000,
    sessionVariables: { transaction_isolation: "serializable" },
  });
