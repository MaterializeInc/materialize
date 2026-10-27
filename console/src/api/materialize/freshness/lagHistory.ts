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

import {
  executeSqlV2,
  IPostgresInterval,
  queryBuilder,
} from "~/api/materialize";
import { buildIdListQuery } from "~/api/materialize/expressionBuilders";

// Lag data is recorded on 60s intervals so we set the minimum bucket size to 1 minute
const MIN_BUCKET_SIZE_MS = 60_000;

// We label it as "Not queryable" when the lag is null. This is because lag
// is only null when an index isn't hydrated or when there is no readable time for a table / mv / source.
// In either case, it's not queryable.
export const NULL_LAG_TEXT = "Not queryable";

export function calculateBucketSizeFromLookback(lookbackMs: number) {
  return Math.max(lookbackMs / 60, MIN_BUCKET_SIZE_MS);
}

export type LagHistoryParameters = {
  lookback:
    | {
        type: "getLatest";
        limit?: number;
      }
    | {
        type: "historical";
        // The lookback in milliseconds from the current time. We change the bucket size depending on the lookback.
        lookbackMs: number;
      };
  // Object IDs to filter on
  objectIds?: string[];
  // The cluster ID to filter on
  clusterId?: string;
  // If grouped by cluster, returns the max lag over all objects per cluster
  groupByCluster?: boolean;
  // If true, includes system objects in the query
  includeSystemObjects?: boolean;
};

/**
 * Binned lag readings for a known set of objects, with no name resolution.
 *
 * The Console already holds every object's name, schema, database and type in
 * the `useAllObjects` subscribe, so a caller that knows which objects it wants
 * needs three columns back, not eleven. `buildLagHistoryQuery` still joins
 * `mz_objects`, `mz_clusters` and `mz_object_fully_qualified_names` (itself a
 * view over `mz_objects`, `mz_schemas` and `mz_databases`) because the
 * environment-wide and single-object callers do depend on those names.
 */
export function buildObjectLagHistoryQuery({
  objectIds,
  lookbackMs,
}: {
  objectIds: string[];
  lookbackMs: number;
}) {
  const bucketSizeMs = sql.raw(
    `${calculateBucketSizeFromLookback(lookbackMs)}`,
  );

  return (
    queryBuilder
      .with("ids", () => buildIdListQuery(objectIds))
      .with("readings", (cte) =>
        cte
          .selectFrom("mz_wallclock_global_lag_recent_history")
          .innerJoin("ids", "ids.id", "object_id")
          .select([
            sql<Date>`date_bin(
              '${bucketSizeMs} MILLISECONDS',
              occurred_at,
              TIMESTAMP '1970-01-01'
            )`.as("bucket_start"),
            "lag",
            "object_id",
          ])
          .where(
            (eb) =>
              sql`${eb.ref("occurred_at")} + INTERVAL '${sql.raw(`${lookbackMs}`)} MILLISECONDS'`,
            ">=",
            sql<Date>`mz_now()`,
          ),
      )
      .selectFrom("readings")
      // The reading kept per bin is the worst one in it, matching what
      // `buildLagHistoryQuery` reports.
      .distinctOn(["bucket_start", "object_id"])
      .select(["bucket_start as bucketStart", "object_id as objectId", "lag"])
      .orderBy(["bucket_start", "object_id", "lag desc"])
  );
}

/**
 * Each object's most recent reading.
 *
 * Separate from the binned series because a bin reports the worst reading in
 * its span, which at a 24 hour range is the worst of 24 minutes. That is the
 * wrong answer to "is this bad right now", and the only range where the last
 * bin happens to be the latest reading is one hour.
 *
 * The two minute filter is what keeps this cheap: a reading lands every
 * minute, so the newest one is always inside that window however wide the
 * range being graphed.
 */
export function buildLatestLagQuery(objectIds: string[]) {
  return queryBuilder
    .with("ids", () => buildIdListQuery(objectIds))
    .selectFrom("mz_wallclock_global_lag_recent_history")
    .innerJoin("ids", "ids.id", "object_id")
    .distinctOn("object_id")
    .select(["object_id as objectId", "lag"])
    .where(
      (eb) => sql`${eb.ref("occurred_at")} + INTERVAL '2 MINUTES'`,
      ">=",
      sql<Date>`mz_now()`,
    )
    .orderBy(["object_id", "occurred_at desc"]);
}

/** How often a wallclock lag reading lands, which paces the latest-reading read. */
export const LATEST_READING_INTERVAL_MS = 60_000;

/** Each percentile the freshness page reports, as the fraction of readings at or below it. */
export const LAG_PERCENTILES = { p90: 0.9, p99: 0.99, p99_9: 0.999 } as const;

/**
 * The nearest-rank position of percentile `p` among `n` readings, counted from
 * the worst, which is 1. Matches the pick the percentile queries make in SQL.
 */
export function rankFromWorst(p: number, n: number) {
  return n - Math.ceil(p * n) + 1;
}

/**
 * The most readings one object can have in a range: one a minute, plus one
 * because both ends of the window can hold a reading.
 *
 * NOTE: holds whatever `wallclock_lag_recording_interval` is.
 * `mz_wallclock_global_lag_history` truncates each reading to its minute and
 * keeps one per object per minute, so recording more often adds no rows here,
 * and recording less often only leaves fewer.
 */
function maxReadingsPerObject(lookbackMs: number) {
  return lookbackMs / LATEST_READING_INTERVAL_MS + 1;
}

/**
 * Every reading for `objectIds` in the range. The IDs are a literal list for
 * the reason `buildObjectLagHistoryQuery` gives.
 */
function lagReadingsSince(objectIds: string[], lookbackMs: number) {
  return queryBuilder
    .selectFrom("mz_wallclock_global_lag_recent_history")
    .select(["object_id", "lag"])
    .where("object_id", "in", objectIds)
    .where(
      (eb) =>
        sql`${eb.ref("occurred_at")} + INTERVAL '${sql.raw(`${lookbackMs}`)} MILLISECONDS'`,
      ">=",
      sql<Date>`mz_now()`,
    );
}

// A NULL lag is a reading that could not be taken. Sorting it first ranks it
// as the worst possible reading, so it counts once in the denominator, the same
// as one slow reading: six unreadable readings in sixty no more breach p90 than
// six slow ones do.
const WORST_FIRST = sql`lag DESC NULLS FIRST`;

const PEAK = sql<IPostgresInterval | null>`max(lag) FILTER (WHERE rn = 1)`;

/** Reads the nearest-rank pick from rows numbered `rn`, worst first, out of `n`. */
const percentileOf = (p: number) =>
  sql<IPostgresInterval | null>`max(lag) FILTER (WHERE rn = n - ceil(${sql.lit(p)} * n)::bigint + 1)`;

const PERCENTILE_COLUMNS = [
  PEAK.as("peak"),
  percentileOf(LAG_PERCENTILES.p90).as("p90"),
  percentileOf(LAG_PERCENTILES.p99).as("p99"),
  percentileOf(LAG_PERCENTILES.p99_9).as("p99_9"),
] as const;

/**
 * Each object's peak and percentiles over every reading in the range, ranked
 * against its own reading count, so an object younger than the range is
 * ranked over the readings it has. An object with no readings has no row.
 *
 * The peak comes from the same readings as the percentiles, so no percentile
 * can exceed it.
 *
 * Only each object's worst readings are ranked, as many as p90 needs. That
 * keeps the work to a Monotonic TopK per object: measured locally on
 * v26.43.0 at 24 hours and 300 objects, 560 ms against 919 ms for ranking
 * every reading.
 */
export function buildObjectLagPercentilesQuery({
  objectIds,
  lookbackMs,
}: {
  objectIds: string[];
  lookbackMs: number;
}) {
  const kept = rankFromWorst(
    LAG_PERCENTILES.p90,
    maxReadingsPerObject(lookbackMs),
  );

  return queryBuilder
    .with("readings", () => lagReadingsSince(objectIds, lookbackMs))
    .with("counts", (qb) =>
      qb
        .selectFrom("readings")
        .select((eb) => ["object_id", eb.fn.countAll().as("n")])
        .groupBy("object_id"),
    )
    .with("worst", (qb) =>
      qb
        .selectFrom("counts")
        .innerJoinLateral(
          (eb) =>
            eb
              .selectFrom("readings")
              .select("lag")
              .whereRef("readings.object_id", "=", "counts.object_id")
              .orderBy(WORST_FIRST)
              .limit(kept)
              .as("w"),
          (join) => join.onTrue(),
        )
        .select(["counts.object_id", "counts.n", "w.lag"]),
    )
    .with("ranked", (qb) =>
      qb
        .selectFrom("worst")
        .select([
          "object_id",
          "n",
          "lag",
          sql<number>`row_number() OVER (PARTITION BY object_id ORDER BY ${WORST_FIRST})`.as(
            "rn",
          ),
        ]),
    )
    .selectFrom("ranked")
    .select(["object_id as objectId", ...PERCENTILE_COLUMNS])
    .groupBy("object_id");
}

/**
 * The peak and percentiles of every reading from every object in `objectIds`,
 * ranked together. No row when none of them has a reading.
 *
 * Ranks only the worst readings p90 needs, sized from the most readings the
 * objects could have. Overestimating only keeps extra rows, because the rank
 * a percentile needs never falls as the reading count grows.
 */
export function buildScopeLagPercentilesQuery({
  objectIds,
  lookbackMs,
}: {
  objectIds: string[];
  lookbackMs: number;
}) {
  const kept = rankFromWorst(
    LAG_PERCENTILES.p90,
    objectIds.length * maxReadingsPerObject(lookbackMs),
  );

  return queryBuilder
    .with("readings", () => lagReadingsSince(objectIds, lookbackMs))
    .with("total", (qb) =>
      qb.selectFrom("readings").select((eb) => eb.fn.countAll().as("n")),
    )
    .with("worst", (qb) =>
      qb.selectFrom("readings").select("lag").orderBy(WORST_FIRST).limit(kept),
    )
    .with("ranked", (qb) =>
      qb
        .selectFrom("worst")
        .select([
          "lag",
          sql<number>`row_number() OVER (ORDER BY ${WORST_FIRST})`.as("rn"),
        ]),
    )
    .selectFrom(["ranked", "total"])
    .select(PERCENTILE_COLUMNS)
    .groupBy("total.n");
}

export async function fetchObjectLagHistory({
  objectIds,
  lookbackMs,
  queryKey,
  requestOptions,
}: {
  objectIds: string[];
  lookbackMs: number;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildObjectLagHistoryQuery({ objectIds, lookbackMs }).compile(),
    queryKey,
    requestOptions,
    sessionVariables: {
      transaction_isolation: "serializable",
    },
  });
}

export async function fetchObjectLagPercentiles({
  objectIds,
  lookbackMs,
  queryKey,
  requestOptions,
}: {
  objectIds: string[];
  lookbackMs: number;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildObjectLagPercentilesQuery({
      objectIds,
      lookbackMs,
    }).compile(),
    queryKey,
    requestOptions,
    sessionVariables: {
      transaction_isolation: "serializable",
    },
  });
}

export async function fetchScopeLagPercentiles({
  objectIds,
  lookbackMs,
  queryKey,
  requestOptions,
}: {
  objectIds: string[];
  lookbackMs: number;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildScopeLagPercentilesQuery({ objectIds, lookbackMs }).compile(),
    queryKey,
    requestOptions,
    sessionVariables: {
      transaction_isolation: "serializable",
    },
  });
}

export async function fetchLatestLag({
  objectIds,
  queryKey,
  requestOptions,
}: {
  objectIds: string[];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildLatestLagQuery(objectIds).compile(),
    queryKey,
    requestOptions,
    sessionVariables: {
      transaction_isolation: "serializable",
    },
  });
}

export function buildLagHistoryQuery(
  params: LagHistoryParameters = {
    lookback: { type: "getLatest" },
  },
) {
  const {
    objectIds,
    clusterId,
    groupByCluster,
    lookback,
    includeSystemObjects,
  } = params;

  const query = queryBuilder
    .with("lag_history_with_temporal_filter", (cte) => {
      let lagHistoryQuery = cte
        .selectFrom("mz_wallclock_global_lag_recent_history")
        .select(["occurred_at", "lag", "object_id"]);

      if (!includeSystemObjects) {
        lagHistoryQuery = lagHistoryQuery.where("object_id", "like", "u%");
      }

      // Push the entity filter to the source read, before the bin + Top-1.
      // Otherwise the cluster/object filter only enters at the final join (the
      // `lag_history` CTE), so the Top-1 over `lag_history_binned` runs over
      // every object in the environment and the join discards the rest. Applied
      // here it scopes the Top-1 to the requested objects via the
      // mz_wallclock_global_lag_recent_history object_id index.
      if (clusterId) {
        lagHistoryQuery = lagHistoryQuery.where("object_id", "in", (eb) =>
          eb
            .selectFrom("mz_objects")
            .select("id")
            .where("cluster_id", "=", clusterId),
        );
      }

      if (objectIds) {
        lagHistoryQuery = lagHistoryQuery.where("object_id", "in", objectIds);
      }

      if (lookback.type === "getLatest") {
        // Temporal filter of 2 minutes since the latest will always be within the last minute.
        // This is an optimation to do work on less records.
        lagHistoryQuery = lagHistoryQuery.where(
          (eb) => sql`${eb.ref("occurred_at")} + INTERVAL '2 MINUTES'`,
          ">=",
          sql<Date>`mz_now()`,
        );
      } else {
        lagHistoryQuery = lagHistoryQuery.where(
          (eb) =>
            sql`${eb.ref("occurred_at")} + INTERVAL '${sql.raw(`${lookback.lookbackMs}`)} MILLISECONDS'`,
          ">=",
          sql<Date>`mz_now()`,
        );
      }

      return lagHistoryQuery;
    })
    .with("lag_history_binned", (cte) => {
      const bucketSizeMs = sql.raw(
        `${lookback.type === "getLatest" ? MIN_BUCKET_SIZE_MS : calculateBucketSizeFromLookback(lookback.lookbackMs)}`,
      );

      return cte.selectFrom("lag_history_with_temporal_filter").select([
        sql<Date>`date_bin(
              '${bucketSizeMs} MILLISECONDS',
              occurred_at,
              TIMESTAMP '1970-01-01'
            )`.as("bucket_start"),
        "lag",
        "object_id",
      ]);
    })
    .with("lag_history_binned_by_max_lag", (cte) =>
      cte
        .selectFrom("lag_history_binned")
        .distinctOn(["bucket_start", "object_id"])
        .select(["bucket_start", "object_id", "lag"])
        .orderBy(["bucket_start desc", "object_id", "lag desc"]),
    )
    .with("lag_history", (cte) => {
      let cteQuery = cte
        .selectFrom("lag_history_binned_by_max_lag as lag_history")
        // TODO: We'd ideally want to inner join on a history of names such that
        // old data points still appear in our freshness UIs, similar to Grafana.
        // https://github.com/MaterializeInc/console/issues/3596
        .innerJoin(
          "mz_objects as objects",
          "lag_history.object_id",
          "objects.id",
        )
        .innerJoin(
          "mz_clusters as clusters",
          "clusters.id",
          "objects.cluster_id",
        )
        // Join fully qualified names to get names of object. This is because the new
        // built in object_history view won't provide the name in the future.
        .innerJoin(
          "mz_object_fully_qualified_names as object_names",
          "lag_history.object_id",
          "object_names.id",
        );

      // orderBy and distinctOn inherently gives us the latest max lag per ID since it takes the first row by group
      // using the order specified.
      if (lookback.type === "getLatest") {
        if (groupByCluster) {
          cteQuery = cteQuery.orderBy([
            "clusters.id",
            "bucket_start desc",
            "lag desc",
          ]);

          cteQuery = cteQuery.distinctOn(["clusters.id"]);
        } else {
          cteQuery = cteQuery.orderBy([
            "object_id",
            "bucket_start desc",
            "lag desc",
          ]);
          cteQuery = cteQuery.distinctOn(["object_id"]);
        }

        if (lookback.limit) {
          cteQuery = cteQuery.limit(sql.raw(`${lookback.limit}`));
        }
      } else if (groupByCluster) {
        cteQuery = cteQuery.orderBy([
          "bucket_start desc",
          "clusters.id",
          "lag desc",
        ]);
        cteQuery = cteQuery.distinctOn(["bucket_start", "clusters.id"]);
      }

      return cteQuery.select([
        "lag_history.bucket_start as bucketStart",
        "clusters.id as clusterId",
        "lag_history.lag",
        "lag_history.object_id as objectId",
        "clusters.name as clusterName",
        "object_names.database_name as databaseName",
        "object_names.schema_name as schemaName",
        "object_names.name as objectName",
        "objects.type as objectType",
      ]);
    })
    .selectFrom("lag_history");

  return query.selectAll().orderBy(["bucketStart asc", "lag desc"]);
}

export async function fetchLagHistory({
  params,
  queryKey,
  requestOptions,
}: {
  params: LagHistoryParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildLagHistoryQuery(params).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
    // TODO: Get rid once we implement the optimization using built in views from https://github.com/MaterializeInc/console/issues/3342
    requestTimeoutMs: 30_000,
    sessionVariables: {
      // We use serializable because we don't care about strict seriailizability and to get consistent performance
      transaction_isolation: "serializable",
    },
  });
}
