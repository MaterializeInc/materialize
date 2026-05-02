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

      if (clusterId) {
        cteQuery = cteQuery.where("clusters.id", "=", clusterId);
      }

      if (objectIds) {
        cteQuery = cteQuery.where("object_id", "in", objectIds);
      }

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
