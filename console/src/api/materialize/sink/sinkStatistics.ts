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

import { extractEnvironmentVersion } from "~/api/buildQueryKeySchema";
import {
  getQueryBuilderForVersion,
  VersionMap,
} from "~/api/materialize/versioning";

import { executeSqlV2, queryBuilder } from "..";

export const COLLECTION_INTERVAL_MS = 60_000;

type SinkStatisticsArgs = {
  sinkId: string;
};

/**
 * Returns a query for the statistics of a single sink.
 * For sinks on multi-replica clusters, this query uses DISTINCT ON to select
 * statistics from only the first replica, sorted alphabetically (e.g., 'r1').
 */
const sinkStatisticsQueryWithReplicaId = ({ sinkId }: SinkStatisticsArgs) =>
  queryBuilder
    // Joins with mz_cluster_replicas to get replica name and id
    .with("mz_sink_statistics_with_replicas", (qb) =>
      qb
        .selectFrom("mz_sink_statistics as ss")
        .innerJoin("mz_cluster_replicas as r", "r.id", "ss.replica_id")
        .distinctOn("ss.id")
        .selectAll("ss")
        .select("r.name as replicaName")
        .orderBy("ss.id", "asc")
        .orderBy("ss.replica_id", "desc"),
    )
    .with("sinks", (qb) =>
      qb
        .selectFrom("mz_sinks as s")
        .where("s.id", "=", sinkId)
        .select(["s.id", "s.id as sinkId"]),
    )
    .selectFrom("sinks as s")
    .innerJoin(`mz_sink_statistics_with_replicas as ss`, "ss.id", "s.id")
    .select((eb) => [
      "s.sinkId as id",
      eb.fn.min<string>("ss.replica_id").as("replicaId"),
      eb.fn.min<string>("ss.replicaName").as("replicaName"),
      eb.fn.sum<number>("messages_staged").as("messagesStaged"),
      eb.fn.sum<number>("messages_committed").as("messagesCommitted"),
      eb.fn.sum<number>("bytes_staged").as("bytesStaged"),
      eb.fn.sum<number>("bytes_committed").as("bytesCommitted"),
    ])
    .groupBy("s.sinkId");

const legacySinkStatisticsQuery = ({ sinkId }: SinkStatisticsArgs) =>
  queryBuilder

    .with("mz_sink_statistics_with_replicas", (qb) =>
      qb
        .selectFrom("mz_sink_statistics as ss")
        .distinctOn("ss.id")
        .selectAll("ss")
        .select(sql<string>`null`.as("replicaName"))
        .orderBy("ss.id", "asc"),
    )
    .with("sinks", (qb) =>
      qb
        .selectFrom("mz_sinks as s")
        .where("s.id", "=", sinkId)
        .select(["s.id", "s.id as sinkId"]),
    )
    .selectFrom("sinks as s")
    .innerJoin(`mz_sink_statistics_with_replicas as ss`, "ss.id", "s.id")
    .select((eb) => [
      "s.sinkId as id",
      sql<string>`null`.as("replicaId"),
      eb.fn.min<string>("ss.replicaName").as("replicaName"),
      eb.fn.sum<number>("messages_staged").as("messagesStaged"),
      eb.fn.sum<number>("messages_committed").as("messagesCommitted"),
      eb.fn.sum<number>("bytes_staged").as("bytesStaged"),
      eb.fn.sum<number>("bytes_committed").as("bytesCommitted"),
    ])
    .groupBy("s.sinkId");

type SinkStatisticsResult = InferResult<
  ReturnType<typeof sinkStatisticsQueryWithReplicaId>
>[0];

const SINK_STATISTICS_QUERIES: VersionMap<
  SinkStatisticsArgs,
  SinkStatisticsResult
> = {
  // For versions >= v0.148.0, use replica_id filtering
  "0.148.0": sinkStatisticsQueryWithReplicaId,

  // For versions < v0.148.0, replica_id doesn't exist in mz_sink_statistics
  "0.0.0": legacySinkStatisticsQuery,
};

/**
 * Higher-order function that returns the appropriate query builder for sink statistics
 * based on the environment version.
 *
 * @param sinkId - The sink ID to query statistics for
 * @param environmentVersion - The environment version string (e.g., "0.148.0").
 * This is automatically extracted from the query key which includes it from buildRegionQueryKey.
 * @returns The appropriate query builder for the given version
 */
export function buildSinkStatisticsQuery(
  sinkId: string,
  environmentVersion?: string,
) {
  const queryBuilderFactory = getQueryBuilderForVersion(
    environmentVersion,
    SINK_STATISTICS_QUERIES,
  );
  return queryBuilderFactory({ sinkId });
}

export type SinkStatisticsDataPoint = InferResult<
  ReturnType<typeof buildSinkStatisticsQuery>
>[0];

export type Sink = InferResult<ReturnType<typeof buildSinkStatisticsQuery>>[0];

export async function fetchSinkStatistics(
  queryKey: QueryKey,
  filters: { sinkId: string },
  requestOptions: RequestInit,
) {
  const environmentVersion = extractEnvironmentVersion(queryKey);

  const compiledQuery = buildSinkStatisticsQuery(
    filters.sinkId,
    environmentVersion,
  ).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
