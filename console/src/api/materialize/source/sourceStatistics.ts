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
import { buildSourceDependenciesQuery } from "~/api/materialize/expressionBuilders";
import {
  getQueryBuilderForVersion,
  VersionMap,
} from "~/api/materialize/versioning";

import { executeSqlV2, queryBuilder } from "..";

export const COLLECTION_INTERVAL_MS = 60_000;

type SourceStatisticsArgs = {
  sourceId: string;
};

const sourceStatisticsQueryWithReplicaId = ({
  sourceId,
}: SourceStatisticsArgs) =>
  queryBuilder
    // Joins with mz_cluster_replicas to get replica name and id
    // Using LEFT JOIN because webhook sources have null replica_id
    .with("mz_source_statistics_with_history", (qb) =>
      qb
        .selectFrom("mz_source_statistics_with_history as ss")
        .leftJoin("mz_cluster_replicas as r", "r.id", "ss.replica_id")
        .where((eb) =>
          eb.or([eb("r.id", "is not", null), eb("ss.replica_id", "is", null)]),
        )
        .distinctOn("ss.id")
        .selectAll("ss")
        .select("r.name as replicaName")
        .orderBy("ss.id", "asc")
        .orderBy("ss.replica_id", "desc"),
    )
    // Represents Postgres and MySQL sources for pre and post source versioning. Also represents Kafka and Webhook sources post source versioning.
    .with("source_dependencies", () => buildSourceDependenciesQuery(sourceId))
    .with("sources", (qb) =>
      qb
        .selectFrom("mz_sources as s")
        .where("s.id", "=", sourceId)
        .select(["s.id", "s.id as sourceId"]),
    )
    .with("combined_sources", (qb) =>
      qb
        .selectFrom("source_dependencies")
        .unionAll((eb) => eb.selectFrom("sources").select(["id", "sourceId"]))
        .select(["id", "sourceId"]),
    )
    .selectFrom("combined_sources as s")
    .leftJoin(`mz_source_statistics_with_history as ss`, "ss.id", "s.id")
    .select((eb) => [
      "s.sourceId as id",
      eb.fn.min<string>("ss.replica_id").as("replicaId"),
      eb.fn.min<string>("ss.replicaName").as("replicaName"),
      eb.fn.sum<number>("messages_received").as("messagesReceived"),
      eb.fn.sum<number>("bytes_received").as("bytesReceived"),
      eb.fn.sum<number>("updates_staged").as("updatesStaged"),
      eb.fn.sum<number>("updates_committed").as("updatesCommitted"),
      eb.fn.sum<number>("snapshot_records_known").as("snapshotRecordsKnown"),
      eb.fn.sum<number>("snapshot_records_staged").as("snapshotRecordsStaged"),
      eb
        .case()
        .when(
          eb.or([
            eb(eb.fn.max("offset_known"), "is", null),
            eb(eb.fn.max("offset_committed"), "is", null),
          ]),
        )
        .then(null)
        .else(
          sql<number>`greatest(max("offset_known")::numeric - max("offset_committed")::numeric, 0)`,
        )
        .end()
        .as("offsetDelta"),
      eb
        .case()
        .when(sql`bool_or("rehydration_latency" IS NULL)`)
        .then(null)
        .else(eb.fn.max("rehydration_latency"))
        .end()
        .as("rehydrationLatency"),
    ])
    .groupBy("s.sourceId");

const legacySourceStatisticsQuery = ({ sourceId }: SourceStatisticsArgs) =>
  queryBuilder
    .with("mz_source_statistics_with_history", (qb) =>
      qb
        .selectFrom("mz_source_statistics_with_history as ss")
        .distinctOn("ss.id")
        .selectAll("ss")
        .select(sql<string>`null`.as("replicaName"))
        .orderBy("ss.id", "asc"),
    )
    .with("source_dependencies", () => buildSourceDependenciesQuery(sourceId))
    .with("sources", (qb) =>
      qb
        .selectFrom("mz_sources as s")
        .where("s.id", "=", sourceId)
        .select(["s.id", "s.id as sourceId"]),
    )
    .with("combined_sources", (qb) =>
      qb
        .selectFrom("source_dependencies")
        .unionAll((eb) => eb.selectFrom("sources").select(["id", "sourceId"]))
        .select(["id", "sourceId"]),
    )
    .selectFrom("combined_sources as s")
    .leftJoin(`mz_source_statistics_with_history as ss`, "ss.id", "s.id")
    .select((eb) => [
      "s.sourceId as id",
      sql<string>`null`.as("replicaId"),
      eb.fn.min<string>("ss.replicaName").as("replicaName"),
      eb.fn.sum<number>("messages_received").as("messagesReceived"),
      eb.fn.sum<number>("bytes_received").as("bytesReceived"),
      eb.fn.sum<number>("updates_staged").as("updatesStaged"),
      eb.fn.sum<number>("updates_committed").as("updatesCommitted"),
      eb.fn.sum<number>("snapshot_records_known").as("snapshotRecordsKnown"),
      eb.fn.sum<number>("snapshot_records_staged").as("snapshotRecordsStaged"),
      eb
        .case()
        .when(
          eb.or([
            eb(eb.fn.max("offset_known"), "is", null),
            eb(eb.fn.max("offset_committed"), "is", null),
          ]),
        )
        .then(null)
        .else(
          sql<number>`greatest(max("offset_known")::numeric - max("offset_committed")::numeric, 0)`,
        )
        .end()
        .as("offsetDelta"),
      eb
        .case()
        .when(sql`bool_or("rehydration_latency" IS NULL)`)
        .then(null)
        .else(eb.fn.max("rehydration_latency"))
        .end()
        .as("rehydrationLatency"),
    ])
    .groupBy("s.sourceId");

type SourceStatisticsResult = InferResult<
  ReturnType<typeof sourceStatisticsQueryWithReplicaId>
>[0];

const SOURCE_STATISTICS_QUERIES: VersionMap<
  SourceStatisticsArgs,
  SourceStatisticsResult
> = {
  // For versions >= v0.148.0, use replica_id filtering
  "0.148.0": sourceStatisticsQueryWithReplicaId,

  // For versions < v0.148.0, replica_id doesn't exist in mz_source_statistics
  "0.0.0": legacySourceStatisticsQuery,
};

/**
 * Higher-order function that returns the appropriate query builder for source statistics
 * based on the environment version.
 *
 * @param sourceId - The source ID to query statistics for
 * @param environmentVersion - The environment version string (e.g., "0.148.0").
 * This is automatically extracted from the query key which includes it from buildRegionQueryKey.
 * @returns The appropriate query builder for the given version
 */
export function buildSourceStatisticsQuery(
  sourceId: string,
  environmentVersion?: string,
) {
  const queryBuilderFactory = getQueryBuilderForVersion(
    environmentVersion,
    SOURCE_STATISTICS_QUERIES,
  );
  return queryBuilderFactory({ sourceId });
}

export type SourceStatisticsDataPoint = InferResult<
  ReturnType<typeof buildSourceStatisticsQuery>
>[0];

export type Source = InferResult<
  ReturnType<typeof buildSourceStatisticsQuery>
>[0];

export async function fetchSourceStatistics(
  queryKey: QueryKey,
  filters: { sourceId: string },
  requestOptions: RequestInit,
) {
  const environmentVersion = extractEnvironmentVersion(queryKey);

  const compiledQuery = buildSourceStatisticsQuery(
    filters.sourceId,
    environmentVersion,
  ).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
