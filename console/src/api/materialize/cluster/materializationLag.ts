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
  executeSqlV2,
  IPostgresInterval,
  queryBuilder,
} from "~/api/materialize";

export const ALLOWED_OBJECT_TYPES = [
  "index" as const,
  "materialized-view" as const,
  "sink" as const,
  "source" as const,
  "table" as const,
];

export type AllowedObjects = (typeof ALLOWED_OBJECT_TYPES)[0];

export type MaterializationLagParams = {
  objectIds?: string[];
};

export const OUTDATED_THRESHOLD_SECONDS = 10;

/*
  This query calculates the "lag", the time difference between frontiers, between
  target objects and their upstream dependencies.
*/
export const buildMaterializationLagQuery = ({
  objectIds,
}: Required<MaterializationLagParams>) => {
  const query = queryBuilder
    .with("materializationLag", (qb) =>
      qb
        .selectFrom(
          qb
            .selectFrom("mz_objects")
            .selectAll()
            .where("id", "in", objectIds)
            .as("objects"),
        )
        .leftJoin(
          qb.selectFrom("mz_hydration_statuses").selectAll().as("hs"),
          "hs.object_id",
          "objects.id",
        )
        .leftJoin(
          // Get the latest lag values for each object.
          // We use a temporal filter of 5 minutes because the lag values for all objects are updated every minute.
          qb
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
            .as("ml"),
          "ml.object_id",
          "objects.id",
        )
        .select([
          "objects.id as targetObjectId",
          "objects.type",
          "hs.hydrated",
          "ml.lag as lag",
        ]),
    )
    .selectFrom("materializationLag")
    .selectAll()
    .select((eb) => [
      eb(
        "lag",
        ">=",
        sql<IPostgresInterval>`INTERVAL '${sql.lit(OUTDATED_THRESHOLD_SECONDS)} seconds'`,
      ).as("isOutdated"),
    ]);

  return query;
};

export type LagResults = InferResult<
  ReturnType<typeof buildMaterializationLagQuery>
>;
type LagResult = LagResults[0];

export type LagInfo = {
  /**
   * We define 'outdated' as when an object's freshness value is greater than 10 seconds.
   * 10 seconds is arbitrary but a good guess as to when things have gone awry for an object.
   */
  isOutdated: LagResult["isOutdated"];
  hydrated: LagResult["hydrated"];
  lag: LagResult["lag"];
};

export type LagMap = Map<string, LagInfo>;

export async function fetchMaterializationLag(
  { objectIds }: MaterializationLagParams,
  queryKey: QueryKey,
  requestOptions?: RequestInit,
) {
  if (!objectIds || objectIds.length === 0) return null;

  const compiledQuery = buildMaterializationLagQuery({ objectIds }).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

/*
  This query uses buildMaterializationLag to calculate, given a list of objects,
  if they're outdated or hydrated. It also returns the source type if it's a source.

*/
export const buildBlockedDependenciesQuery = ({
  objectIds,
}: Required<MaterializationLagParams>) => {
  const materializationLagQuery = buildMaterializationLagQuery({
    objectIds,
  });

  return queryBuilder
    .selectFrom(materializationLagQuery.as("materializationLag"))
    .leftJoin(
      "mz_sources as sources",
      "materializationLag.targetObjectId",
      "sources.id",
    )
    .leftJoin(
      "mz_indexes as indexes",
      "indexes.id",
      "materializationLag.targetObjectId",
    )
    .leftJoin(
      "mz_materialized_views as mvs",
      "mvs.id",
      "materializationLag.targetObjectId",
    )
    .leftJoin("mz_clusters as clusters", (join) =>
      join.on((eb) =>
        eb(
          "clusters.id",
          "=",
          eb.fn.coalesce("indexes.cluster_id", "mvs.cluster_id"),
        ),
      ),
    )
    .select([
      "materializationLag.targetObjectId",
      "materializationLag.isOutdated",
      "materializationLag.hydrated",
      "materializationLag.type",
      "sources.type as sourceType",
      "clusters.id as clusterId",
      "clusters.name as clusterName",
    ]);
};

type BlockedDependenciesResults = InferResult<
  ReturnType<typeof buildBlockedDependenciesQuery>
>;

export type BlockedDependencyInfo = BlockedDependenciesResults[0];

export async function fetchBlockedDependencies(
  { objectIds }: MaterializationLagParams,
  queryKey: QueryKey,
  requestOptions?: RequestInit,
) {
  if (!objectIds || objectIds.length === 0) return null;

  const compiledQuery = buildBlockedDependenciesQuery({ objectIds }).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
