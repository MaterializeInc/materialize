// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult } from "kysely";

import { executeSqlV2, queryBuilder } from "~/api/materialize";
import { countAll, jsonArrayFrom } from "~/api/materialize/expressionBuilders";

export function buildClusterDetailsQuery() {
  return queryBuilder
    .selectFrom("mz_clusters as clusters")
    .select((eb) => [
      "clusters.name as clusterName",
      "clusters.id as clusterId",
      jsonArrayFrom<{
        id: string;
        name: string;
        size: string | null;
        disk: boolean | null;
      }>(
        eb
          .selectFrom("mz_cluster_replicas as replicas")
          .select([
            "replicas.id",
            "replicas.name",
            "replicas.size",
            "replicas.disk",
          ])
          .whereRef("replicas.cluster_id", "=", "clusters.id"),
      ).as("replicas"),
      eb
        .selectFrom("mz_sources as sources")
        .where("sources.cluster_id", "=", eb.ref("clusters.id"))
        .select(countAll())
        .as("numSources"),
      eb
        .selectFrom("mz_sinks as sinks")
        .where("sinks.cluster_id", "=", eb.ref("clusters.id"))
        .select(countAll())
        .as("numSinks"),
      eb
        .selectFrom("mz_indexes as indexes")
        .where("indexes.cluster_id", "=", eb.ref("clusters.id"))
        .where("indexes.id", "like", "u%")
        .select(countAll())
        .as("numIndexes"),
      eb
        .selectFrom("mz_materialized_views as mvs")
        .where("mvs.cluster_id", "=", eb.ref("clusters.id"))
        .select(countAll())
        .as("numMaterializedViews"),
    ])
    .where("clusters.id", "like", "u%");
}

/**
 * Fetches details about all clusters in the system for the
 * environment overview page
 */
export async function fetchClusterDetails({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildClusterDetailsQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type ClusterReplicaDetails = InferResult<
  ReturnType<typeof buildClusterDetailsQuery>
>[0];
