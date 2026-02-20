// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { QueryKey } from "@tanstack/react-query";
import { InferResult } from "kysely";

import { executeSqlV2, queryBuilder } from "~/api/materialize";
import { getOwners, jsonArrayFrom } from "~/api/materialize/expressionBuilders";

export type ClusterListFilters = {
  queryOwnership?: boolean;
  includeSystemObjects: boolean;
};

export const buildClustersQuery = ({
  queryOwnership = true,
  includeSystemObjects = true,
}: ClusterListFilters) => {
  const latestClusterStatusUpdate = queryBuilder
    .selectFrom("mz_clusters as c")
    .leftJoin("mz_cluster_replica_history as crh", "crh.cluster_id", "c.id")
    .leftJoin(
      "mz_cluster_replica_status_history as crsh",
      "crh.replica_id",
      "crsh.replica_id",
    )
    .select([
      "c.id as cluster_id",
      (eb) =>
        eb.fn
          .max("crsh.occurred_at")
          .$castTo<string>()
          .as("latest_status_update"),
    ])
    .groupBy("c.id");

  let qb = queryBuilder
    .selectFrom("mz_clusters as c")
    .innerJoin(
      latestClusterStatusUpdate.as("latest_cluster_status_update"),
      "latest_cluster_status_update.cluster_id",
      "c.id",
    )
    .$if(queryOwnership, (query) =>
      query
        .innerJoin(getOwners().as("owners"), "owners.id", "c.owner_id")
        .select("owners.isOwner"),
    )
    .select((eb) => [
      "c.id",
      "c.name",
      "c.disk",
      "c.managed",
      "c.size",
      jsonArrayFrom<{
        id: string;
        name: string;
        size: string | null;
        disk: boolean | null;
        statuses: {
          replica_id: string;
          process_id: string;
          reason: string | null;
          status: string;
          updated_at: string;
        }[];
      }>(
        eb
          .selectFrom("mz_cluster_replicas as cr")
          .select([
            "cr.id",
            "cr.name",
            "cr.size",
            "cr.disk",
            jsonArrayFrom(
              eb
                .selectFrom("mz_cluster_replica_statuses as crs_inner")
                .select([
                  "crs_inner.replica_id",
                  "crs_inner.process_id",
                  "crs_inner.status",
                  "crs_inner.reason",
                  "crs_inner.updated_at",
                ])
                .whereRef("crs_inner.replica_id", "=", "c.id"),
            ).as("statuses"),
          ])
          .whereRef("cr.cluster_id", "=", "c.id")
          .orderBy("cr.id"),
      ).as("replicas"),
      "latest_cluster_status_update.latest_status_update as latestStatusUpdate",
    ])
    .orderBy("c.name");

  if (!includeSystemObjects) {
    qb = qb.where("c.id", "like", "u%");
  }

  return qb;
};

/**
 * Fetches all clusters with their replicas in the current environment.
 */
export async function fetchClusters({
  filters,
  queryKey,
  requestOptions,
}: {
  filters: ClusterListFilters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildClustersQuery({
    ...filters,
    queryOwnership: true,
  }).compile();

  return Sentry.startSpan(
    {
      name: "fetchClusters",
      op: "http.client",
    },
    () => {
      return executeSqlV2({
        queries: compiledQuery,
        queryKey: queryKey,
        requestOptions,
      });
    },
  );
}

export type ClusterWithOwnership = InferResult<
  ReturnType<typeof buildClustersQuery>
>[0];
export type Cluster = Omit<
  InferResult<ReturnType<typeof buildClustersQuery>>[0],
  "isOwner"
>;
export type Replica = Cluster["replicas"][0];
