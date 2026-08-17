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
import { InferResult, sql } from "kysely";

import { executeSqlV2, queryBuilder } from "~/api/materialize";
import {
  buildClusterReplicaUtilizationTable,
  getOwners,
  jsonArrayFrom,
} from "~/api/materialize/expressionBuilders";

/**
 * Replica utilization percentages, guaranteed one row per replica.
 *
 * The shared utilization builder groups by (replica_id, process_id), which is
 * the key of the underlying view, so it emits a row per process. Joining that
 * straight into the replica list would duplicate every multi-process replica
 * and inflate the replica count, so collapse the processes here.
 *
 * Dividing by the row count rather than using AVG mirrors the shared builder:
 * an offline process still has a row, with null metrics, and is meant to drag
 * the replica's number down rather than be skipped. Averaging percentages is
 * only sound because every process of a replica has the same allocation, so
 * the per-process denominators are identical.
 */
function buildReplicaUtilization() {
  return queryBuilder
    .selectFrom(buildClusterReplicaUtilizationTable().as("cru"))
    .groupBy("cru.replica_id")
    .select([
      "cru.replica_id as replicaId",
      sql<number | null>`SUM(cru.cpu_percent) / COUNT(*)`.as("cpuPercent"),
      sql<number | null>`SUM(cru.memory_percent) / COUNT(*)`.as(
        "memoryPercent",
      ),
      sql<number | null>`SUM(cru.disk_percent) / COUNT(*)`.as("diskPercent"),
    ]);
}

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
      "c.owner_id as ownerId",
      jsonArrayFrom<{
        id: string;
        name: string;
        size: string | null;
        disk: boolean | null;
        cpuPercent: number | null;
        memoryPercent: number | null;
        diskPercent: number | null;
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
          // A left join keeps replicas that have no metrics rows yet. The
          // cluster detail query inner joins here, which drops them.
          .leftJoin(
            buildReplicaUtilization().as("cru"),
            "cru.replicaId",
            "cr.id",
          )
          .select((replicaEb) => [
            "cr.id",
            "cr.name",
            "cr.size",
            "cr.disk",
            "cru.cpuPercent",
            "cru.memoryPercent",
            "cru.diskPercent",
            jsonArrayFrom(
              replicaEb
                .selectFrom("mz_cluster_replica_statuses as crs_inner")
                .select([
                  "crs_inner.replica_id",
                  "crs_inner.process_id",
                  "crs_inner.status",
                  "crs_inner.reason",
                  "crs_inner.updated_at",
                ])
                .whereRef("crs_inner.replica_id", "=", "cr.id"),
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
