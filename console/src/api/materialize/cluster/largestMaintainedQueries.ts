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
  buildSessionVariables,
  executeSqlV2,
  queryBuilder,
} from "~/api/materialize";

export type LargestMaintainedQueriesParams = {
  replicaHeapLimit: number;
  limit: number;
  clusterName: string;
  replicaName: string;
};

export function buildLargestMaintainedQueriesQuery({
  replicaHeapLimit,
  limit,
}: Omit<LargestMaintainedQueriesParams, "replicaName" | "clusterName">) {
  return (
    queryBuilder
      // Per-dataflow arrangement sizes, equivalent to mz_dataflow_arrangement_sizes
      // restricted to `size`: scanning only the two size logs avoids the view's
      // seven other per-operator log aggregations.
      .with("per_operator", (qb) =>
        qb
          .selectFrom((eb) =>
            eb
              .selectFrom("mz_arrangement_heap_size_raw")
              .select("operator_id")
              .unionAll(
                eb
                  .selectFrom("mz_arrangement_batcher_size_raw")
                  .select("operator_id"),
              )
              .as("raw_logs"),
          )
          .select((eb) => ["operator_id", eb.fn.countAll<bigint>().as("size")])
          .groupBy("operator_id"),
      )
      .with("per_dataflow", (qb) =>
        qb
          .selectFrom("per_operator as po")
          .innerJoin(
            "mz_dataflow_operator_dataflows as mdod",
            "mdod.id",
            "po.operator_id",
          )
          .select([
            "mdod.dataflow_id",
            "mdod.dataflow_name",
            sql<bigint>`sum(${sql.ref("po.size")})::int8`.as("size"),
          ])
          .groupBy(["mdod.dataflow_id", "mdod.dataflow_name"]),
      )
      .selectFrom("per_dataflow as s")
      .innerJoin("mz_compute_exports as ce", "ce.dataflow_id", "s.dataflow_id")
      .leftJoin("mz_objects as o", "o.id", "ce.export_id")
      .leftJoin("mz_schemas as sc", "sc.id", "o.schema_id")
      .leftJoin("mz_databases as da", "da.id", "sc.database_id")
      .select((eb) => {
        return [
          "o.id",
          "o.name",
          eb.ref("s.size").$castTo<bigint | null>().as("size"),
          replicaHeapLimit
            ? sql<number | null>`(${sql.id("s", "size")}::float8 / ${sql.raw(
                replicaHeapLimit.toString(),
              )}) * 100`.as("memoryPercentage")
            : sql<null>`null`.as("memoryPercentage"),
          sql<"materialized-view" | "index">`o.type`.as("type"),
          "sc.name as schemaName",
          "da.name as databaseName",
          eb.ref("s.dataflow_id").$castTo<string | null>().as("dataflowId"),
          eb.ref("s.dataflow_name").$castTo<string | null>().as("dataflowName"),
        ];
      })
      // Filter out transient dataflows
      .where("ce.export_id", "not like", "t%")
      .orderBy("memoryPercentage", sql`desc NULLS LAST`)
      .limit(() => sql.raw(limit.toString()))
  );
}

/**
 * Fetches the largest cluster replica for a given cluster.
 */
export async function fetchLargestMaintainedQueries({
  params,
  queryKey,
  requestOptions,
}: {
  params: LargestMaintainedQueriesParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const query = buildLargestMaintainedQueriesQuery(params);
  return executeSqlV2({
    sessionVariables: buildSessionVariables({
      cluster: params.clusterName,
      cluster_replica: params.replicaName,
    }),
    queries: query.compile(),
    queryKey: queryKey,
    requestOptions,
  });
}

export type LargestMaintainedObjectSizesParams = {
  clusterId: string;
  replicaName: string;
  replicaHeapLimit: number;
  limit: number;
};

/**
 * Sizes of the largest maintained objects on a replica, from
 * `mz_object_arrangement_sizes` (Materialize >= v26.35, where sizes stay
 * correct across replica restarts). Served by the collection's replica_id
 * index on mz_catalog_server: no cluster or replica session variables, and
 * no introspection dataflows on the replica being inspected.
 */
export function buildLargestMaintainedObjectSizesQuery({
  clusterId,
  replicaName,
  replicaHeapLimit,
  limit,
}: LargestMaintainedObjectSizesParams) {
  return (
    queryBuilder
      .selectFrom("mz_object_arrangement_sizes as s")
      .innerJoin("mz_cluster_replicas as cr", (join) =>
        join
          .onRef("cr.id", "=", "s.replica_id")
          .on("cr.cluster_id", "=", clusterId)
          .on("cr.name", "=", replicaName),
      )
      .select((eb) => [
        "s.object_id",
        eb.ref("s.size").$castTo<bigint | null>().as("size"),
        replicaHeapLimit
          ? sql<number | null>`(${sql.id("s", "size")}::float8 / ${sql.raw(
              replicaHeapLimit.toString(),
            )}) * 100`.as("memoryPercentage")
          : sql<null>`null`.as("memoryPercentage"),
      ])
      // Ties are common because sizes are 10 MiB-quantized; break them by id so
      // rows keep a stable order across polls.
      .orderBy("size", sql`desc NULLS LAST`)
      .orderBy("s.object_id")
      .limit(() => sql.raw(limit.toString()))
  );
}

export async function fetchLargestMaintainedObjectSizes({
  params,
  queryKey,
  requestOptions,
}: {
  params: LargestMaintainedObjectSizesParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const query = buildLargestMaintainedObjectSizesQuery(params);
  return executeSqlV2({
    queries: query.compile(),
    queryKey: queryKey,
    requestOptions,
  });
}

/**
 * Display names for a fixed, non-empty set of object ids. Callers cache by
 * id set: names are stable, so this only needs to run when the set changes.
 */
export function buildMaintainedObjectNamesQuery(objectIds: string[]) {
  return queryBuilder
    .selectFrom("mz_objects as o")
    .leftJoin("mz_schemas as sc", "sc.id", "o.schema_id")
    .leftJoin("mz_databases as da", "da.id", "sc.database_id")
    .select([
      "o.id",
      "o.name",
      sql<"materialized-view" | "index">`o.type`.as("type"),
      "sc.name as schemaName",
      "da.name as databaseName",
    ])
    .where("o.id", "in", objectIds);
}

export async function fetchMaintainedObjectNames({
  objectIds,
  queryKey,
  requestOptions,
}: {
  objectIds: string[];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const query = buildMaintainedObjectNamesQuery(objectIds);
  return executeSqlV2({
    queries: query.compile(),
    queryKey: queryKey,
    requestOptions,
  });
}
