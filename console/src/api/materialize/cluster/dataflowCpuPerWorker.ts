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
  buildSessionVariables,
  executeSqlV2,
  queryBuilder,
} from "~/api/materialize";

export type DataflowCpuPerWorkerParams = {
  clusterName: string;
  replicaName: string;
};

/**
 * One row per (dataflow on this cluster, worker on the selected replica) with
 * total CPU elapsed since the replica started. Powers the cluster CPU heatmap.
 *
 * Joined paths:
 * - mz_scheduling_elapsed_per_worker x mz_dataflow_operator_dataflows: per-op
 *   CPU by worker, summed to the dataflow root (operator name `Dataflow:%`).
 * - mz_compute_exports: dataflow_id -> the GlobalId of the exported object,
 *   so each row links back to its index / materialized view / subscription.
 * - mz_objects / mz_schemas / mz_databases: human-readable identity.
 */
export function buildDataflowCpuPerWorkerQuery() {
  return (
    queryBuilder
      .selectFrom("mz_scheduling_elapsed_per_worker as mse")
      .innerJoin("mz_dataflow_operator_dataflows as dod", "dod.id", "mse.id")
      .innerJoin(
        "mz_compute_exports as ce",
        "ce.dataflow_id",
        "dod.dataflow_id",
      )
      .leftJoin("mz_objects as o", "o.id", "ce.export_id")
      .leftJoin("mz_schemas as sc", "sc.id", "o.schema_id")
      .leftJoin("mz_databases as da", "da.id", "sc.database_id")
      .where("dod.name", "like", "Dataflow:%")
      // Filter transient dataflows (peeks, subscribes) — they have ids like `t12`.
      .where("ce.export_id", "not like", "t%")
      .select((eb) => [
        eb.ref("ce.export_id").as("objectId"),
        eb.ref("o.name").as("objectName"),
        eb.ref("sc.name").as("schemaName"),
        eb.ref("da.name").as("databaseName"),
        sql<"materialized-view" | "index" | "subscription">`o.type`.as(
          "objectType",
        ),
        eb.ref("dod.dataflow_name").as("dataflowName"),
        sql<number>`${sql.id("mse", "worker_id")}::int`.as("workerId"),
        sql<bigint>`sum(${sql.id("mse", "elapsed_ns")})::bigint`.as(
          "elapsedNs",
        ),
      ])
      .groupBy([
        "ce.export_id",
        "o.name",
        "sc.name",
        "da.name",
        "o.type",
        "dod.dataflow_name",
        "mse.worker_id",
      ])
  );
}

export async function fetchDataflowCpuPerWorker({
  params,
  queryKey,
  requestOptions,
}: {
  params: DataflowCpuPerWorkerParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const query = buildDataflowCpuPerWorkerQuery().compile();
  return executeSqlV2({
    sessionVariables: buildSessionVariables({
      cluster: params.clusterName,
      cluster_replica: params.replicaName,
    }),
    queries: query,
    queryKey,
    requestOptions,
  });
}

export type DataflowCpuPerWorkerRow = InferResult<
  ReturnType<typeof buildDataflowCpuPerWorkerQuery>
>[0];
