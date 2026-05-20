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

export type OperatorCpuPerWorkerParams = {
  /** GlobalId of the maintained object (index, materialized view). */
  objectId: string;
  clusterName: string;
  replicaName: string;
};

/**
 * One row per (operator within this object's dataflow, worker on the selected
 * replica) with elapsed CPU since the replica started. Powers the heatmap on
 * the object detail Performance tab.
 *
 * Structural operators are filtered out — the list mirrors the one in the
 * upstream dataflow-troubleshooting prototype (Frank McSherry / docs) so the
 * remaining rows are the operators a user can actually reason about (joins,
 * arrangements, reduces, sources, sinks, etc.).
 */
export function buildOperatorCpuPerWorkerQuery(objectId: string) {
  return queryBuilder
    .selectFrom("mz_scheduling_elapsed_per_worker as mse")
    .innerJoin("mz_dataflow_operator_dataflows as dod", "dod.id", "mse.id")
    .innerJoin("mz_compute_exports as ce", "ce.dataflow_id", "dod.dataflow_id")
    .where("ce.export_id", "=", objectId)
    .where("dod.name", "not like", "Dataflow:%")
    .where("dod.name", "not like", "BuildRegion:%")
    .where("dod.name", "not like", "BuildingObject%")
    .where("dod.name", "not like", "InputRegion:%")
    .where("dod.name", "not like", "Binding(LocalId%")
    .where("dod.name", "not like", "LogOperatorHydration%")
    .where("dod.name", "!=", "Main Body")
    .select((eb) => [
      sql<string>`${sql.id("dod", "id")}::text`.as("operatorId"),
      eb.ref("dod.name").as("operatorName"),
      sql<number>`${sql.id("mse", "worker_id")}::int`.as("workerId"),
      sql<bigint>`sum(${sql.id("mse", "elapsed_ns")})::bigint`.as("elapsedNs"),
    ])
    .groupBy(["dod.id", "dod.name", "mse.worker_id"]);
}

export async function fetchOperatorCpuPerWorker({
  params,
  queryKey,
  requestOptions,
}: {
  params: OperatorCpuPerWorkerParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const query = buildOperatorCpuPerWorkerQuery(params.objectId).compile();
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

export type OperatorCpuPerWorkerRow = InferResult<
  ReturnType<typeof buildOperatorCpuPerWorkerQuery>
>[0];
