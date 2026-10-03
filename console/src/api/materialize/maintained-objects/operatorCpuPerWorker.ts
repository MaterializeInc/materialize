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
} from "~/api/materialize/";

export type OperatorCpuPerWorkerParams = {
  /** GlobalId of the maintained object (index, materialized view). */
  objectId: string;
  clusterName: string;
  replicaName: string;
};

/**
 * One row per (operator within this object's dataflow, worker on the selected
 * replica) with cumulative CPU elapsed. Powers the object detail Performance tab.
 *
 * Scoped to one object, so the `export_id` equality can be served by the
 * per-cluster index on `mz_compute_exports_per_worker (export_id, worker_id)`
 * that every replica maintains automatically. That makes this the cheaper of
 * the two skew queries and the better default entry point.
 *
 * `mz_scheduling_elapsed_per_worker` already groups by (id, worker_id), so
 * there is exactly one row per (operator, worker) and no aggregation is needed
 * here. As with the cluster query, `elapsed_ns` is cumulative and callers
 * difference two samples to get a rate.
 *
 * Structural operators are filtered out by name so the remaining rows are ones
 * a user can reason about. TODO: `EXPLAIN ANALYZE ... WITH SKEW` resolves
 * operators through `mz_lir_mapping` instead of by name prefix, which is both
 * more accurate and not ours to maintain. Move to it rather than growing this
 * list.
 */
export function buildOperatorCpuPerWorkerQuery(objectId: string) {
  return queryBuilder
    .selectFrom("mz_scheduling_elapsed_per_worker as mse")
    .innerJoin(
      (eb) =>
        eb
          .selectFrom("mz_dataflow_addresses")
          .select(({ ref }) => [
            "id",
            sql<number>`${ref("address")}[1]`.as("dataflowId"),
          ])
          .as("addrs"),
      (join) => join.onRef("addrs.id", "=", "mse.id"),
    )
    .innerJoin("mz_dataflow_operators as ops", (join) =>
      join.onRef("ops.id", "=", "mse.id"),
    )
    .innerJoin("mz_compute_exports as ce", (join) =>
      join.onRef("ce.dataflow_id", "=", "addrs.dataflowId"),
    )
    .where("ce.export_id", "=", objectId)
    .where("ops.name", "not like", "Dataflow:%")
    .where("ops.name", "not like", "BuildRegion:%")
    .where("ops.name", "not like", "BuildingObject%")
    .where("ops.name", "not like", "InputRegion:%")
    .where("ops.name", "not like", "Binding(LocalId%")
    .where("ops.name", "not like", "LogOperatorHydration%")
    .where("ops.name", "!=", "Main Body")
    .select((eb) => [
      sql<string>`${sql.id("ops", "id")}::text`.as("operatorId"),
      eb.ref("ops.name").as("operatorName"),
      sql<number>`${sql.id("mse", "worker_id")}::int`.as("workerId"),
      sql<bigint>`${sql.id("mse", "elapsed_ns")}::bigint`.as("elapsedNs"),
    ]);
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
  const compiledQuery = buildOperatorCpuPerWorkerQuery(
    params.objectId,
  ).compile();
  return executeSqlV2({
    sessionVariables: buildSessionVariables({
      cluster: params.clusterName,
      cluster_replica: params.replicaName,
    }),
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

export type OperatorCpuPerWorkerRow = InferResult<
  ReturnType<typeof buildOperatorCpuPerWorkerQuery>
>[0];
