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

export type DataflowCpuPerWorkerParams = {
  clusterName: string;
  replicaName: string;
};

/**
 * One row per (dataflow on this cluster, worker on the selected replica) with
 * cumulative CPU elapsed. Powers the cluster CPU heatmap.
 *
 * This runs with the session pinned to a replica, so every relation it touches
 * is planned and executed on the customer's own cluster. Two consequences drive
 * the shape below.
 *
 * Reaching the dataflow via `mz_dataflow_addresses.address[1]`, the root of the
 * operator's address tree, avoids `mz_dataflow_operator_dataflows`, which is a
 * filter over a three-way join that re-reads the operators and addresses logs a
 * second time. Both join views drop out of the plan.
 *
 * Object names are deliberately absent. Joining `mz_objects` here would plan
 * that join on the customer's cluster rather than against the indexes on
 * `mz_catalog_server`, so the caller resolves names from the app-wide objects
 * subscribe instead.
 *
 * `elapsed_ns` is cumulative since the operator was created, never windowed, so
 * a single result is a lifetime average. Callers difference two samples
 * (`diffDataflowCpuSamples`) to get a rate.
 */
export function buildDataflowCpuPerWorkerQuery() {
  return (
    queryBuilder
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
      .innerJoin("mz_compute_exports as ce", (join) =>
        join.onRef("ce.dataflow_id", "=", "addrs.dataflowId"),
      )
      // Transient dataflows (peeks, subscribes) have ids like `t12` and vanish
      // between samples, so they would only ever add noise to the heatmap.
      .where("ce.export_id", "not like", "t%")
      .select((eb) => [
        eb.ref("ce.export_id").as("objectId"),
        sql<number>`${sql.id("mse", "worker_id")}::int`.as("workerId"),
        sql<bigint>`sum(${sql.id("mse", "elapsed_ns")})::bigint`.as(
          "elapsedNs",
        ),
      ])
      .groupBy(["ce.export_id", "mse.worker_id"])
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
  const compiledQuery = buildDataflowCpuPerWorkerQuery().compile();
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

export type DataflowCpuPerWorkerRow = InferResult<
  ReturnType<typeof buildDataflowCpuPerWorkerQuery>
>[0];
