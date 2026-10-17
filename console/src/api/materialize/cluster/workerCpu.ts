// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { queryBuilder } from "~/api/materialize";

// These read a replica's introspection relations, so they must run in a
// session pinned to that replica (`cluster` and `cluster_replica`). They then
// plan and run on that cluster, not on `mz_catalog_server`.
//
// A dataflow's root operator (an address of length one) accounts for all the
// time spent in the regions and operators it contains, so summing roots counts
// each nanosecond once. Summing every operator would count it once per level
// of nesting.

/** Each worker's cumulative time running dataflows, in nanoseconds. */
export function buildWorkerCpuQuery() {
  return queryBuilder
    .selectFrom("mz_scheduling_elapsed_per_worker as e")
    .innerJoin("mz_dataflow_addresses as a", "a.id", "e.id")
    .where(sql<boolean>`list_length(${sql.ref("a.address")}) = 1`)
    .select([
      sql<number>`${sql.ref("e.worker_id")}::int4`.as("workerId"),
      sql<number>`sum(${sql.ref("e.elapsed_ns")})::float8`.as("elapsedNs"),
    ])
    .groupBy("e.worker_id");
}

/** Each dataflow's cumulative time on each worker, in nanoseconds. */
export function buildDataflowWorkerCpuQuery() {
  return queryBuilder
    .selectFrom("mz_scheduling_elapsed_per_worker as e")
    .innerJoin("mz_dataflow_addresses as a", "a.id", "e.id")
    .where(sql<boolean>`list_length(${sql.ref("a.address")}) = 1`)
    .select([
      sql<string>`${sql.ref("a.address")}[1]::text`.as("dataflowId"),
      sql<number>`${sql.ref("e.worker_id")}::int4`.as("workerId"),
      sql<number>`${sql.ref("e.elapsed_ns")}::float8`.as("elapsedNs"),
    ]);
}

/**
 * What each dataflow maintains: user objects (`u` ids) and the temporary
 * dataflows built for ad-hoc queries and subscribes (`t` ids). System
 * dataflows still count toward each worker's total, but have nothing to name.
 */
export function buildDataflowExportsQuery() {
  return queryBuilder
    .selectFrom("mz_compute_exports")
    .select([
      "export_id as objectId",
      sql<string>`dataflow_id::text`.as("dataflowId"),
    ])
    .where((eb) =>
      eb.or([eb("export_id", "like", "u%"), eb("export_id", "like", "t%")]),
    );
}
