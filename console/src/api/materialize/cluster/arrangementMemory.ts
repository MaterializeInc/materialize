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

import { executeSqlV2, queryBuilder } from "../";

export type ArrangmentMemoryUsageParams = {
  arrangmentIds?: string[];
  clusterName?: string;
  replicaName?: string;
  replicaHeapLimit?: number;
};

export function buildArrangmentMemoryUsageQuery({
  replicaHeapLimit,
  arrangmentIds,
}: Required<
  Pick<ArrangmentMemoryUsageParams, "replicaHeapLimit" | "arrangmentIds">
>) {
  return queryBuilder
    .selectFrom("mz_objects as o")
    .innerJoin("mz_compute_exports as ce", "ce.export_id", "o.id")
    .innerJoin(
      "mz_dataflow_arrangement_sizes as das",
      "das.id",
      "ce.dataflow_id",
    )
    .select((eb) => {
      return [
        "o.id",
        eb.ref("das.size").$castTo<bigint | null>().as("size"),
        replicaHeapLimit
          ? sql<number | null>`(${sql.id("das", "size")}::float8 / ${sql.raw(
              replicaHeapLimit.toString(),
            )}) * 100`.as("memoryPercentage")
          : sql<null>`null`.as("memoryPercentage"),
      ];
    })
    .where("o.id", "in", arrangmentIds);
}

/**
 * Fetches arrangment memory usage for a given cluster replica.
 */
export async function fetchArrangmentMemoryUsage({
  params: { clusterName, replicaName, arrangmentIds, replicaHeapLimit },
  queryKey,
  requestOptions,
}: {
  params: ArrangmentMemoryUsageParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  if (
    !clusterName ||
    !arrangmentIds ||
    !replicaHeapLimit ||
    !replicaName ||
    arrangmentIds?.length === 0
  ) {
    return null;
  }

  const compiledQuery = buildArrangmentMemoryUsageQuery({
    arrangmentIds,
    replicaHeapLimit,
  }).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
    sessionVariables: { cluster: clusterName, cluster_replica: replicaName },
  });
}

export type MemoryUsage = InferResult<
  ReturnType<typeof buildArrangmentMemoryUsageQuery>
>[0];
