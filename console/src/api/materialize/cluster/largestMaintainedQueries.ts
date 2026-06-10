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
      .selectFrom("mz_dataflow_arrangement_sizes as s")
      .innerJoin("mz_compute_exports as ce", "ce.dataflow_id", "s.id")
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
          "s.id as dataflowId",
          "s.name as dataflowName",
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
