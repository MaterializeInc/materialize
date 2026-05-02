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

import { executeSqlV2, queryBuilder } from "~/api/materialize";

export const buildAllowedClusterReplicaSizesQuery = () => {
  return sql<{
    allowed_cluster_replica_sizes: string;
  }>`SHOW allowed_cluster_replica_sizes`;
};

export const buildAvailableClusterReplicaSizesQuery = () => {
  return (
    queryBuilder
      .selectFrom("mz_cluster_replica_sizes")
      .select("size")
      // We don't want to show the mz_probe size in the UI
      .where("size", "not like", "mz_%")
  );
};
/**
 * Fetches available cluster sizes based on LaunchDarkly configuration
 */
export default async function fetchAvailableClusterSizes({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const compiledQueries = [
    buildAllowedClusterReplicaSizesQuery().compile(queryBuilder),
    buildAvailableClusterReplicaSizesQuery().compile(),
  ] as const;

  let sizes: string[] | null = null;

  const [allowedSizesRes, availableSizesRes] = await executeSqlV2({
    queries: compiledQueries,
    queryKey,
    requestOptions,
  });

  if (allowedSizesRes.rows && availableSizesRes.rows) {
    // There exists cases where the allowed sizes is an empty string (i.e. self-hosted, private stacks).
    // If the allowed sizes is an empty string, we want to return all available sizes.
    const allowedSizesStr =
      allowedSizesRes.rows[0]?.allowed_cluster_replica_sizes.trim() ?? "";

    const allowedSizes =
      allowedSizesStr !== ""
        ? allowedSizesStr
            .split(",")
            .map((v: string) => v.replaceAll('"', "").trim())
        : [];

    const availableSizes = availableSizesRes.rows.map((row) => row.size);

    sizes = availableSizes.filter(
      (size) => allowedSizes.length === 0 || allowedSizes.includes(size),
    );
  }

  return sizes;
}
