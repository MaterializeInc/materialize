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

export const buildMaxReplicasPerClusterQuery = () => {
  return sql<{
    max_replicas_per_cluster: string;
  }>`SHOW max_replicas_per_cluster`;
};

/**
 * Fetches the maximum number replicas per cluster based on LaunchDarkly configuration
 */
export default async function fetchMaxReplicasPerCluster({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const compiledQuery = buildMaxReplicasPerClusterQuery().compile(queryBuilder);

  const response = await executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });

  let max: number | null = null;

  if (response.rows) {
    max = parseInt(response.rows[0].max_replicas_per_cluster);
  }

  return max;
}
