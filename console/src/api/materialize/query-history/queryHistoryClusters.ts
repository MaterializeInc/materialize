// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult } from "kysely";

import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";

export function buildQueryHistoryClustersQuery() {
  return queryBuilder
    .selectFrom("mz_clusters as c")
    .select(["c.id", "c.name"])
    .orderBy(["c.name"]);
}

/**
 * Fetches all clusters in the query history cluster filter.
 */
export async function fetchQueryHistoryClusters({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildQueryHistoryClustersQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type QueryHistoryCluster = InferResult<
  ReturnType<typeof buildQueryHistoryClustersQuery>
>[0];
