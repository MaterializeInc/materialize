// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";

import { executeSqlV2, queryBuilder } from "~/api/materialize";
import { buildSourceExportsQuery } from "~/api/materialize/expressionBuilders";

/**
 * The subsources and source-fed tables whose source runs on `clusterId`.
 * They are ingested on that cluster but carry no `cluster_id` of their own.
 */
export function buildClusterSourceExportsQuery(clusterId: string) {
  return queryBuilder
    .selectFrom(buildSourceExportsQuery().as("se"))
    .innerJoin("mz_sources as s", "s.id", "se.sourceId")
    .where("s.cluster_id", "=", clusterId)
    .select("se.exportId");
}

export async function fetchClusterSourceExports({
  clusterId,
  queryKey,
  requestOptions,
}: {
  clusterId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildClusterSourceExportsQuery(clusterId).compile(),
    queryKey,
    requestOptions,
  });
}
