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

export interface HydrationAggregateRow {
  object_id: string;
  hydratedReplicas: bigint;
  totalReplicas: bigint;
}

/**
 * One row per object: how many of its replicas report `hydrated = true`, out
 * of the total replicas recorded in `mz_hydration_statuses`
 */
export function buildHydrationAggregateQuery() {
  return queryBuilder
    .selectFrom("mz_hydration_statuses")
    .select((eb) => [
      "object_id",
      sql<bigint>`count(*) FILTER (WHERE ${eb.ref("hydrated")})`.as(
        "hydratedReplicas",
      ),
      sql<bigint>`count(*)`.as("totalReplicas"),
    ])
    .groupBy("object_id");
}
