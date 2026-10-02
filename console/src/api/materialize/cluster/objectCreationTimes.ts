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

/**
 * When each of the given objects was created, for those created at or after
 * `startDate`. The literal id list makes this a lookup on
 * `mz_object_lifetimes_ind`.
 */
export function buildObjectCreationTimesQuery({
  objectIds,
  startDate,
}: {
  objectIds: string[];
  startDate: string;
}) {
  return queryBuilder
    .selectFrom("mz_object_lifetimes")
    .select(["id", "occurred_at as occurredAt"])
    .where("id", "in", objectIds)
    .where("event_type", "=", "create")
    .where("occurred_at", ">=", sql<Date>`${sql.lit(startDate)}::timestamptz`);
}

export async function fetchObjectCreationTimes({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildObjectCreationTimesQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildObjectCreationTimesQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}
