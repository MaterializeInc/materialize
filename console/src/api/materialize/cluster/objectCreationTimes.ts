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
import { buildIdListQuery } from "~/api/materialize/expressionBuilders";

/**
 * When each of the given objects of `objectTypes` was created, for those
 * created at or after `startDate` when given.
 *
 * NOTE: clusters, replicas, roles and schemas share the `u<N>` id form from
 * separate counters, so one id can name objects of several requested types.
 * Match rows on `objectType` as well as `id`.
 */
export function buildObjectCreationTimesQuery({
  objectIds,
  objectTypes,
  startDate,
}: {
  objectIds: string[];
  objectTypes: string[];
  startDate?: string;
}) {
  let qb = queryBuilder
    .with("ids", () => buildIdListQuery(objectIds))
    .selectFrom("mz_object_lifetimes as ol")
    .innerJoin("ids", "ids.id", "ol.id")
    .select(["ol.id", "object_type as objectType", "occurred_at as occurredAt"])
    .where("object_type", "in", objectTypes)
    .where("event_type", "=", "create");
  if (startDate) {
    qb = qb.where(
      "occurred_at",
      ">=",
      sql<Date>`${sql.lit(startDate)}::timestamptz`,
    );
  }
  return qb;
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
