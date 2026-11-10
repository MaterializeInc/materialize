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

/**
 * When each object of `objectTypes` on `clusterId` that was created at or
 * after `startDate`, and dropped since, was created and dropped.
 *
 * NOTE: `mz_object_history` is indexed only by id, so filtering by cluster
 * scans every object the environment has created. Measured on v26.47: 8.5 ms
 * at 32,000 objects, plus about 0.23 ms per 1,000. Looking up ids instead
 * costs about 60 µs an id, so it pays off only in much larger environments.
 */
export function buildDroppedObjectLifetimesQuery({
  clusterId,
  objectTypes,
  startDate,
}: {
  clusterId: string;
  objectTypes: string[];
  startDate: string;
}) {
  return queryBuilder
    .selectFrom("mz_object_history")
    .select([
      "id",
      "object_type as objectType",
      // Never null here: the filters on them leave out built-in objects, which
      // have no creation time, and objects not dropped.
      sql<Date>`created_at`.as("createdAt"),
      sql<Date>`dropped_at`.as("droppedAt"),
    ])
    .where("cluster_id", "=", clusterId)
    .where("object_type", "in", objectTypes)
    .where("dropped_at", "is not", null)
    .where("created_at", ">=", sql<Date>`${sql.lit(startDate)}::timestamptz`);
}

export async function fetchDroppedObjectLifetimes({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildDroppedObjectLifetimesQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildDroppedObjectLifetimesQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
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
