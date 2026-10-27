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

import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";

export type ListFilters = {
  databaseId?: string;
  schemaId?: string;
  nameFilter?: string;
};

function buildNoticesListQuery({
  databaseId,
  schemaId,
  nameFilter,
}: ListFilters) {
  let qb = queryBuilder
    .selectFrom("mz_notices_redacted as n")
    .leftJoin("mz_objects as o", "o.id", "n.object_id")
    .leftJoin("mz_schemas as s", "s.id", "o.schema_id")
    .leftJoin("mz_databases as d", "d.id", "s.database_id")
    .select([
      "n.notice_type as noticeType",
      "n.object_id as objectId",
      "o.name as objectName",
      "s.name as schemaName",
      "d.name as databaseName",
      "n.created_at as createdAt",
      sql<string>`md5(concat(
        d.name::text, '.', s.name::text, '.', o.name::text,
        ':',
        n.message::text
      ))`.as("key"),
    ])
    .orderBy(["n.created_at"]);
  if (databaseId) {
    qb = qb.where("d.id", "=", databaseId);
  }
  if (schemaId) {
    qb = qb.where("s.id", "=", schemaId);
  }
  if (nameFilter) {
    qb = qb.where("n.notice_type", "like", `%${nameFilter}%`);
  }
  return qb;
}

/**
 * Fetches all notices that satisfy the given filters.
 */
export async function fetchNoticesList({
  filters,
  queryKey,
  requestOptions,
}: {
  filters: ListFilters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildNoticesListQuery(filters).compile(),
    queryKey: queryKey,
    requestOptions,
  });
}

export type ListPageNotice = InferResult<
  ReturnType<typeof buildNoticesListQuery>
>[0];
