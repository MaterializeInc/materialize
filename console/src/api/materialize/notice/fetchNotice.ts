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

export type DetailFilters = {
  key: string;
};

function buildNoticeQuery({ key }: DetailFilters) {
  const qb = queryBuilder
    .selectFrom("mz_notices_redacted as n")
    .leftJoin("mz_objects as o", "o.id", "n.object_id")
    .leftJoin("mz_schemas as s", "s.id", "o.schema_id")
    .leftJoin("mz_databases as d", "d.id", "s.database_id")
    .select([
      "n.notice_type as noticeType",
      "n.message",
      "n.hint",
      "n.action",
      "n.action_type as actionType",
      "n.object_id as objectId",
      "n.created_at as createdAt",
      "o.name as objectName",
      "s.name as schemaName",
      "d.name as databaseName",
    ])
    .where(
      sql<string>`md5(concat(
        d.name::text, '.', s.name::text, '.', o.name::text,
        ':',
        n.message::text
      ))`,
      "=",
      key,
    );

  return qb;
}

/**
 * Fetches details about a notice.
 */
export async function fetchNoticeDetail({
  filters,
  queryKey,
  requestOptions,
}: {
  filters: DetailFilters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildNoticeQuery(filters).compile(),
    queryKey: queryKey,
    requestOptions,
  });
}

export type DetailsNotice = InferResult<ReturnType<typeof buildNoticeQuery>>[0];
