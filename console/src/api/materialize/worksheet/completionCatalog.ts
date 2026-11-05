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

import { queryBuilder } from "../db";
import { executeSqlV2 } from "../executeSqlV2";

/** Every relation that has columns, with its column names in order. */
export function buildCompletionCatalogQuery() {
  return queryBuilder
    .selectFrom("mz_object_fully_qualified_names as o")
    .innerJoin("mz_columns as c", "c.id", "o.id")
    .select([
      "o.database_name as databaseName",
      "o.schema_name as schemaName",
      "o.name",
      sql<string[]>`array_agg(c.name ORDER BY c.position)`.as("columns"),
    ])
    .groupBy(["o.database_name", "o.schema_name", "o.name"]);
}

export type CompletionCatalogRelation = InferResult<
  ReturnType<typeof buildCompletionCatalogQuery>
>[0];

export async function fetchCompletionCatalog({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const response = await executeSqlV2({
    queries: buildCompletionCatalogQuery().compile(),
    queryKey,
    requestOptions,
  });
  return response.rows;
}
