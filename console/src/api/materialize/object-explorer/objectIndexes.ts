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

import { NULL_DATABASE_NAME } from "~/platform/object-explorer/constants";

import { queryBuilder } from "../db";
import { executeSqlV2 } from "../executeSqlV2";

export type ObjectIndexesParameters = {
  databaseName: string;
  schemaName: string;
  name: string;
};

export function buildObjectIndexesQuery(params: ObjectIndexesParameters) {
  let query = queryBuilder
    .selectFrom("mz_object_fully_qualified_names as objectNames")
    .innerJoin("mz_indexes as indexes", "indexes.on_id", "objectNames.id")
    .innerJoin(
      "mz_object_fully_qualified_names as indexNames",
      "indexNames.id",
      "indexes.id",
    )
    // System objects don't have audit events
    .leftJoin("mz_object_lifetimes as lifetimes", (join) =>
      join
        .onRef("lifetimes.id", "=", "indexNames.id")
        .on("lifetimes.event_type", "=", "create"),
    )
    .innerJoin("mz_roles as roles", "roles.id", "indexes.owner_id")
    .innerJoin("mz_show_indexes as showIndexes", (join) =>
      join
        .onRef("showIndexes.name", "=", "indexNames.name")
        .onRef("showIndexes.schema_id", "=", "indexNames.schema_id"),
    )
    .select([
      "indexNames.id",
      "indexNames.name",
      "indexNames.database_name as databaseName",
      "indexNames.schema_name as schemaName",
      "roles.name as owner",
      "lifetimes.occurred_at as createdAt",
      "showIndexes.key as indexedColumns",
    ])
    .where("objectNames.name", "=", params.name)
    .where("objectNames.schema_name", "=", params.schemaName);
  if (params.databaseName === NULL_DATABASE_NAME) {
    query = query.where("objectNames.database_name", "is", null);
  } else {
    query = query.where("objectNames.database_name", "=", params.databaseName);
  }
  return query;
}

export function fetchObjectIndexes({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: ObjectIndexesParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildObjectIndexesQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type ObjectIndex = InferResult<
  ReturnType<typeof buildObjectIndexesQuery>
>[0];
