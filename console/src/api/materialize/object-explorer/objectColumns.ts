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

export type ObjectExplorerColumnsParameters = {
  databaseName: string;
  schemaName: string;
  name: string;
};

export function buildObjectColumnsQuery(params: {
  databaseName: string;
  schemaName: string;
  name: string;
}) {
  let query = queryBuilder
    .selectFrom("mz_object_fully_qualified_names as ofqn")
    .innerJoin("mz_columns as c", "c.id", "ofqn.id")
    .leftJoin("mz_comments as col_comments", (join) =>
      join
        .onRef("col_comments.id", "=", "ofqn.id")
        .onRef("col_comments.object_sub_id", "=", "c.position"),
    )
    .leftJoin("mz_comments as tbl_comments", (join) =>
      join
        .onRef("tbl_comments.id", "=", "ofqn.id")
        // When object_sub_id is NULL for a relation, the comment is for the relation object itself
        .on("tbl_comments.object_sub_id", "is", null),
    )
    .select([
      "c.name",
      "c.type",
      "c.nullable",
      "col_comments.comment as columnComment",
      "tbl_comments.comment as relationComment",
    ])
    .where("ofqn.name", "=", params.name)
    .where("ofqn.schema_name", "=", params.schemaName);
  if (params.databaseName === NULL_DATABASE_NAME) {
    query = query.where("ofqn.database_name", "is", null);
  } else {
    query = query.where("ofqn.database_name", "=", params.databaseName);
  }
  return query;
}

export function fetchObjectColumns({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: ObjectExplorerColumnsParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildObjectColumnsQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type Column = InferResult<ReturnType<typeof buildObjectColumnsQuery>>[0];
