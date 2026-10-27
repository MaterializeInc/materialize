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

import { queryBuilder } from "./db";
import { executeSqlV2 } from "./executeSqlV2";

export const DEFAULT_DATABASE_NAME = "materialize";

function buildDatabaseListQuery() {
  return queryBuilder
    .selectFrom("mz_databases")
    .select(["id", "name"])
    .orderBy("name");
}

/**
 * Fetches all databases in the current environment
 */
export async function fetchDatabaseList({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const compiledQuery = buildDatabaseListQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type Database = InferResult<
  ReturnType<typeof buildDatabaseListQuery>
>[0];
