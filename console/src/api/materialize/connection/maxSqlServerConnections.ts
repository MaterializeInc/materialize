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

export function buildMaxSqlServerConnectionsQuery() {
  return sql<{
    max_sql_server_connections: string;
  }>`SHOW max_sql_server_connections`;
}

/**
 * Fetches the maximum number of SQL Server connections based on the database's
 * LaunchDarkly configuration
 */
export default async function fetchMaxSqlServerConnections({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const compiledQuery =
    buildMaxSqlServerConnectionsQuery().compile(queryBuilder);

  const response = await executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });

  let max: number | null = null;

  if (response.rows) {
    max = parseInt(response.rows[0].max_sql_server_connections);
  }

  return max;
}
