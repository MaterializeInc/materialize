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

export function buildMaxMySqlConnectionsQuery() {
  return sql<{ max_mysql_connections: string }>`SHOW max_mysql_connections`;
}

/**
 * Fetches the maximum number of MySQL connections based on the database's
 * LaunchDarkly configuration
 */
export default async function fetchMaxMySqlConnections({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const compiledQuery = buildMaxMySqlConnectionsQuery().compile(queryBuilder);

  const response = await executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });

  let max: number | null = null;

  if (response.rows) {
    max = parseInt(response.rows[0].max_mysql_connections);
  }

  return max;
}
