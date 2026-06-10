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

import { MAX_TIME_SPAN_HOURS } from "./queryHistoryList";

export function buildQueryHistoryUsersQuery() {
  return (
    queryBuilder
      .selectFrom("mz_session_history as msh")
      .select("msh.authenticated_user as email")
      .distinct()
      // Given mz_session_history can be very large, we apply a temporal window to
      // avoid CPU spikes. This however implies we only get users that have issued
      // queries in the last MAX_TIME_SPAN_HOURS, which is sufficient since the
      // query history result set is bounded by that time frame too.
      // TODO: Replace with pg_roles WHERE rolcanlogin https://github.com/MaterializeInc/console/issues/2402
      .where(
        "connected_at",
        ">=",
        sql<Date>`now() - INTERVAL '${sql.raw(`${MAX_TIME_SPAN_HOURS}`)} HOURS'`,
      )
      .orderBy(["email"])
  );
}

/**
 * Fetches all users in the query history user filter.
 */
export async function fetchQueryHistoryUsers({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildQueryHistoryUsersQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type QueryHistoryUser = InferResult<
  ReturnType<typeof buildQueryHistoryUsersQuery>
>[0];
