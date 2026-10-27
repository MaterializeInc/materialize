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

import { QUERY_HISTORY_REQUEST_TIMEOUT_MS } from "./constants";
import {
  buildActivityLogTable,
  buildDurationSelection,
  buildFinishedStatusSelection,
} from "./expressionBuilders";
import { MAX_TIME_SPAN_HOURS } from "./queryHistoryList";

export const STATEMENT_LIFECYCLE_TABLE =
  "mz_statement_lifecycle_history" as const;

export function buildStatementInfoQuery({
  executionId,
  isRedacted = false,
  shouldUseIndexedView,
}: QueryHistoryStatementInfoParameters & {
  shouldUseIndexedView: boolean;
}) {
  return queryBuilder
    .selectFrom(
      buildActivityLogTable({
        showRedacted: isRedacted,
        shouldUseIndexedView,
      }).as("mal"),
    )
    .select((eb) => [
      "mal.application_name as applicationName",
      "mal.database_name as databaseName",
      "mal.search_path as searchPath",
      "mal.began_at as startTime",
      "mal.finished_at as endTime",
      "mal.cluster_name as clusterName",
      "mal.cluster_id as clusterId",
      buildDurationSelection(eb, "mal.finished_at", "mal.began_at").as(
        "duration",
      ),
      eb
        .exists(
          queryBuilder
            .selectFrom("mz_clusters as clusters")
            .where("clusters.id", "=", eb.ref("mal.cluster_id")),
        )
        .as("clusterExists"),
      "mal.mz_version as databaseVersion",
      "mal.error_message as errorMessage",
      "mal.execution_id as executionId",
      "mal.execution_strategy as executionStrategy",
      buildFinishedStatusSelection(eb, "mal.finished_status").as(
        "finishedStatus",
      ),
      "mal.session_id as sessionId",
      "mal.sql",
      "mal.authenticated_user as authenticatedUser",
      eb.ref("mal.rows_returned").$castTo<bigint | null>().as("rowsReturned"),
      eb.ref("mal.result_size").$castTo<bigint | null>().as("resultSize"),
      "mal.transaction_isolation as transactionIsolation",
      eb.ref("mal.throttled_count").$castTo<bigint>().as("throttledCount"),
    ])
    .where("execution_id", "=", executionId);
}

export type QueryHistoryStatementInfoRow = InferResult<
  ReturnType<typeof buildStatementInfoQuery>
>[0];

export function buildStatementLifecycleQuery({
  executionId,
}: {
  executionId: string;
}) {
  return queryBuilder
    .selectFrom(`${STATEMENT_LIFECYCLE_TABLE} as mslh`)
    .select([
      "mslh.statement_id as statementId",
      "mslh.event_type as eventType",
      "mslh.occurred_at as occurredAt",
    ])
    .where("statement_id", "=", executionId)
    .where(
      "occurred_at",
      ">=",
      sql.raw<Date>(`now() - INTERVAL '${MAX_TIME_SPAN_HOURS} hours'`),
    );
}

export type QueryHistoryStatementLifecycleRow = InferResult<
  ReturnType<typeof buildStatementLifecycleQuery>
>[0];

export const LIFECYCLE_EVENT_TYPES = [
  "compute-dependencies-finished",
  "execution-began",
  "execution-finished",
  "storage-dependencies-finished",
  "optimization-finished",
] as const;

export type LifecycleEventType = (typeof LIFECYCLE_EVENT_TYPES)[number];

export type QueryHistoryStatementInfoParameters = {
  executionId: string;
  isRedacted?: boolean;
};

/**
 * Fetches specific information of an execution from the activity log
 */
export async function fetchQueryHistoryStatementInfo({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: QueryHistoryStatementInfoParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildStatementInfoQuery({
      ...parameters,
      shouldUseIndexedView: true,
    }).compile(),
    queryKey: queryKey,
    requestTimeoutMs: QUERY_HISTORY_REQUEST_TIMEOUT_MS,
    requestOptions,
  });

  // If the statement lies outside the temporal window of the indexed view, then we need to fetch the data from the
  // non-indexed view
  if (res.rows.length === 0) {
    return executeSqlV2({
      queries: buildStatementInfoQuery({
        ...parameters,
        shouldUseIndexedView: false,
      }).compile(),
      queryKey: queryKey,
      requestTimeoutMs: QUERY_HISTORY_REQUEST_TIMEOUT_MS,
      requestOptions,
    });
  }

  return res;
}

export type QueryHistoryStatementLifecycleParameters = {
  executionId: string;
};

/**
 * Fetches lifecycle information of an execution
 */
export async function fetchQueryHistoryStatementLifecycle({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: QueryHistoryStatementLifecycleParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const lifecycleQuery = buildStatementLifecycleQuery({
    executionId: parameters.executionId,
  }).compile();

  return executeSqlV2({
    queries: lifecycleQuery,
    queryKey: queryKey,
    requestOptions,
    requestTimeoutMs: QUERY_HISTORY_REQUEST_TIMEOUT_MS,
  });
}
