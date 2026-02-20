// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ExpressionBuilder, sql, StringReference } from "kysely";

import { queryBuilder } from "~/api/materialize/db";
import {
  QUERY_HISTORY_LIST_TABLE,
  QUERY_HISTORY_LIST_TABLE_REDACTED,
} from "~/api/materialize/query-history/constants";
import { DB, Interval } from "~/types/materialize";

function buildSearchPathCommonColumn(
  eb: ExpressionBuilder<
    DB,
    "mz_recent_activity_log_redacted" | "mz_recent_activity_log"
  >,
) {
  // We need to cast the search_path to string[] because it's a custom Materialize list type,
  // which becomes typed as a string in Kysely codegen.
  return eb.ref("search_path").$castTo<string[]>().as("search_path");
}

export function buildActivityLogTable({
  showRedacted,
  shouldUseIndexedView,
}: {
  showRedacted: boolean;
  shouldUseIndexedView: boolean;
}) {
  if (shouldUseIndexedView) {
    const commonColumns = [
      "application_name",
      "authenticated_user",
      "database_name",
      "cluster_name",
      "mz_version",
      "execution_id",
      "execution_strategy",
      "finished_status",
      "session_id",
      "cluster_id",
      "statement_type",
      "transaction_isolation",
      "finished_at",
      "prepared_at",
      "began_at",
      "rows_returned",
      "result_size",
      "throttled_count",
    ] as const;
    if (showRedacted) {
      return queryBuilder
        .selectFrom(QUERY_HISTORY_LIST_TABLE_REDACTED)
        .select([
          ...commonColumns,
          (eb) => buildSearchPathCommonColumn(eb),
          (eb) => eb.lit(null).as("error_message"),
          "redacted_sql as sql",
        ]);
    }
    return queryBuilder
      .selectFrom(QUERY_HISTORY_LIST_TABLE)
      .select([
        ...commonColumns,
        (eb) => buildSearchPathCommonColumn(eb),
        "error_message",
        "sql",
      ]);
  }

  const mzActivityLogThinned = queryBuilder
    .selectFrom("mz_prepared_statement_history as mpsh")
    .innerJoin(
      "mz_session_history as msh",
      "mpsh.session_id",
      "msh.session_id",
    );
  if (showRedacted) {
    return mzActivityLogThinned
      .innerJoin(
        "mz_statement_execution_history_redacted as meh",
        "mpsh.id",
        "meh.prepared_statement_id",
      )
      .innerJoin(
        "mz_recent_sql_text_redacted as mrslt",
        "mrslt.sql_hash",
        "mpsh.sql_hash",
      )
      .select([
        "msh.initial_application_name as application_name",
        "msh.authenticated_user",
        "database_name",
        "cluster_name",
        "mz_version",
        "meh.id as execution_id",
        "execution_strategy",
        "finished_status",
        "mpsh.session_id",
        "cluster_id",
        "statement_type",
        "transaction_isolation",
        "finished_at",
        "mpsh.prepared_at",
        "began_at",
        "rows_returned",
        "result_size",
        (eb) => eb.ref("search_path").$castTo<string[]>().as("search_path"),
        (eb) => eb.lit(null).as("error_message"),
        "redacted_sql as sql",
        "throttled_count",
      ]);
  }
  return mzActivityLogThinned
    .innerJoin(
      "mz_statement_execution_history as meh",
      "mpsh.id",
      "meh.prepared_statement_id",
    )
    .innerJoin("mz_recent_sql_text as mrslt", "mrslt.sql_hash", "mpsh.sql_hash")
    .select([
      "msh.initial_application_name as application_name",
      "msh.authenticated_user",
      "database_name",
      "cluster_name",
      "mz_version",
      "meh.id as execution_id",
      "execution_strategy",
      "finished_status",
      "mpsh.session_id",
      "cluster_id",
      "statement_type",
      "transaction_isolation",
      "finished_at",
      "mpsh.prepared_at",
      "began_at",
      "rows_returned",
      "result_size",
      (eb) => eb.ref("search_path").$castTo<string[]>().as("search_path"),
      "error_message",
      "sql",
      "throttled_count",
    ]);
}

export function buildFinishedStatusSelection<DB, TB extends keyof DB>(
  eb: ExpressionBuilder<DB, TB>,
  ref: StringReference<DB, TB>,
) {
  return eb.fn.coalesce(ref, sql<string>`'running'`).$castTo<string>(); // NULL is treated as running
}

export function buildDurationSelection<DB, TB extends keyof DB>(
  eb: ExpressionBuilder<DB, TB>,
  finishedAtRef: StringReference<DB, TB>,
  beganAtRef: StringReference<DB, TB>,
) {
  return eb(finishedAtRef, "-", eb.ref(beganAtRef)).$castTo<Interval | null>(); // Kysely processes the subtraction of Timestamp types as Timestamp
}
