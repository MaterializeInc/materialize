// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { isAfter, isBefore, subHours } from "date-fns";
import { InferResult, sql } from "kysely";
import { z } from "zod";

import {
  APPLICATION_NAME,
  escapedLiteral as lit,
  rawLimit,
} from "~/api/materialize";
import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";
import {
  DATE_FORMAT_SHORT,
  formatDate,
  TIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { QUERY_HISTORY_REQUEST_TIMEOUT_MS } from "./constants";
import {
  buildActivityLogTable,
  buildDurationSelection,
  buildFinishedStatusSelection,
} from "./expressionBuilders";

/**
 * The query history can be queried between now and 72 hours ago for performance and durability.
 * Any activity past 72 hours will be redacted and not shown.
 */
export const MAX_TIME_SPAN_HOURS = 72;

/*
 * We set the max selectable time span to (MAX_TIME_SPAN_HOURS - 1) hours to add
 * some buffer as a user is updating the date range filter. For example, they might set the start time to 72 hours ago but by the time they
 * click apply, the minimum start time is already outside the temporal window.
 * */
export const MAX_TIME_SPAN_BUFFERED_HOURS = MAX_TIME_SPAN_HOURS - 1;

/**
 * We have an index for the last 24 hours in the Activity log but want to use MFP pushdown when querying within the INDEX_TIME_SPAN_HOURS and MAX_TIME_SPAN_HOURS.
 * This is because the index is only used for the last 24 hours and MFP pushdown is more efficient for larger time spans.
 */
const INDEX_TIME_SPAN_HOURS = 24;

/**
 * We buffer INDEX_TIME_SPAN_HOURS by 1 hour for the same reason as MAX_TIME_SPAN_BUFFERED_HOURS.
 */
const INDEX_TIME_SPAN_BUFFERED_HOURS = INDEX_TIME_SPAN_HOURS - 1;

export const DEFAULT_TIME_SPAN_HOURS = 3;

// Arbitrary limit to prevent users from  querying too much data.
export const LIST_LIMIT = 150;

export const WILDCARD_TOKEN = "*";

export const DEFAULT_SCHEMA_VALUES = {
  dateRange: null,
  clusterId: null,
  sessionId: null,
  user: WILDCARD_TOKEN,
  statementTypes: [],
  finishedStatuses: [],
  showConsoleIntrospection: false,
  applicationName: null,
  sqlText: null,
  executionId: null,
  durationRange: {
    minDuration: null,
    maxDuration: null,
  },
  sortField: "start_time" as const,
  sortOrder: "desc" as const,
};

export const FINISHED_STATUSES = [
  "running",
  "error",
  "success",
  "canceled",
] as const;

export const QUERY_HISTORY_LIST_TABLE = "mz_recent_activity_log" as const;
export const QUERY_HISTORY_LIST_TABLE_REDACTED =
  "mz_recent_activity_log_redacted" as const;

export type FinishedStatus = (typeof FINISHED_STATUSES)[number];

export const STATEMENT_TYPES = [
  "select",
  "create",
  "drop",
  "alter",
  "subscribe",
  "explain",
] as const;

export const SORT_OPTIONS = [
  "start_time",
  "end_time",
  "duration",
  "status",
  "resultSize",
] as const;

export const queryHistoryListSchema = z.object({
  dateRange: z
    .tuple(
      [
        z.string({ required_error: "Min start time is required." }).datetime(),
        z.string({ required_error: "Max start time is required." }).datetime(),
      ],
      {
        errorMap: (issue, ctx) =>
          issue.code === "too_small"
            ? { message: "Both times are required" }
            : { message: ctx.defaultError },
      },
    )
    .nullable()
    .transform((dateRange) => {
      if (dateRange !== null) {
        return dateRange;
      }

      const currentTime = new Date();
      const defaultDateRange: [string, string] = [
        subHours(currentTime, DEFAULT_TIME_SPAN_HOURS).toISOString(),
        currentTime.toISOString(),
      ];
      return defaultDateRange;
    })
    .refine(
      (dateRange) => {
        const [startDateStr, endDateStr] = dateRange;

        const [startDate, endDate] = [
          new Date(startDateStr),
          new Date(endDateStr),
        ];

        // Ensure start date is before end date to second precision
        startDate.setMilliseconds(0);
        startDate.setSeconds(0);
        endDate.setMilliseconds(0);
        endDate.setSeconds(0);

        return isBefore(startDate, endDate);
      },
      {
        message: "Min must be before max.",
      },
    )
    .refine(
      (dateRange) => {
        const [startDateStr] = dateRange;
        const startDate = new Date(startDateStr);
        const currentTime = new Date();
        const minStartDate = subHours(currentTime, MAX_TIME_SPAN_HOURS);

        return isAfter(startDate, minStartDate);
      },
      () => {
        const currentTime = new Date();
        const minStartDate = subHours(currentTime, MAX_TIME_SPAN_HOURS);
        return {
          message: `Date range must start after ${formatDate(minStartDate, `${DATE_FORMAT_SHORT} ${TIME_FORMAT_NO_SECONDS}`)}.`,
        };
      },
    ),
  clusterId: z.string().nullable(),
  sessionId: z.string().uuid().nullable(),
  user: z.string(),
  statementTypes: z.array(z.enum(STATEMENT_TYPES)),
  finishedStatuses: z.array(z.enum(FINISHED_STATUSES)),
  showConsoleIntrospection: z.boolean(),
  applicationName: z.string().nullable(),
  sqlText: z.string().nullable(),
  executionId: z.string().uuid().nullable(),
  durationRange: z
    .object({
      minDuration: z.coerce
        .string()
        .nullable()
        .transform((arg) => (arg ? Number.parseInt(arg) : null))
        .pipe(
          z
            .number()
            .int({ message: "Lower bound must be an integer." })
            .nonnegative({
              message: "Lower bound must be a non-negative integer.",
            })
            .nullable(),
        ),
      maxDuration: z.coerce
        .string()
        .nullable()
        .transform((arg) => (arg ? Number.parseInt(arg) : null))
        .pipe(
          z
            .number()
            .int({ message: "Upper bound must be an integer." })
            .nonnegative({
              message: "Upper bound must be a non-negative integer.",
            })
            .nullable(),
        ),
    })
    .refine((durationRange) => {
      const { minDuration, maxDuration } = durationRange;
      if (minDuration && maxDuration) {
        return minDuration <= maxDuration;
      }

      return true;
    }, "The lower bound must be less than or equal to the upper bound."),
  sortField: z.enum(SORT_OPTIONS),
  sortOrder: z.enum(["asc", "desc"]),
});

export type QueryHistoryListSchema = z.infer<typeof queryHistoryListSchema>;

export type QueryHistoryListParameters = {
  filters: QueryHistoryListSchema;
  isRedacted?: boolean;
  isV0_132_0?: boolean;
};

export function buildQueryHistoryListQuery({
  filters: {
    dateRange,
    clusterId,
    sessionId,
    user,
    statementTypes,
    finishedStatuses,
    showConsoleIntrospection,
    applicationName,
    sqlText,
    executionId,
    durationRange,
    sortField,
    sortOrder,
  },
  isRedacted = false,
  isV0_132_0,
}: QueryHistoryListParameters) {
  /** Temporal window filter */
  const [startDate, endDate] = dateRange;

  // We use the indexed view if the start date is within INDEX_TIME_SPAN_BUFFERED_HOURS.
  const shouldUseIndexedView = isAfter(
    new Date(startDate),
    subHours(new Date(), INDEX_TIME_SPAN_BUFFERED_HOURS),
  );

  let qb = queryBuilder
    .selectFrom(
      buildActivityLogTable({
        showRedacted: isRedacted,
        // We assume any user before v0.132 will always use an indexed view.
        shouldUseIndexedView: isV0_132_0 ? shouldUseIndexedView : true,
      }).as("mal"),
    )
    .select((eb) => [
      "mal.application_name as applicationName",
      "mal.cluster_name as clusterName",
      "mal.execution_id as executionId",
      "mal.execution_strategy as executionStrategy",
      buildFinishedStatusSelection(eb, "mal.finished_status").as(
        "finishedStatus",
      ),
      "mal.session_id as sessionId",
      "mal.sql",
      "mal.authenticated_user as authenticatedUser",
      buildDurationSelection(eb, "mal.finished_at", "mal.began_at").as(
        "duration",
      ),
      "mal.began_at as startTime",
      "mal.finished_at as endTime",
      eb.ref("mal.rows_returned").$castTo<bigint | null>().as("rowsReturned"),
      eb.ref("mal.result_size").$castTo<bigint | null>().as("resultSize"),
      "mal.transaction_isolation as transactionIsolation",
      eb.ref("mal.throttled_count").$castTo<bigint>().as("throttledCount"),
    ]);

  // TODO: Once Kysely-codegen supports typing Timestamp fields as strings too (https://github.com/RobinBlomberg/kysely-codegen/issues/123),
  // we should remove these casts.
  qb = qb
    .where("mal.began_at", ">=", sql<Date>`${startDate}`)
    .where("mal.prepared_at", ">=", sql<Date>`${startDate}`)
    .where("mal.began_at", "<=", sql<Date>`${endDate}`)
    .where("mal.prepared_at", "<=", sql<Date>`${endDate}`);

  if (clusterId) {
    qb = qb.where("mal.cluster_id", "=", clusterId);
  }

  if (sessionId) {
    qb = qb.where("mal.session_id", "=", sessionId);
  }

  if (user !== WILDCARD_TOKEN) {
    qb = qb.where("mal.authenticated_user", "=", user);
  }

  if (statementTypes.length > 0) {
    qb = qb.where((eb) =>
      eb.or(
        statementTypes.map((statementType) =>
          eb("mal.statement_type", "ilike", lit(`${statementType}%`)),
        ),
      ),
    );
  }

  if (finishedStatuses.length > 0) {
    // Since 'running' doesn't exist in the activity log and is implemented as null, we map it to null
    const finishedStatusesCopy = [...finishedStatuses];
    const runningIndex = finishedStatusesCopy.indexOf("running");

    if (runningIndex !== -1) {
      finishedStatusesCopy.splice(runningIndex, 1);

      if (finishedStatusesCopy.length > 0) {
        qb = qb.where((eb) =>
          eb.or([
            eb("mal.finished_status", "in", finishedStatusesCopy.map(lit)),
            eb("mal.finished_status", "is", null),
          ]),
        );
      } else {
        qb = qb.where((eb) => eb("mal.finished_status", "is", null));
      }
    } else {
      qb = qb.where("mal.finished_status", "in", finishedStatuses.map(lit));
    }
  }

  if (!showConsoleIntrospection) {
    qb = qb.where("mal.application_name", "!=", APPLICATION_NAME);
  }

  if (applicationName) {
    qb = qb.where("mal.application_name", "=", applicationName);
  }

  if (sqlText) {
    // Match the sql text with a case-insensitive partial match
    const partialMatchSqlText = `%${sqlText}%`;
    qb = qb.where((eb) =>
      eb.or([
        eb(
          // Replace newlines and carriage returns with spaces. This is so that if someone copies something with newlines
          // and pastes into the search filter, it'll still match.
          sql<string>`regexp_replace(${eb.ref("sql")},'[\r\n]', ' ', 'g')`,
          "ilike",
          partialMatchSqlText,
        ),
        eb(
          // Collapse SQL text whitespace (i.e. newlines, tabs) into a single space
          // since in the query history list view, all SQL is displayed as such. Thus users are likely
          // to copy and paste the collapsed SQL version.
          sql<string>`regexp_replace(${eb.ref("sql")},'\\s+', ' ', 'g')`,
          "ilike",
          partialMatchSqlText,
        ),
      ]),
    );
  }

  if (executionId) {
    qb = qb.where("mal.execution_id", "=", executionId);
  }

  const { minDuration, maxDuration } = durationRange;

  if (minDuration !== null) {
    qb = qb.where(
      (eb) => eb("mal.finished_at", "-", eb.ref("mal.began_at")),
      ">=",
      sql<Date>`'${sql.raw(`${minDuration}`)} MILLISECONDS'`,
    );
  }

  if (maxDuration !== null) {
    qb = qb.where(
      (eb) => eb("mal.finished_at", "-", eb.ref("mal.began_at")),
      "<=",
      sql<Date>`'${sql.raw(`${maxDuration}`)} MILLISECONDS'`,
    );
  }

  qb = qb.orderBy(
    (eb) => {
      switch (sortField) {
        case "start_time":
          return eb.ref("mal.began_at");
        case "end_time":
          return eb.ref("mal.finished_at");
        case "duration":
          return eb("mal.finished_at", "-", eb.ref("mal.began_at"));
        case "status":
          return eb.ref("mal.finished_status");
        case "resultSize":
          return eb.ref("mal.result_size");
      }
    },
    sql`${sql.raw(sortOrder)} NULLS LAST`,
  );

  return rawLimit(qb, LIST_LIMIT);
}

export type QueryHistoryListRow = InferResult<
  ReturnType<typeof buildQueryHistoryListQuery>
>[0];

/**
 * Fetches history of queries and statements in the current environment
 */
export async function fetchQueryHistoryList({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: QueryHistoryListParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildQueryHistoryListQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
    requestTimeoutMs: QUERY_HISTORY_REQUEST_TIMEOUT_MS,
  });
}
