// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { addMilliseconds, addSeconds } from "date-fns";
import PostgresInterval from "postgres-interval";

import {
  executeSqlHttp,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import {
  buildQueryHistoryListQuery,
  DEFAULT_SCHEMA_VALUES,
  queryHistoryListSchema,
} from "./queryHistoryList";

vi.mock("./constants", async (importOriginal) => {
  const constants = await importOriginal<typeof import("./constants")>();
  return {
    ...constants,
    QUERY_HISTORY_LIST_TABLE: "public.recent_activity_log",
    QUERY_HISTORY_LIST_TABLE_REDACTED: "public.recent_activity_log_redacted",
  };
});

function toSqlTimestampValue(
  value: null | undefined | Date,
  defaultValue: Date | null,
) {
  if (value === null) return "null";
  if (value === undefined && !defaultValue) return "null";
  return `'${(value ?? defaultValue)?.toISOString()}'`;
}

function toSqlStringValue(
  value: null | undefined | string,
  defaultValue: string | null,
) {
  if (value === null) return "null";
  if (value === undefined && defaultValue === null) return "null";

  value = value ?? defaultValue ?? "";
  // Escape single quotes
  value = value.replaceAll("'", "''");
  // Escape newlines
  value = value.replaceAll("\n", "\\n").replaceAll("\r", "\\r");
  // Convert into an escape string constant since testdrive considers newlines as a new command
  return `E'${value}'`;
}

function insertActivityLog(
  values: Partial<{
    executionId: string;
    clusterId: string;
    applicationName: string;
    databaseName: string;
    searchPath: string[];
    clusterName: string;
    transactionIsolation: string;
    mzVersion: string;
    beganAt: Date;
    finishedAt: Date;
    finishedStatus: string | null;
    errorMessage: string;
    rowsReturned: number;
    resultSize: number;
    throttledCount: number;
    executionStrategy: string;
    sessionId: string;
    preparedAt: Date;
    statementType: string;
    authenticatedUser: string;
    sql: string;
  }> = {},
) {
  return `INSERT INTO recent_activity_log VALUES (${[
    toSqlStringValue(
      values.executionId,
      "001197f6-5ae9-40e5-837a-181569f1f1db",
    ),
    toSqlStringValue(values.clusterId, "u1"),
    toSqlStringValue(values.applicationName, "psql"),
    toSqlStringValue(values.databaseName, "materialize"),
    values.searchPath ?? "LIST['public']",
    toSqlStringValue(values.clusterName, "quickstart"),
    toSqlStringValue(values.transactionIsolation, "strict serializable"),
    toSqlStringValue(values.mzVersion, "v0.96.2"),
    toSqlTimestampValue(values.beganAt, new Date()),
    toSqlTimestampValue(values.finishedAt, addMilliseconds(new Date(), 1)),
    toSqlStringValue(values.finishedStatus, "success"),
    toSqlStringValue(values.errorMessage, null),
    `${values.rowsReturned ?? 1}`,
    `${values.resultSize ?? 1024}`,
    `${values.throttledCount ?? 0}`,
    toSqlStringValue(values.executionStrategy, "constant"),
    toSqlStringValue(values.sessionId, "bf245c1f-a2e2-4f09-b6a2-c8bd1af7d03f"),
    toSqlTimestampValue(values.preparedAt, new Date()),
    toSqlStringValue(values.statementType, "select"),
    toSqlStringValue(values.authenticatedUser, "user@example.com"),
    toSqlStringValue(values.sql, "SELECT 1"),
  ].join(", ")});`;
}

describe("buildQueryHistoryListQuery", () => {
  it("with isRedacted: false", async () => {
    const strictQueryHistoryListSchema = queryHistoryListSchema.strict();
    await testdrive(`
        > CREATE TABLE recent_activity_log (
            execution_id UUID NOT NULL,
            cluster_id TEXT,
            application_name TEXT NOT NULL,
            database_name TEXT NOT NULL,
            search_path TEXT LIST NOT NULL,
            cluster_name TEXT,
            transaction_isolation TEXT NOT NULL,
            mz_version TEXT NOT NULL,
            began_at TIMESTAMP WITH TIME ZONE NOT NULL,
            finished_at TIMESTAMP WITH TIME ZONE,
            finished_status TEXT,
            error_message TEXT,
            rows_returned BIGINT,
            result_size BIGINT,
            throttled_count uint8,
            execution_strategy TEXT,
            session_id UUID NOT NULL,
            prepared_at TIMESTAMP WITH TIME ZONE NOT NULL,
            statement_type TEXT,
            authenticated_user TEXT NOT NULL,
            sql TEXT NOT NULL
          );
        > ${insertActivityLog()}
    `);
    {
      // default filters
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters,
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows).toEqual([
        {
          applicationName: "psql",
          authenticatedUser: "user@example.com",
          clusterName: "quickstart",
          duration: expect.any(PostgresInterval),
          endTime: expect.any(Date),
          executionId: expect.any(String),
          executionStrategy: "constant",
          finishedStatus: "success",
          rowsReturned: 1n,
          resultSize: 1024n,
          throttledCount: 0n,
          sessionId: expect.any(String),
          sql: "SELECT 1",
          startTime: expect.any(Date),
          transactionIsolation: "strict serializable",
        },
      ]);
    }
    {
      // date range
      const testStartTime = new Date();
      await testdrive(`> ${insertActivityLog({ sql: "SELECT 2" })}`, {
        noReset: true,
      });
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          dateRange: [testStartTime.toISOString(), new Date().toISOString()],
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 2"]);
    }
    {
      // user filter
      await testdrive(
        `> ${insertActivityLog({ authenticatedUser: "admin@example.com", sql: "SELECT 3" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          user: "admin@example.com",
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 3"]);
    }
    {
      // cluster filter
      await testdrive(
        `> ${insertActivityLog({ clusterId: "u2", sql: "SELECT 4" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          clusterId: "u2",
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 4"]);
    }
    {
      // session filter
      await testdrive(
        `> ${insertActivityLog({ sessionId: "bf245c1f-a2e2-4f09-b6a2-000000000000", sql: "SELECT 5" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          sessionId: "bf245c1f-a2e2-4f09-b6a2-000000000000",
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 5"]);
    }
    {
      // statement type filter
      // Ensure the statements sort correctly by adding a millisecond to the second one
      await testdrive(
        `> ${insertActivityLog({ statementType: "explain", sql: "EXPLAIN SELECT 1", beganAt: addMilliseconds(new Date(), -1) })}
         > ${insertActivityLog({ statementType: "create", sql: "CREATE TABLE t (id int)" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          statementTypes: ["explain", "create"],
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual([
        "CREATE TABLE t (id int)",
        "EXPLAIN SELECT 1",
      ]);
    }
    {
      // finished status filter
      await testdrive(
        `> ${insertActivityLog({ finishedStatus: null, sql: "SELECT 6", beganAt: addMilliseconds(new Date(), -1) })}
         > ${insertActivityLog({ finishedStatus: "error", sql: "SELECT oops" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          finishedStatuses: ["running", "error"],
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual([
        "SELECT oops",
        "SELECT 6",
      ]);
    }
    {
      // console introspection filter
      await testdrive(
        `> ${insertActivityLog({ applicationName: "web_console", sql: "SELECT 7" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          showConsoleIntrospection: true,
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toContain("SELECT 7");
    }
    {
      // application name filter
      await testdrive(
        `> ${insertActivityLog({ applicationName: "mz_psql", sql: "SELECT 8" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          applicationName: "mz_psql",
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 8"]);
    }
    {
      // SQL Text filter
      const exactMatch = "select * from mz_objects where name like '%deduped'";
      const newlinesAtEnd =
        "select * from mz_objects where name like '%deduped'\n\n\n";

      const carriageReturns =
        "select *\r\rfrom mz_objects where name like '%deduped'";

      const enclosedInParentheses =
        "SELECT (select * from mz_objects where name like '%deduped')";
      const newlinesAtBeginningAndMiddle =
        "\nSELECT * FROM mz_objects\nWHERE name LIKE '%deduped'";

      await testdrive(
        `
        > ${insertActivityLog({ sql: exactMatch })}
        > ${insertActivityLog({
          sql: newlinesAtEnd,
        })}
        > ${insertActivityLog({
          sql: carriageReturns,
        })}
        > ${insertActivityLog({ sql: enclosedInParentheses })}
        > ${insertActivityLog({
          sql: newlinesAtBeginningAndMiddle,
        })}

         `,
        {
          noReset: true,
          // If set to true, \n becomes interpolated as a newline which breaks testdrive. Setting
          // this to false keeps the \n as a literal string.
          escapeSpecialCharacters: false,
        },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          sqlText: exactMatch,
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });

      expect(result.rows.map((row) => row.sql).sort()).toEqual(
        [
          // Exact match
          exactMatch,
          // Prefix partial match
          newlinesAtEnd,
          // Match carriage returns
          carriageReturns,
          // Suffix partial match
          enclosedInParentheses,
          // In the filter, matches spaces to newlines
          newlinesAtBeginningAndMiddle,
        ].sort(),
      );
    }
    {
      // SQL Text filter with collapsed whitespace should work for SQL with multiple whitespace characters
      const sqlWithCollapsedWhitespace = "SELECT 'collapsed_whitespace_test'";

      const sqlWithMultipleNewlines = "SELECT\n\n'collapsed_whitespace_test'";

      const sqlWithMultipleSpaces = "SELECT    'collapsed_whitespace_test'";

      await testdrive(
        `
        > ${insertActivityLog({
          sql: sqlWithCollapsedWhitespace,
        })};
        > ${insertActivityLog({
          sql: sqlWithMultipleNewlines,
        })};
        > ${insertActivityLog({
          sql: sqlWithMultipleSpaces,
        })};
         `,
        {
          noReset: true,
          escapeSpecialCharacters: false,
        },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          sqlText: sqlWithCollapsedWhitespace,
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });

      expect(result.rows.map((row) => row.sql).sort()).toEqual(
        [
          sqlWithCollapsedWhitespace,
          sqlWithMultipleNewlines,
          sqlWithMultipleSpaces,
        ].sort(),
      );
    }
    {
      // execution ID filter
      await testdrive(
        `> ${insertActivityLog({ executionId: "001197f6-5ae9-40e5-837a-000000000000", sql: "SELECT 9" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          executionId: "001197f6-5ae9-40e5-837a-000000000000",
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 9"]);
    }
    {
      // min duration filters
      const beganAt = new Date();
      await testdrive(
        `> ${insertActivityLog({ beganAt, finishedAt: addSeconds(beganAt, 2), sql: "SELECT 10" })}
         > ${insertActivityLog({ beganAt, finishedAt: addSeconds(beganAt, 3), sql: "SELECT 11" })}`,
        { noReset: true },
      );
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          durationRange: { minDuration: 1000, maxDuration: null },
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 10", "SELECT 11"]);
    }
    {
      // max duration filters
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          durationRange: { minDuration: null, maxDuration: 0 },
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual([]);
    }
    {
      // min and max duration filters
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          durationRange: { minDuration: 1000, maxDuration: 2000 },
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 10"]);
    }
    {
      // sort field and order
      const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
      const query = buildQueryHistoryListQuery({
        filters: {
          ...filters,
          durationRange: { minDuration: 1000, maxDuration: null },
          sortOrder: "desc",
          sortField: "duration",
        },
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: { cluster: QUICKSTART_CLUSTER },
      });
      expect(result.rows.map((r) => r.sql)).toEqual(["SELECT 11", "SELECT 10"]);
    }
  }, 30_000);

  it("with isRedacted: true", async () => {
    const strictQueryHistoryListSchema = queryHistoryListSchema.strict();
    await testdrive(`
        > CREATE TABLE recent_activity_log_redacted (
            execution_id UUID NOT NULL,
            cluster_id TEXT,
            application_name TEXT NOT NULL,
            database_name TEXT NOT NULL,
            search_path TEXT LIST NOT NULL,
            cluster_name TEXT,
            transaction_isolation TEXT NOT NULL,
            mz_version TEXT NOT NULL,
            began_at TIMESTAMP WITH TIME ZONE NOT NULL,
            finished_at TIMESTAMP WITH TIME ZONE,
            finished_status TEXT,
            throttled_count uint8,
            rows_returned BIGINT,
            result_size BIGINT,
            execution_strategy TEXT,
            session_id UUID NOT NULL,
            prepared_at TIMESTAMP WITH TIME ZONE NOT NULL,
            statement_type TEXT,
            authenticated_user TEXT NOT NULL,
            redacted_sql TEXT NOT NULL
          );
        > INSERT INTO recent_activity_log_redacted VALUES (
            '001197f6-5ae9-40e5-837a-181569f1f1db',
            'u1',
            'psql',
            'materialize',
            LIST['public'],
            'quickstart',
            'strict serializable',
            'v0.96.2',
            '${new Date().toISOString()}',
            '${new Date().toISOString()}',
            'success',
            0,
            1,
            1024,
            'constant',
            'bf245c1f-a2e2-4f09-b6a2-c8bd1af7d03f',
            '${new Date().toISOString()}',
            'select',
            'admin@example.com',
            'SELECT ''<REDACTED>'''
          );
    `);
    const filters = strictQueryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES);
    const query = buildQueryHistoryListQuery({
      filters,
      isRedacted: true,
    }).compile();
    const result = await executeSqlHttp(query, {
      sessionVariables: { cluster: QUICKSTART_CLUSTER },
    });
    expect(result.rows.map((r) => r.sql)).toEqual(["SELECT '<REDACTED>'"]);
  });
});
