// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import PostgresInterval from "postgres-interval";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { STATEMENT_LIFECYCLE_TABLE } from "~/api/materialize/query-history/queryHistoryDetail";
import {
  DEFAULT_SCHEMA_VALUES,
  QUERY_HISTORY_LIST_TABLE,
  QUERY_HISTORY_LIST_TABLE_REDACTED,
  queryHistoryListSchema,
} from "~/api/materialize/query-history/queryHistoryList";
import {
  Column,
  ColumnMetadata,
  ErrorCode,
  MzDataType,
} from "~/api/materialize/types";
import { buildMockPrivilegesQueryHandler } from "~/api/mocks/buildMockPrivilegesQueryHandler";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { cancelQueryQueryKeys } from "~/queries/cancelQuery";
import { buildQueryClient } from "~/queryClient";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { formatDate, FRIENDLY_DATETIME_FORMAT } from "~/utils/dateFormat";

import { queryHistoryQueryKeys, useFetchQueryHistoryList } from "./queries";
import QueryHistoryDetail from "./QueryHistoryDetail";

const useFetchQueryHistoryStatementInfoColumns: Array<Column> = [
  buildColumn({ name: "applicationName" }),
  buildColumn({ name: "databaseName" }),
  buildColumn({ name: "searchPath", type_oid: MzDataType._text }),
  buildColumn({ name: "startTime", type_oid: MzDataType.timestamptz }),
  buildColumn({ name: "endTime", type_oid: MzDataType.timestamptz }),
  buildColumn({ name: "clusterName" }),
  buildColumn({ name: "clusterId" }),
  buildColumn({ name: "duration", type_oid: MzDataType.interval }),
  buildColumn({ name: "clusterExists", type_oid: MzDataType.bool }),
  buildColumn({ name: "databaseVersion" }),
  buildColumn({ name: "errorMessage" }),
  buildColumn({ name: "executionId" }),
  buildColumn({ name: "executionStrategy" }),
  buildColumn({ name: "finishedStatus" }),
  buildColumn({ name: "sessionId" }),
  buildColumn({ name: "sql" }),
  buildColumn({ name: "authenticatedUser" }),
  buildColumn({ name: "rowsReturned", type_oid: MzDataType.int8 }),
  buildColumn({ name: "resultSize", type_oid: MzDataType.int8 }),
  buildColumn({ name: "throttledCount", type_oid: MzDataType.int8 }),
  buildColumn({ name: "transactionIsolation" }),
];

const MOCK_EXCECUTION_ID = "a07dd117-b011-453b-ad36-d28f239f565f";

const INITIAL_ROUTE_ENTRIES = [`/${MOCK_EXCECUTION_ID}`];

const DEFAULT_PRIVILEGES_HANDLER = buildMockPrivilegesQueryHandler({
  isSuperUser: false,
  privileges: [
    {
      relation: STATEMENT_LIFECYCLE_TABLE,
      privilege: "SELECT",
      hasTablePrivilege: true,
    },
    {
      relation: QUERY_HISTORY_LIST_TABLE_REDACTED,
      privilege: "SELECT",
      hasTablePrivilege: false,
    },
    {
      relation: QUERY_HISTORY_LIST_TABLE,
      privilege: "SELECT",
      hasTablePrivilege: true,
    },
  ],
});

const QueryHistoryDetailWithRoute = () => (
  <Routes>
    <Route path=":id">
      <Route index path="*" element={<QueryHistoryDetail />} />
    </Route>
  </Routes>
);

const MOCK_QUERY_INFO = {
  applicationName: "web_console_shell",
  databaseName: "materialize",
  searchPath: ["public"],
  startTime: "1722403204834.000",
  endTime: "1722403204835.000",
  clusterName: "quickstart",
  clusterId: "u1",
  duration: "00:00:00.001",
  clusterExists: false,
  databaseVersion: "v0.110.1 (6bb4a5f1a)",
  errorMessage: 'ERROR: relation "all" does not exist',
  executionId: MOCK_EXCECUTION_ID,
  executionStrategy: "constant",
  finishedStatus: "success",
  sessionId: "367342cf-f504-4489-b966-1d455ed8ff82",
  sql: "show all",
  authenticatedUser: "jun@materialize.com",
  rowsReturned: "59",
  resultSize: "1024",
  throttledCount: "0",
  transactionIsolation: "strict serializable",
};

describe("QueryHistoryDetail", () => {
  it("Should redirect if query history info does not exist", async () => {
    server.use(
      DEFAULT_PRIVILEGES_HANDLER,
      buildSqlQueryHandlerV2({
        queryKey: queryHistoryQueryKeys.statementInfo({
          executionId: MOCK_EXCECUTION_ID,
          isRedacted: false,
        }),
        results: mapKyselyToTabular({
          // By returning no rows, we simulate the query history info not existing
          rows: [],
          columns: useFetchQueryHistoryStatementInfoColumns,
        }),
      }),
    );

    await renderComponent(
      <Routes>
        <Route path=":id/*" element={<QueryHistoryDetail />} />
        <Route path="/" element={<>Redirected to List page</>} />
      </Routes>,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
        initialRouterEntries: INITIAL_ROUTE_ENTRIES,
      },
    );

    expect(await screen.findByText("Redirected to List page")).toBeVisible();
  });

  it("Should show unauthorized state when user lacks privileges", async () => {
    server.use(
      // These three privileges are required to view the query history detail page
      buildMockPrivilegesQueryHandler({
        isSuperUser: false,
        privileges: [
          {
            relation: STATEMENT_LIFECYCLE_TABLE,
            privilege: "SELECT",
            hasTablePrivilege: false,
          },
          {
            relation: QUERY_HISTORY_LIST_TABLE_REDACTED,
            privilege: "SELECT",
            hasTablePrivilege: false,
          },
          {
            relation: QUERY_HISTORY_LIST_TABLE,
            privilege: "SELECT",
            hasTablePrivilege: false,
          },
        ],
      }),
    );

    await renderComponent(<QueryHistoryDetailWithRoute />, {
      initialRouterEntries: INITIAL_ROUTE_ENTRIES,
    });

    expect(
      await screen.findByText("You do not have permission to use this feature"),
    ).toBeVisible();
  });

  it("Should not show an unauthorized state when a user has permissions to see redacted SQL", async () => {
    server.use(
      // These three privileges are required to view the query history detail page
      buildMockPrivilegesQueryHandler({
        isSuperUser: false,
        privileges: [
          {
            relation: STATEMENT_LIFECYCLE_TABLE,
            privilege: "SELECT",
            // Set to true such that user has permissions to see redacted SQL
            hasTablePrivilege: true,
          },
          {
            relation: QUERY_HISTORY_LIST_TABLE_REDACTED,
            privilege: "SELECT",
            // Set to true such that user has permissions to see redacted SQL
            hasTablePrivilege: true,
          },
          {
            relation: QUERY_HISTORY_LIST_TABLE,
            privilege: "SELECT",
            hasTablePrivilege: false,
          },
        ],
      }),

      buildSqlQueryHandlerV2({
        queryKey: queryHistoryQueryKeys.statementInfo({
          executionId: MOCK_EXCECUTION_ID,
          // Based on the mock privileges, this query will be called with isRedacted to true
          isRedacted: true,
        }),
        results: mapKyselyToTabular({
          // By returning no rows, we simulate the query history info not existing
          rows: [
            {
              ...MOCK_QUERY_INFO,
              sql: "SELECT * FROM table WHERE id=<REDACTED>",
            },
          ],
          columns: useFetchQueryHistoryStatementInfoColumns,
        }),
      }),
    );

    await renderComponent(<QueryHistoryDetailWithRoute />, {
      initialRouterEntries: INITIAL_ROUTE_ENTRIES,
    });

    // Expect redacted SQL
    expect(
      await screen.findByText("SELECT * FROM table WHERE id=<REDACTED>"),
    ).toBeVisible();
  });

  it("Should show loading state when query history info is loading", async () => {
    server.use(
      DEFAULT_PRIVILEGES_HANDLER,
      buildSqlQueryHandlerV2(
        {
          queryKey: queryHistoryQueryKeys.statementInfo({
            executionId: MOCK_EXCECUTION_ID,
            isRedacted: false,
          }),
          results: mapKyselyToTabular({
            rows: [],
            columns: useFetchQueryHistoryStatementInfoColumns,
          }),
        },
        { waitTimeMs: "infinite" },
      ),
    );

    await renderComponent(<QueryHistoryDetailWithRoute />, {
      initialRouterEntries: INITIAL_ROUTE_ENTRIES,
    });

    expect(await screen.findByTestId("loading-spinner")).toBeVisible();
  });

  it("Should try to show list data as a placeholder while query history info is loading", async () => {
    const queryClient = buildQueryClient();

    // Prefill cache with query history list data
    queryClient.setQueryData<
      ReturnType<typeof useFetchQueryHistoryList>["data"]
    >(
      queryHistoryQueryKeys.list({
        filters: queryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES),
        isRedacted: false,
        isV0_132_0: false,
      }),
      () => ({
        rows: [
          {
            applicationName: "web_console_shell",
            startTime: new Date(1722403204834),
            endTime: new Date(1722403204835),
            clusterName: "quickstart",
            duration: PostgresInterval("00:00:00.001"),
            executionId: MOCK_EXCECUTION_ID,
            executionStrategy: "constant",
            finishedStatus: "success",
            sessionId: "367342cf-f504-4489-b966-1d455ed8ff82",
            sql: "show all",
            authenticatedUser: "jun@materialize.com",
            rowsReturned: BigInt(59),
            resultSize: BigInt(1024),
            throttledCount: BigInt(0),
            transactionIsolation: "strict serializable",
          },
        ],
        columns: [] as ColumnMetadata[],
        tag: "SELECT 1",
        notices: [],
      }),
    );

    // Have the statement info query load infinitely to trigger placeholder data
    server.use(
      DEFAULT_PRIVILEGES_HANDLER,
      buildSqlQueryHandlerV2(
        {
          queryKey: queryHistoryQueryKeys.statementInfo({
            executionId: MOCK_EXCECUTION_ID,
            isRedacted: false,
          }),
          results: mapKyselyToTabular({
            rows: [],
            columns: useFetchQueryHistoryStatementInfoColumns,
          }),
        },
        { waitTimeMs: "infinite" },
      ),
    );

    await renderComponent(<QueryHistoryDetailWithRoute />, {
      initialRouterEntries: INITIAL_ROUTE_ENTRIES,
      queryClient,
    });

    expect(await screen.findByText("jun@materialize.com")).toBeVisible();
    expect(await screen.findByText(MOCK_EXCECUTION_ID)).toBeVisible();
  });

  it("Should show an error state if query history info fails to fetch", async () => {
    server.use(
      DEFAULT_PRIVILEGES_HANDLER,
      buildSqlQueryHandlerV2({
        queryKey: queryHistoryQueryKeys.statementInfo({
          executionId: MOCK_EXCECUTION_ID,
          isRedacted: false,
        }),
        results: {
          error: {
            code: ErrorCode.INTERNAL_ERROR,
            message: "An error occurred while fetching query history info",
          },
          notices: [],
        },
      }),
    );

    await renderComponent(<QueryHistoryDetailWithRoute />, {
      initialRouterEntries: INITIAL_ROUTE_ENTRIES,
    });

    expect(
      await screen.findByText("An unexpected error has occurred"),
    ).toBeVisible();
  });

  it("Should render query history info if fetch is successful", async () => {
    const startTime = new Date("2021-09-01T00:05:00");
    const endTime = new Date("2021-09-01T00:05:05");
    const durationMinutes = 5;
    // Mock successful fetch of query history info
    server.use(
      DEFAULT_PRIVILEGES_HANDLER,
      buildSqlQueryHandlerV2({
        queryKey: queryHistoryQueryKeys.statementInfo({
          executionId: MOCK_EXCECUTION_ID,
          isRedacted: false,
        }),
        results: mapKyselyToTabular({
          rows: [
            {
              ...MOCK_QUERY_INFO,
              duration: `00:0${durationMinutes}:00.000`,
              startTime: `${startTime.getTime()}`,
              endTime: `${endTime.getTime()}`,
            },
          ],
          columns: useFetchQueryHistoryStatementInfoColumns,
        }),
      }),
    );

    await renderComponent(<QueryHistoryDetailWithRoute />, {
      initialRouterEntries: INITIAL_ROUTE_ENTRIES,
    });

    // Application name
    expect(await screen.findByText("web_console_shell")).toBeVisible();
    // Database name
    expect(await screen.findByText("materialize")).toBeVisible();
    // Search path
    expect(await screen.findByText("public")).toBeVisible();
    // Start time
    expect(
      await screen.findByText(formatDate(startTime, FRIENDLY_DATETIME_FORMAT)),
    ).toBeVisible();
    // End time
    expect(
      await screen.findByText(formatDate(endTime, FRIENDLY_DATETIME_FORMAT)),
    ).toBeVisible();
    // Cluster name
    expect(await screen.findByText(/quickstart/)).toBeVisible();
    // Duration
    expect(await screen.findByText(`${durationMinutes}m0s`)).toBeVisible();
    // Database version
    expect(await screen.findByText("v0.110.1 (6bb4a5f1a)")).toBeVisible();
    // Error message
    expect(
      await screen.findByText('ERROR: relation "all" does not exist'),
    ).toBeVisible();
    // Execution ID
    expect(await screen.findByText(MOCK_EXCECUTION_ID)).toBeVisible();
    // Execution strategy
    expect(await screen.findByText("constant")).toBeVisible();
    // Finished status
    expect(await screen.findByText("Success")).toBeVisible();
    //Session ID
    expect(
      await screen.findByText("367342cf-f504-4489-b966-1d455ed8ff82"),
    ).toBeVisible();
    // SQL text
    expect(await screen.findByText("show all")).toBeVisible();
    // Authenticated user
    expect(await screen.findByText("jun@materialize.com")).toBeVisible();
    // Rows returned
    expect(await screen.findByText("59")).toBeVisible();
    // Transaction isolation
    expect(await screen.findByText("strict serializable")).toBeVisible();
    // Throttled count
    expect(await screen.findByText("0")).toBeVisible();
  });

  it("Should show success toast if query cancellation succeeds", async () => {
    server.use(
      DEFAULT_PRIVILEGES_HANDLER,
      // Mock successful fetch of query history info
      buildSqlQueryHandlerV2({
        queryKey: queryHistoryQueryKeys.statementInfo({
          executionId: MOCK_EXCECUTION_ID,
          isRedacted: false,
        }),
        results: mapKyselyToTabular({
          rows: [
            {
              ...MOCK_QUERY_INFO,
              // We need to query to be running to show the cancellation button
              finishedStatus: "running",
            },
          ],
          columns: useFetchQueryHistoryStatementInfoColumns,
        }),
      }),

      // Mock successful cancellation of query
      buildSqlQueryHandlerV2({
        queryKey: cancelQueryQueryKeys.connectionIdFromSessionId({
          sessionId: MOCK_QUERY_INFO.sessionId,
        }),
        results: mapKyselyToTabular({
          rows: [{ connection_id: "some-connection-id" }],
          columns: [buildColumn({ name: "connection_id" })],
        }),
      }),
      buildSqlQueryHandlerV2({
        queryKey: cancelQueryQueryKeys.cancelQuery({
          sessionId: MOCK_QUERY_INFO.sessionId,
        }),
        results: mapKyselyToTabular({
          rows: [{ pg_cancel_backend: true }],
          columns: [
            buildColumn({
              name: "pg_cancel_backend",
              type_oid: MzDataType.bool,
            }),
          ],
        }),
      }),
    );

    await renderComponent(<QueryHistoryDetailWithRoute />, {
      initialRouterEntries: INITIAL_ROUTE_ENTRIES,
    });

    const queryCancelButton = await screen.findByText("Request cancellation");

    queryCancelButton.click();
    // Toast should pop up to indicate that the query was successfully cancelled
    expect(await screen.findByText("Query successfully cancelled"));
  });

  it("Should show error toast if query cancellation fails", async () => {
    server.use(
      DEFAULT_PRIVILEGES_HANDLER,
      // Mock successful fetch of query history info
      buildSqlQueryHandlerV2({
        queryKey: queryHistoryQueryKeys.statementInfo({
          executionId: MOCK_EXCECUTION_ID,
          isRedacted: false,
        }),
        results: mapKyselyToTabular({
          rows: [
            {
              ...MOCK_QUERY_INFO,
              // We need to query to be running to show the cancellation button
              finishedStatus: "running",
            },
          ],
          columns: useFetchQueryHistoryStatementInfoColumns,
        }),
      }),

      buildSqlQueryHandlerV2({
        queryKey: cancelQueryQueryKeys.connectionIdFromSessionId({
          sessionId: MOCK_QUERY_INFO.sessionId,
        }),
        results: mapKyselyToTabular({
          rows: [{ connection_id: "some-connection-id" }],
          columns: [buildColumn({ name: "connection_id" })],
        }),
      }),

      // Mock successful cancellation of query
      buildSqlQueryHandlerV2({
        queryKey: cancelQueryQueryKeys.cancelQuery({
          sessionId: MOCK_QUERY_INFO.sessionId,
        }),
        results: mapKyselyToTabular({
          // Mock unsuccessful cancellation of query

          rows: [{ pg_cancel_backend: false }],
          columns: [
            buildColumn({
              name: "pg_cancel_backend",
              type_oid: MzDataType.bool,
            }),
          ],
        }),
      }),
    );

    await renderComponent(<QueryHistoryDetailWithRoute />, {
      initialRouterEntries: INITIAL_ROUTE_ENTRIES,
    });

    const queryCancelButton = await screen.findByText("Request cancellation");

    queryCancelButton.click();
    // Toast should pop up to indicate that the query failed to cancel
    expect(await screen.findByText("Unable to cancel query"));
  });
});
