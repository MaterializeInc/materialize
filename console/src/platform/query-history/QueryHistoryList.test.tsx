// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { STATEMENT_LIFECYCLE_TABLE } from "~/api/materialize/query-history/queryHistoryDetail";
import {
  DEFAULT_SCHEMA_VALUES,
  QUERY_HISTORY_LIST_TABLE,
  QUERY_HISTORY_LIST_TABLE_REDACTED,
  QueryHistoryListSchema,
  queryHistoryListSchema,
} from "~/api/materialize/query-history/queryHistoryList";
import { Column, ErrorCode, MzDataType } from "~/api/materialize/types";
import { buildMockPrivilegesQueryHandler } from "~/api/mocks/buildMockPrivilegesQueryHandler";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { formatDate, FRIENDLY_DATETIME_FORMAT } from "~/utils/dateFormat";
import { formatBytesShort } from "~/utils/format";

import { queryHistoryQueryKeys } from "./queries";
import QueryHistoryList from "./QueryHistoryList";
import { COLUMNS, DEFAULT_COLUMNS } from "./useColumns";

const useFetchQueryHistoryListColumns: Array<Column> = [
  buildColumn({ name: "applicationName" }),
  buildColumn({ name: "clusterName" }),
  buildColumn({ name: "executionId" }),
  buildColumn({ name: "executionStrategy" }),
  buildColumn({ name: "finishedStatus" }),
  buildColumn({ name: "sessionId" }),
  buildColumn({ name: "sql" }),
  buildColumn({ name: "authenticatedUser" }),
  buildColumn({ name: "duration", type_oid: MzDataType.interval }),
  buildColumn({ name: "startTime", type_oid: MzDataType.timestamptz }),
  buildColumn({ name: "endTime", type_oid: MzDataType.timestamptz }),
  buildColumn({ name: "rowsReturned" }),
  buildColumn({ name: "resultSize" }),
  buildColumn({ name: "transactionIsolation" }),
  buildColumn({ name: "throttledCount", type_oid: MzDataType.uint8 }),
];

const useFetchQueryHistoryClustersColumns: Array<Column> = [
  buildColumn({ name: "id" }),
  buildColumn({ name: "name" }),
];

const useFetchQueryHistoryUsersColumns: Array<Column> = [
  buildColumn({ name: "email" }),
];

const DEFAULT_FETCH_QUERY_LIST_HANDLER = buildSqlQueryHandlerV2({
  queryKey: queryHistoryQueryKeys.list({
    filters: queryHistoryListSchema.parse(DEFAULT_SCHEMA_VALUES),
    isRedacted: false,
    isV0_132_0: false,
  }),
  results: mapKyselyToTabular({
    rows: [],
    columns: useFetchQueryHistoryListColumns,
  }),
});

const DEFAULT_FETCH_CLUSTER_LIST_HANDLER = buildSqlQueryHandlerV2({
  queryKey: queryHistoryQueryKeys.clusters(),
  results: mapKyselyToTabular({
    rows: [],
    columns: useFetchQueryHistoryClustersColumns,
  }),
});

const DEFAULT_FETCH_QUERY_HISTORY_USERS_HANDLER = buildSqlQueryHandlerV2({
  queryKey: queryHistoryQueryKeys.users(),
  results: mapKyselyToTabular({
    rows: [],
    columns: useFetchQueryHistoryUsersColumns,
  }),
});

const PARSED_DEFAULT_SCHEMA_VALUES = queryHistoryListSchema.parse(
  DEFAULT_SCHEMA_VALUES,
);

const ALL_COLUMNS = COLUMNS.map(({ key }) => key);

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

describe("QueryHistoryList", () => {
  beforeEach(() => {
    server.use(DEFAULT_PRIVILEGES_HANDLER);
    server.use(DEFAULT_FETCH_QUERY_LIST_HANDLER);
    server.use(DEFAULT_FETCH_CLUSTER_LIST_HANDLER);
    server.use(DEFAULT_FETCH_QUERY_HISTORY_USERS_HANDLER);
  });

  afterEach(() => {
    server.resetHandlers();
  });

  it("Should render a list of queries with no columns hidden", async () => {
    const startTime = new Date("2021-09-01T00:05:00");
    const endTime = new Date("2021-09-01T00:05:05");

    const durationMinutes = 5;

    const mockRow = {
      applicationName: "web_console",
      clusterName: "mz_catalog_server",
      executionId: "65cc0294-46b4-41b7-a785-4a52c46db6cd",
      executionStrategy: "standard",
      finishedAt: "1705526864756",
      finishedStatus: "success",
      sessionId: "279dd8ac-3a4f-478e-af83-a21ff2509702",
      sql: "SELECT * FROM mz_catalog.mz_schemas",
      authenticatedUser: "jun@materialize.com",
      duration: `00:0${durationMinutes}:00.000`,
      startTime: `${startTime.getTime()}`,
      endTime: `${endTime.getTime()}`,
      rowsReturned: "5069",
      resultSize: "2048",
      transactionIsolation: "strict serializable",
      throttledCount: "0",
    };

    server.use(
      buildSqlQueryHandlerV2({
        queryKey: queryHistoryQueryKeys.list({
          filters: PARSED_DEFAULT_SCHEMA_VALUES,
          isRedacted: false,
          isV0_132_0: false,
        }),
        results: mapKyselyToTabular({
          columns: useFetchQueryHistoryListColumns,
          rows: [mockRow],
        }),
      }),
    );

    await renderComponent(
      <QueryHistoryList
        initialFilters={PARSED_DEFAULT_SCHEMA_VALUES}
        initialColumns={ALL_COLUMNS}
      />,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      },
    );

    // SQL command
    expect(
      await screen.findByText(mockRow.sql, undefined, {
        // this is slow in CI
        timeout: 5_000,
      }),
    ).toBeVisible();

    // Application name
    expect(await screen.findByText(mockRow.applicationName)).toBeVisible();

    // Cluster name
    expect(await screen.findByText(mockRow.clusterName)).toBeVisible();

    // Execution id
    expect(await screen.findByText(mockRow.executionId)).toBeVisible();

    // Execution strategy
    expect(await screen.findByText(mockRow.executionStrategy)).toBeVisible();

    // Finished status
    expect((await screen.findAllByText("Success"))?.length).toBeGreaterThan(0);

    // Session id
    expect(await screen.findByText(mockRow.sessionId)).toBeVisible();

    // Email
    expect(await screen.findByText(mockRow.authenticatedUser)).toBeVisible();

    // Duration
    expect(await screen.findByText(`${durationMinutes}m0s`)).toBeVisible();

    // Start time
    expect(
      await screen.findByText(formatDate(startTime, FRIENDLY_DATETIME_FORMAT)),
    ).toBeVisible();

    // End time
    expect(
      await screen.findByText(formatDate(endTime, FRIENDLY_DATETIME_FORMAT)),
    ).toBeVisible();

    // Rows returned
    expect(
      (await screen.findAllByText(mockRow.rowsReturned))?.[0],
    ).toBeVisible();

    // Result size
    expect(
      await screen.findByText(formatBytesShort(BigInt(mockRow.resultSize))),
    ).toBeVisible();

    // Throttled count
    expect(await screen.findByText(mockRow.throttledCount)).toBeVisible();

    // Transaction isolation
    expect(await screen.findByText(mockRow.transactionIsolation)).toBeVisible();
  });

  it("When the Filter dropdown menu is open and then the cluster dropdown opens, clicking anywhere on the page should close the cluster dropdown", async () => {
    const user = userEvent.setup();
    const mockCluster = { id: "u1", name: "Cluster 1" };

    server.use(
      buildSqlQueryHandlerV2({
        queryKey: queryHistoryQueryKeys.clusters(),
        results: mapKyselyToTabular({
          rows: [mockCluster],
          columns: useFetchQueryHistoryClustersColumns,
        }),
      }),
    );

    await renderComponent(
      <QueryHistoryList
        initialFilters={PARSED_DEFAULT_SCHEMA_VALUES}
        initialColumns={DEFAULT_COLUMNS}
      />,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      },
    );

    const openFilterDropdownMenuTitle = "Filters";

    // Expect the filter dropdown menu to be closed. We use getByText since the element exists in the DOM but is hidden
    expect(
      await screen.findByText(openFilterDropdownMenuTitle),
    ).not.toBeVisible();

    // Expect the cluster dropdown to be closed since the default value is "All clusters"
    expect(screen.queryByText(mockCluster.name)).toBeNull();
    // Open filter dropdown menu
    await user.click(screen.getByLabelText("Filter menu"));

    // Wait for dropdown to open
    await waitFor(async () => {
      expect(screen.getByText(openFilterDropdownMenuTitle)).toBeVisible();
    });

    // Open cluster dropdown
    await user.click(screen.getByLabelText("Cluster filter"));

    // Expect the cluster dropdown to be open
    expect(screen.getByText("Cluster 1")).toBeVisible();

    // Click on the page header to close the cluster dropdown
    await user.click(screen.getByText("Query History"));

    // Expect the cluster dropdown to be closed
    expect(screen.queryByText("Cluster 1")).toBeNull();
  });

  it("Should show an unauthorized state when a user lacks privileges", async () => {
    server.use(
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

    await renderComponent(
      <QueryHistoryList
        initialFilters={PARSED_DEFAULT_SCHEMA_VALUES}
        initialColumns={DEFAULT_COLUMNS}
      />,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      },
    );
    expect(
      await screen.findByText("You do not have permission to use this feature"),
    ).toBeVisible();
  });

  it("Should not show an unauthorized state when a user has permissions to see redacted SQL", async () => {
    server.use(
      // privileges handler
      buildMockPrivilegesQueryHandler({
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
            // Mock such that user has permissions to see redacted SQL
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
        queryKey: queryHistoryQueryKeys.list({
          filters: PARSED_DEFAULT_SCHEMA_VALUES,
          isRedacted: true,
          isV0_132_0: false,
        }),
        results: mapKyselyToTabular({
          rows: [],
          columns: useFetchQueryHistoryListColumns,
        }),
      }),
    );

    await renderComponent(
      <QueryHistoryList
        initialFilters={PARSED_DEFAULT_SCHEMA_VALUES}
        initialColumns={DEFAULT_COLUMNS}
      />,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      },
    );
    // Expect empty state
    expect(await screen.findByText("No results found.")).toBeVisible();
  });

  // TODO (robinclowers): Fix and renenable this https://github.com/MaterializeInc/console/issues/2482
  it.skip(
    "Should show an error state when we fail to fetch the query history list",
    {
      // Flaky in CI, hopefully increasing the timeout will fix it.
      timeout: 60_000,
    },
    async () => {
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: queryHistoryQueryKeys.list({
            filters: PARSED_DEFAULT_SCHEMA_VALUES,
          }),
          results: {
            error: {
              message: "Something went wrong",
              code: ErrorCode.INTERNAL_ERROR,
            },
            notices: [],
          },
        }),
      );

      await renderComponent(
        <QueryHistoryList
          initialFilters={PARSED_DEFAULT_SCHEMA_VALUES}
          initialColumns={DEFAULT_COLUMNS}
        />,
        {
          initializeState: ({ set }) =>
            setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
        },
      );
      expect(
        await screen.findByText(
          "An error occurred loading the query history list",
        ),
      ).toBeVisible();
    },
  );

  it(
    "Should select filters and get data that corresponds to the query key",
    {
      // Increase the timeout because this test takes around ~10 seconds to run in CI which passes the 5 second default threshold.
      timeout: 25_000,
    },
    async () => {
      // Mock "new Date()"
      const mockedDateNow = new Date("1999-09-03T00:00:00");
      vi.useFakeTimers({
        now: mockedDateNow,
        shouldAdvanceTime: true, // MSW doesn't match handlers without this
      });
      const user = userEvent.setup();

      const initialFilters = queryHistoryListSchema.parse(
        DEFAULT_SCHEMA_VALUES,
      );
      // Mock query history list fetch with default filters
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: queryHistoryQueryKeys.list({
            filters: initialFilters,
            isRedacted: false,
            isV0_132_0: false,
          }),
          results: mapKyselyToTabular({
            columns: useFetchQueryHistoryListColumns,
            // Initial page should be empty
            rows: [],
          }),
        }),
      );

      // Mock user list fetch
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: queryHistoryQueryKeys.users(),
          results: mapKyselyToTabular({
            rows: [{ email: "jun@materialize.com" }],
            columns: useFetchQueryHistoryUsersColumns,
          }),
        }),
      );

      const CATALOG_SERVER_CLUSTER = {
        id: "s2",
        name: "mz_catalog_server",
      };
      // Mock cluster list fetch
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: queryHistoryQueryKeys.clusters(),
          results: mapKyselyToTabular({
            columns: useFetchQueryHistoryClustersColumns,
            rows: [
              {
                id: "u28",
                name: "default",
              },
              {
                id: "u34",
                name: "my_production_cluster",
              },
              CATALOG_SERVER_CLUSTER,
              {
                id: "s1",
                name: "mz_system",
              },
            ],
          }),
        }),
      );

      const expectedStartTimeFilter = new Date("1999-09-01T00:00:00");
      const expectedEndTimeFilter = new Date("1999-09-02T23:59:59.999");
      const durationMinutes = 5;
      const expectedFilters: QueryHistoryListSchema = {
        dateRange: [
          expectedStartTimeFilter.toISOString(),
          expectedEndTimeFilter.toISOString(),
        ],
        user: "jun@materialize.com",
        sessionId: "279dd8ac-3a4f-478e-af83-a21ff2509702",
        applicationName: "web_console",
        sqlText: "SELECT",
        executionId: "65cc0294-46b4-41b7-a785-4a52c46db6cd",
        clusterId: CATALOG_SERVER_CLUSTER.id,
        finishedStatuses: ["error"],
        statementTypes: ["select"],
        durationRange: {
          minDuration: 30,
          maxDuration: null,
        },
        showConsoleIntrospection: true,
        sortOrder: "asc",
        sortField: "duration",
      };

      const expectedRow = {
        applicationName: "web_console",
        clusterName: "mz_catalog_server",
        executionId: "65cc0294-46b4-41b7-a785-4a52c46db6cd",
        executionStrategy: "standard",
        finishedAt: "1705526864756",
        finishedStatus: expectedFilters.finishedStatuses[0],
        sessionId: expectedFilters.sessionId,
        sql: "SELECT * FROM mz_catalog.mz_schemas",
        authenticatedUser: expectedFilters.user,
        duration: `00:0${durationMinutes}:00.000`,
        startTime: `${expectedFilters.dateRange[0]}`,
        endTime: `${expectedFilters.dateRange[1]}`,
        rowsReturned: "5069",
        resultSize: "2048",
        throttledCount: "0",
      };

      // Mock query history list fetch with expected filters
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: queryHistoryQueryKeys.list({
            filters: expectedFilters,
            isRedacted: false,
            isV0_132_0: false,
          }),
          results: mapKyselyToTabular({
            columns: useFetchQueryHistoryListColumns,
            rows: [expectedRow],
          }),
        }),
      );

      await renderComponent(
        <QueryHistoryList
          initialFilters={initialFilters}
          initialColumns={ALL_COLUMNS}
        />,
        {
          initializeState: ({ set }) =>
            setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
        },
      );

      expect(await screen.findByText("No results found.")).toBeVisible();

      // Set user filter
      await user.click(screen.getByLabelText("User filter"));
      await user.click(screen.getByText(expectedFilters.user));

      // Set cluster filter
      await user.click(screen.getByLabelText("Cluster filter"));
      await user.click(screen.getByText(CATALOG_SERVER_CLUSTER.name));

      // Set date range filter
      await user.click(screen.getByLabelText("Date range filter"));

      await user.click(
        // Min start time
        await screen.findByLabelText(
          new Date("1999-09-01T00:00:00").toString(),
        ),
      );

      await user.click(
        // Max start time
        await screen.findByLabelText(
          new Date("1999-09-02T00:00:00").toString(),
        ),
      );
      await user.click(screen.getByText("Apply filter"));

      // Set statement filter
      await user.click(screen.getByLabelText("Filter menu")); // Open accordion item
      await user.click(screen.getByLabelText("Statement type"));
      await user.click(
        screen.getByLabelText(expectedFilters.statementTypes[0]),
      );

      await user.click(screen.getByLabelText("Status"));
      await user.click(
        screen.getByLabelText(expectedFilters.finishedStatuses[0]),
      );

      // Set duration filter
      await user.click(screen.getByLabelText("Duration (ms)")); // Open accordion item
      const minDurationTextInput = screen.getByPlaceholderText("10");
      await user.type(
        minDurationTextInput,
        expectedFilters.durationRange.minDuration!.toString(),
      );

      // Set session ID filter
      await user.click(screen.getByLabelText("Session ID")); // Open accordion item
      const sessionIdTextInput =
        screen.getByPlaceholderText("Enter session ID");
      await user.type(sessionIdTextInput, expectedFilters.sessionId!);

      // Set application name filter
      await user.click(screen.getByLabelText("Application name")); // Open accordion item
      const applicationNameTextInput = screen.getByPlaceholderText(
        "Enter application name",
      );
      await user.type(
        applicationNameTextInput,
        expectedFilters.applicationName!,
      );

      // Set SQL text filter
      await user.click(screen.getByLabelText("SQL text")); // Open accordion item
      const sqlTextInput = screen.getByPlaceholderText("Enter SQL text");
      await user.type(sqlTextInput, expectedFilters.sqlText!);

      // Set query ID filter
      await user.click(screen.getByLabelText("Query ID")); // Open accordion item
      const executionIdInput = screen.getByPlaceholderText("Enter query ID");

      await user.type(executionIdInput, expectedFilters.executionId!);

      // Set show console introspection filter
      await user.click(screen.getByLabelText("Show Console introspection")); // toggle checkbox

      // Apply filters
      await user.click(screen.getByText("Apply filter"));

      // Set sort order to ascending

      await user.click(screen.getByLabelText("Sort filter")); // Open sort filter
      await user.click(screen.getByText(/Ascending/g));

      // Set sort by to duration
      await user.click(screen.getByLabelText("Sort filter")); // Open sort filter

      const durationElements = screen.getAllByText("Duration"); // Since many elements with text 'Duration' exist
      await user.click(durationElements[0]);

      // The expected API handler should be called and display the expected row
      expect(await screen.findByText(expectedRow.sql)).toBeVisible();

      vitest.useRealTimers();
    },
  );
});
