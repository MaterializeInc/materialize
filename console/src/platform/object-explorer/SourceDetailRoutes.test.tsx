// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { findByText, screen } from "@testing-library/react";
import { formatDate } from "date-fns";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { Source } from "~/api/materialize/source/sourceList";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  buildUseSqlQueryHandler,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { DEFAULT_TIME_PERIOD } from "~/hooks/useTimePeriodSelect";
import { renderComponent, RenderWithPathname } from "~/test/utils";
import { DATE_FORMAT_SHORT } from "~/utils/dateFormat";

import { SOURCES_FETCH_ERROR_MESSAGE } from "../sources/constants";
import { sourceQueryKeys } from "../sources/queries";
import { objectExplorerQueryKeys } from "./queries";
import { SourceDetailRoutes } from "./SourceDetailRoutes";

vi.mock("~/platform/sources/SourceErrorsGraph", () => ({
  default: function () {
    return <div>Source Errors Graph</div>;
  },
}));

const useSourceListColumns = buildColumns([
  "id",
  "name",
  "type",
  "size",
  "schemaName",
  "databaseName",
  "status",
  "error",
  "snapshotCommitted",
  { name: "createdAt", type_oid: MzDataType.timestamptz },
  "clusterId",
  "clusterName",
  "connectionId",
  "connectionName",
  "kafkaTopic",
  "webhookUrl",
  "isOwner",
]);

const useCurrentSourceStatisticsColumns = buildColumns([
  "id",
  { name: "messagesReceived", type_oid: MzDataType.numeric },
  { name: "bytesReceived", type_oid: MzDataType.numeric },
  { name: "updatesStaged", type_oid: MzDataType.numeric },
  { name: "updatesCommitted", type_oid: MzDataType.numeric },
  { name: "snapshotRecordsKnown", type_oid: MzDataType.numeric },
  { name: "snapshotRecordsStaged", type_oid: MzDataType.numeric },
  { name: "offsetDelta", type_oid: MzDataType.numeric },
  { name: "rehydrationLatency", type_oid: MzDataType.interval },
]);

const useSourceErrorsColumns = buildColumns([
  {
    name: "lastOccurred",
    type_oid: MzDataType.timestamptz,
    type_mod: 2555947,
  },
  {
    name: "error",
    type_oid: MzDataType.text,
  },
  {
    name: "count",
    type_oid: MzDataType.int8,
  },
]);

const DEFAULT_SOURCE_ERRORS_TIME_PERIOD = parseInt(DEFAULT_TIME_PERIOD);

function buildPostgresSource(overrides?: Partial<Source>): Source {
  return {
    id: "u4",
    name: "test_source",
    type: "postgres",
    size: "xsmall",
    schemaName: "public",
    databaseName: "default",
    status: "stalled",
    error: "reached maximum WAL lag",
    snapshotCommitted: true,
    // TODO: figure out a better solution for mocking columns that get transformed by our
    // query layer. It's useful to have the object type here so we are prompted to add /
    // update fields, but it doesn't actually match the types on the wire.
    createdAt: "1706634695748" as unknown as Date,
    clusterId: "u4",
    clusterName: "source_cluster",
    connectionId: "u123",
    connectionName: "postgres_connection",
    kafkaTopic: null,
    webhookUrl: null,
    isOwner: true,
    ...overrides,
  };
}

function setupSourceOverviewPage({
  sourceOverrides,
}: {
  sourceOverrides?: Partial<Source>;
} = {}) {
  const source = buildPostgresSource(sourceOverrides);

  const validSourceListHandler = buildSqlQueryHandlerV2({
    queryKey: sourceQueryKeys.list({ filters: {} }),
    results: mapKyselyToTabular({
      rows: [source],
      columns: useSourceListColumns,
    }),
  });
  const validUseShowCreateHandler = buildUseSqlQueryHandler({
    type: "SHOW CREATE" as const,
    name: source.name,
    createSql: "CREATE SOURCE ...",
  });

  const currentSourceStatisticsHandler = buildSqlQueryHandlerV2({
    queryKey: sourceQueryKeys.statistics({
      sourceId: source.id,
    }),
    results: mapKyselyToTabular({
      rows: [
        {
          id: "u79",
          messagesReceived: "3",
          bytesReceived: "179",
          updatesStaged: "3",
          updatesCommitted: "3",
          snapshotRecordsKnown: "100000",
          snapshotRecordsStaged: "17000",
          offsetDelta: "0",
          rehydrationLatency: "04:47:51.367",
        },
      ],
      columns: useCurrentSourceStatisticsColumns,
    }),
  });

  const isOwnerResponse = buildSqlQueryHandlerV2({
    queryKey: objectExplorerQueryKeys.isOwner({
      objectId: source.id,
    }),
    results: mapKyselyToTabular({
      columns: buildColumns([{ name: "isOwner", type_oid: MzDataType.bool }]),
      rows: [
        {
          isOwner: true,
        },
      ],
    }),
  });

  server.use(
    validSourceListHandler,
    validUseShowCreateHandler,
    currentSourceStatisticsHandler,
    isOwnerResponse,
  );

  return {
    path: `/${source.databaseName}/schemas/${source.schemaName}/sources/${source.name}/${source.id}`,
    source,
  };
}

function setupSourceErrorsPage() {
  const source = buildPostgresSource();

  const validSourceListHandler = buildSqlQueryHandlerV2({
    queryKey: sourceQueryKeys.list({ filters: {} }),
    results: mapKyselyToTabular({
      rows: [source],
      columns: useSourceListColumns,
    }),
  });

  const sourceDetailsQueryHandler = buildSqlQueryHandlerV2({
    queryKey: sourceQueryKeys.show({ sourceId: source.id }),
    results: mapKyselyToTabular({
      rows: [],
      columns: useSourceListColumns,
    }),
  });

  const emptyUseSourceErrorsHandler = buildSqlQueryHandlerV2({
    queryKey: sourceQueryKeys.errors({
      sourceId: source.id,
      timePeriodMinutes: DEFAULT_SOURCE_ERRORS_TIME_PERIOD,
    }),
    results: mapKyselyToTabular({
      rows: [],
      columns: useSourceErrorsColumns,
    }),
  });

  const isOwnerResponse = buildSqlQueryHandlerV2({
    queryKey: objectExplorerQueryKeys.isOwner({
      objectId: source.id,
    }),
    results: mapKyselyToTabular({
      columns: buildColumns([{ name: "isOwner", type_oid: MzDataType.bool }]),
      rows: [
        {
          isOwner: true,
        },
      ],
    }),
  });

  server.use(
    validSourceListHandler,
    sourceDetailsQueryHandler,
    emptyUseSourceErrorsHandler,
    isOwnerResponse,
  );

  return {
    path: `/${source.databaseName}/schemas/${source.schemaName}/sources/${source.name}/${source.id}/errors`,
    source,
  };
}

function renderSourceDetails(initialRouterEntries: string[]) {
  return renderComponent(
    <Routes>
      <Route path=":databaseName?/schemas?/:schemaName?/:objectType?/:objectName?/:id?">
        <Route
          index
          path="*"
          element={
            <RenderWithPathname>
              <SourceDetailRoutes />
            </RenderWithPathname>
          }
        />
      </Route>
    </Routes>,
    {
      initialRouterEntries,
    },
  );
}

describe("SourceDetailRoutes", () => {
  describe("SourceOverview", () => {
    it("shows source information", async () => {
      const { path, source } = setupSourceOverviewPage();

      renderSourceDetails([path]);

      const mainContent = await screen.findByTestId(
        "main-content",
        {},
        { timeout: 5_000 },
      );
      expect(
        await findByText(
          mainContent,
          `${source.databaseName}.${source.schemaName}`,
        ),
      ).toBeVisible();
      expect(await findByText(mainContent, source.name)).toBeVisible();
      expect(await findByText(mainContent, "Postgres")).toBeVisible();
      expect(await findByText(mainContent, "Stalled")).toBeVisible();
      expect(await findByText(mainContent, /Jan. 30, 2024/)).toBeVisible();
      expect(
        await findByText(mainContent, source.connectionName!),
      ).toBeVisible();
      expect(await findByText(mainContent, "5 hours")).toBeVisible();
      // graph titles
      expect(await findByText(mainContent, "Ingestion lag")).toBeVisible();
      expect(await findByText(mainContent, "Rows received")).toBeVisible();
      expect(await findByText(mainContent, "Bytes received")).toBeVisible();
      expect(await findByText(mainContent, "Ingestion rate")).toBeVisible();
    });

    it("shows snapshot progress", async () => {
      const { path, source } = setupSourceOverviewPage({
        sourceOverrides: {
          snapshotCommitted: false,
          status: "running",
          error: null,
        },
      });

      renderSourceDetails([path]);

      const mainContent = await screen.findByTestId(
        "main-content",
        {},
        { timeout: 5_000 },
      );
      expect(
        await findByText(
          mainContent,
          `${source.databaseName}.${source.schemaName}`,
        ),
      ).toBeVisible();
      expect(await screen.findByText("Snapshotting progress")).toBeVisible();
      expect(screen.getByText("100,000 rows")).toBeVisible();
      expect(screen.getByText("17,000")).toBeVisible();
      expect(screen.getByText("17%")).toBeVisible();
    });
  });

  describe("SourceErrors", async () => {
    it("renders an error state when source errors fail to fetch", async () => {
      const { source, path } = setupSourceErrorsPage();

      const useSourcesErrorsHandler = buildSqlQueryHandlerV2({
        queryKey: sourceQueryKeys.errors({
          sourceId: source.id,
          timePeriodMinutes: DEFAULT_SOURCE_ERRORS_TIME_PERIOD,
        }),
        results: {
          notices: [],
          error: {
            message: "Something went wrong",
            code: ErrorCode.INTERNAL_ERROR,
          },
        },
      });

      server.use(useSourcesErrorsHandler);

      renderSourceDetails([path]);
      expect(
        await screen.findByText(SOURCES_FETCH_ERROR_MESSAGE),
      ).toBeVisible();
    });

    it("renders an empty state when there are no source errors", async () => {
      const { path } = setupSourceErrorsPage();

      renderSourceDetails([path]);
      expect(
        await screen.findByText("No errors during this time period."),
      ).toBeVisible();
    });

    it("renders a list of source errors", async () => {
      const { source, path } = setupSourceErrorsPage();
      const mockTimestamp = 1692123417788;

      const validUseSourcesErrorsHandler = buildSqlQueryHandlerV2({
        queryKey: sourceQueryKeys.errors({
          sourceId: source.id,
          timePeriodMinutes: DEFAULT_SOURCE_ERRORS_TIME_PERIOD,
        }),
        results: mapKyselyToTabular({
          rows: [
            {
              lastOccurred: mockTimestamp.toString(),
              error: "error_1",
              count: "1",
            },
          ],
          columns: useSourceErrorsColumns,
        }),
      });

      server.use(validUseSourcesErrorsHandler);

      renderSourceDetails([path]);
      expect(await screen.findByText("error_1")).toBeVisible();
      expect(await screen.findByText("1")).toBeVisible();
      expect(
        await screen.findByText(
          formatDate(new Date(mockTimestamp), DATE_FORMAT_SHORT),
        ),
      ).toBeVisible();
    });
  });
});
