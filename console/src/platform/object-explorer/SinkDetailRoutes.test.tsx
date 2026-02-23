// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { formatDate } from "date-fns";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { Sink } from "~/api/materialize/sink/sinkList";
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

import { SINKS_FETCH_ERROR_MESSAGE } from "../sinks/constants";
import { sinkQueryKeys } from "../sinks/queries";
import { objectExplorerQueryKeys } from "./queries";
import { SinkDetailRoutes } from "./SinkDetailRoutes";

vi.mock("~/platform/sinks/SinkErrorsGraph", () => ({
  default: function () {
    return <div>Sink Errors Graph</div>;
  },
}));

const DEFAULT_SINK_ERRORS_TIME_PERIOD = parseInt(DEFAULT_TIME_PERIOD);

const sinkErrorsColumns = buildColumns([
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

const sinkListColumns = buildColumns([
  "id",
  "name",
  "type",
  "size",
  "schemaName",
  "databaseName",
  "status",
  "error",
  "isOwner",
]);

function buildSink(overrides: Partial<Sink> = {}): Sink {
  return {
    id: "u7",
    name: "json_sink",
    type: "kafka",
    size: "xsmall",
    schemaName: "public",
    databaseName: "default",
    status: "running",
    error: null,
    isOwner: true,
    clusterName: "xsmall_cluster",
    clusterId: "u2",
    connectionId: "u6",
    connectionName: "kafka_conn",
    kafkaTopic: "sink_topic",
    createdAt: new Date(),
    ...overrides,
  };
}

function setupSinkDetailPage() {
  const sink = buildSink();
  const validUseSinksHandler = buildSqlQueryHandlerV2({
    queryKey: sinkQueryKeys.list(),
    results: mapKyselyToTabular({
      columns: sinkListColumns,
      rows: [sink],
    }),
  });

  const validUseSinkErrorsHandler = buildSqlQueryHandlerV2({
    queryKey: sinkQueryKeys.errors({
      sinkId: sink.id,
      timePeriodMinutes: DEFAULT_SINK_ERRORS_TIME_PERIOD,
    }),
    results: mapKyselyToTabular({
      rows: [],
      columns: sinkErrorsColumns,
    }),
  });

  const validUseShowCreateHandler = buildUseSqlQueryHandler({
    type: "SHOW CREATE" as const,
    name: sink.name,
    createSql: "CREATE SINK ...",
  });

  const isOwnerResponse = buildSqlQueryHandlerV2({
    queryKey: objectExplorerQueryKeys.isOwner({
      objectId: sink.id,
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

  server.use(validUseSinksHandler);
  server.use(validUseShowCreateHandler);
  server.use(validUseSinkErrorsHandler);
  server.use(isOwnerResponse);

  return {
    sink,
    initialRoute: `/${sink.databaseName}/schemas/${sink.schemaName}/sinks/${sink.name}/${sink.id}/errors`,
  };
}

function renderSinkDetails(initialRouterEntries: string[]) {
  return renderComponent(
    <Routes>
      <Route path=":databaseName?/schemas?/:schemaName?/:objectType?/:objectName?/:id?">
        <Route
          index
          path="*"
          element={
            <RenderWithPathname>
              <SinkDetailRoutes />
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

describe("SinkDetailRoutes", () => {
  describe("SinkErrors", () => {
    it("renders an error state when sink errors fail to fetch", async () => {
      const { initialRoute, sink } = setupSinkDetailPage();

      const useSinkErrorsHandler = buildSqlQueryHandlerV2({
        queryKey: sinkQueryKeys.errors({
          sinkId: sink.id,
          timePeriodMinutes: DEFAULT_SINK_ERRORS_TIME_PERIOD,
        }),
        results: {
          notices: [],
          error: {
            message: "Something went wrong",
            code: ErrorCode.INTERNAL_ERROR,
          },
        },
      });

      server.use(useSinkErrorsHandler);

      renderSinkDetails([initialRoute]);
      expect(await screen.findByText(SINKS_FETCH_ERROR_MESSAGE)).toBeVisible();
    });

    it("renders an empty state when there are no sink errors", async () => {
      const { initialRoute } = setupSinkDetailPage();

      renderSinkDetails([initialRoute]);

      expect(
        await screen.findByText("No errors during this time period."),
      ).toBeVisible();
    });

    it("renders a list of sink errors", async () => {
      const { initialRoute, sink } = setupSinkDetailPage();
      const mockTimestamp = 1692123417788;

      const validUseSinkErrorsHandler = buildSqlQueryHandlerV2({
        queryKey: sinkQueryKeys.errors({
          sinkId: sink.id,
          timePeriodMinutes: DEFAULT_SINK_ERRORS_TIME_PERIOD,
        }),
        results: mapKyselyToTabular({
          rows: [
            {
              lastOccurred: mockTimestamp.toString(),
              error: "error_1",
              count: "1",
            },
          ],
          columns: sinkErrorsColumns,
        }),
      });

      server.use(validUseSinkErrorsHandler);

      renderSinkDetails([initialRoute]);

      expect(await screen.findByText("error_1")).toBeVisible();
      expect(await screen.findByText("1")).toBeVisible();
      expect(
        await screen.findByText(
          formatDate(new Date(mockTimestamp), DATE_FORMAT_SHORT),
        ),
      ).toBeVisible();
    });
  });

  describe("Redirect", () => {
    it("shows sink details", async () => {
      const { sink } = setupSinkDetailPage();

      renderSinkDetails([
        `/materialize/schemas/public/sinks/kafka_sink/${sink.id}`,
      ]);

      expect(
        await screen.findByText(
          `/materialize/schemas/public/sinks/kafka_sink/${sink.id}`,
        ),
      ).toBeVisible();
    });
  });
});
