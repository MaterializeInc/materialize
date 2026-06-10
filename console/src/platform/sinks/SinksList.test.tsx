// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import React from "react";

import { Sink } from "~/api/materialize/sink/sinkList";
import { ErrorCode } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { MSW_HANDLER_LOADING_WAIT_TIME, renderComponent } from "~/test/utils";

import { SINKS_FETCH_ERROR_MESSAGE } from "./constants";
import { sinkQueryKeys } from "./queries";
import SinksList from "./SinksList";

vi.mock("~/platform/sinks/SinkErrorsGraph", () => ({
  default: function () {
    return <div>Sink Errors Graph</div>;
  },
}));

const useSinkListColumns = buildColumns([
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

const emptySinksResponse = buildSqlQueryHandlerV2({
  queryKey: sinkQueryKeys.list(),
  results: mapKyselyToTabular({
    columns: useSinkListColumns,
    rows: [],
  }),
});

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

describe("SinksList", () => {
  describe("SinksList", () => {
    it("shows a spinner initially", async () => {
      server.use(
        buildSqlQueryHandlerV2(
          {
            queryKey: sinkQueryKeys.list(),
            results: mapKyselyToTabular({
              columns: useSinkListColumns,
              rows: [],
            }),
          },
          { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME }, // ensure we have time to assert on the spinner
        ),
      );

      renderComponent(<SinksList />);

      expect(await screen.findByText("Sinks")).toBeVisible();
      await waitFor(() => {
        expect(screen.getByTestId("loading-spinner")).toBeVisible();
      });
    });

    it("shows an error state when sinks fail to fetch", async () => {
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: sinkQueryKeys.list(),
          results: {
            error: {
              message: "Something went wrong",
              code: ErrorCode.INTERNAL_ERROR,
            },
            notices: [],
          },
        }),
      );
      renderComponent(<SinksList />);

      expect(await screen.findByText("Sinks")).toBeVisible();
      // Use a waitFor to work around a sporadic flake with Suspense rendering
      // the loading state before the error is rendered.
      await waitFor(
        () => expect(screen.getByText(SINKS_FETCH_ERROR_MESSAGE)).toBeVisible(),
        { timeout: 3000 },
      );
    });

    it("shows the empty state when there are no results", async () => {
      server.use(emptySinksResponse);
      renderComponent(<SinksList />);

      expect(await screen.findByText("No available sinks")).toBeVisible();
    });

    it("renders the sink list", async () => {
      const sink = buildSink();
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: sinkQueryKeys.list(),
          results: mapKyselyToTabular({
            columns: useSinkListColumns,
            rows: [sink],
          }),
        }),
      );
      renderComponent(<SinksList />);

      expect(
        await screen.findByText(`${sink.databaseName}.${sink.schemaName}`),
      ).toBeVisible();
      expect(await screen.findByText("json_sink")).toBeVisible();
      expect(await screen.findByText("Running")).toBeVisible();
      expect(await screen.findByText("Kafka")).toBeVisible();
    });
  });
});
