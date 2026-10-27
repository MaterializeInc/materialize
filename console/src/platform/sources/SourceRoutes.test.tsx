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

import { Source } from "~/api/materialize/source/sourceList";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";

import { SOURCES_FETCH_ERROR_MESSAGE } from "./constants";
import { sourceQueryKeys } from "./queries";
import SourceRoutes from "./SourceRoutes";

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

const emptySourceListHandler = buildSqlQueryHandlerV2({
  queryKey: sourceQueryKeys.list({ filters: {} }),
  results: mapKyselyToTabular({
    rows: [],
    columns: useSourceListColumns,
  }),
});

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
    connectionName: "pogres_connection",
    kafkaTopic: null,
    webhookUrl: null,
    isOwner: true,
    ...overrides,
  };
}

describe("SourceRoutes", () => {
  describe("SourcesList", () => {
    it("shows a spinner initially", async () => {
      server.use(emptySourceListHandler);
      renderComponent(<SourceRoutes />, {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      });

      expect(await screen.findByText("Sources")).toBeVisible();
      waitFor(() => {
        expect(screen.getByTestId("loading-spinner")).toBeVisible();
      });
    });

    it("shows an error state when sources fail to fetch", async () => {
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: sourceQueryKeys.list({ filters: {} }),
          results: {
            error: {
              message: "Something went wrong",
              code: ErrorCode.INTERNAL_ERROR,
            },
            notices: [],
          },
        }),
      );
      renderComponent(<SourceRoutes />, {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      });
      expect(
        await screen.findByText(SOURCES_FETCH_ERROR_MESSAGE),
      ).toBeVisible();
    });

    it("shows the empty state when there are no results", async () => {
      server.use(emptySourceListHandler);
      renderComponent(<SourceRoutes />, {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      });

      expect(await screen.findByText("No available sources")).toBeVisible();
    });

    it("renders the source list", async () => {
      const source = buildPostgresSource();
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: sourceQueryKeys.list({ filters: {} }),
          results: mapKyselyToTabular({
            rows: [source],
            columns: useSourceListColumns,
          }),
        }),
      );
      renderComponent(<SourceRoutes />, {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
      });
      expect(await screen.findByText("default.public")).toBeVisible();
      expect(await screen.findByText("test_source")).toBeVisible();
      expect(await screen.findByText("Stalled")).toBeVisible();
      expect(await screen.findByText("Postgres")).toBeVisible();
    });
  });
});
