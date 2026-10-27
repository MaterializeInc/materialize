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

import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { showCreateQueryKey } from "~/queries/showCreate";
import { renderComponent } from "~/test/utils";

import { ConnectionDetails } from "./ConnectionDetails";
import { objectExplorerQueryKeys } from "./queries";

const MOCK_CONNECTION = {
  databaseName: "materialize",
  schemaName: "public",
  id: "u1",
  name: "test_connection",
};

const OBJECT_DETAIL_COLUMNS = [
  buildColumn({ name: "schemaName" }),
  buildColumn({ name: "databaseName" }),
  buildColumn({ name: "id" }),
  buildColumn({ name: "name" }),
  buildColumn({ name: "type" }),
  buildColumn({ name: "owner" }),
  buildColumn({ name: "createdAt", type_oid: MzDataType.timestamp }),
];

const CONNECTION_DEPENDENCIES_COLUMNS = [
  buildColumn({ name: "id" }),
  buildColumn({ name: "name" }),
  buildColumn({ name: "type" }),
  buildColumn({ name: "subType" }),
  buildColumn({ name: "databaseName" }),
  buildColumn({ name: "schemaName" }),
];

const SHOW_CREATE_COLUMNS = [buildColumn({ name: "sql" })];

describe("ConnectionDetailsContainer", () => {
  it("shows a spinner initially", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: objectExplorerQueryKeys.objectDetails({
            databaseName: "materialize",
            schemaName: "public",
            name: "test_connection",
          }),
          results: mapKyselyToTabular({
            columns: OBJECT_DETAIL_COLUMNS,
            rows: [],
          }),
        },
        {
          waitTimeMs: "infinite",
        },
      ),
    );
    renderComponent(
      <ConnectionDetails
        databaseName={MOCK_CONNECTION.databaseName}
        schemaName={MOCK_CONNECTION.schemaName}
        objectName={MOCK_CONNECTION.name}
        id={MOCK_CONNECTION.id}
      />,
    );

    await waitFor(() => {
      expect(screen.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  it("shows an error state when object details fails to load", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: objectExplorerQueryKeys.objectDetails({
          databaseName: "materialize",
          schemaName: "public",
          name: "test_connection",
        }),
        results: {
          error: {
            code: ErrorCode.INTERNAL_ERROR,
            message: "An error occurred",
          },
          notices: [],
        },
      }),
    );
    renderComponent(
      <ConnectionDetails
        databaseName={MOCK_CONNECTION.databaseName}
        schemaName={MOCK_CONNECTION.schemaName}
        objectName={MOCK_CONNECTION.name}
        id={MOCK_CONNECTION.id}
      />,
    );

    await waitFor(() => {
      expect(
        screen.getByText("An error occurred loading object details."),
      ).toBeVisible();
    });
  });

  it("renders details about the connection successfully", async () => {
    server.use(
      // details handler
      buildSqlQueryHandlerV2({
        queryKey: objectExplorerQueryKeys.objectDetails({
          databaseName: "materialize",
          schemaName: "public",
          name: "test_connection",
        }),
        results: mapKyselyToTabular({
          columns: OBJECT_DETAIL_COLUMNS,
          rows: [
            {
              databaseName: MOCK_CONNECTION.databaseName,
              schemaName: MOCK_CONNECTION.schemaName,
              id: MOCK_CONNECTION.id,
              name: MOCK_CONNECTION.name,
              type: "connection",
              owner: "jun@materialize.com",
              createdAt: "0",
            },
          ],
        }),
      }),
      // connection dependency table handler
      buildSqlQueryHandlerV2({
        queryKey: objectExplorerQueryKeys.connectionDependencies({
          connectionId: MOCK_CONNECTION.id,
        }),
        results: mapKyselyToTabular({
          columns: CONNECTION_DEPENDENCIES_COLUMNS,
          rows: [
            {
              id: "u1",
              name: "my_sink",
              type: "sink",
              subType: "kafka",
              databaseName: MOCK_CONNECTION.databaseName,
              schemaName: MOCK_CONNECTION.schemaName,
            },
            {
              id: "u2",
              name: "my_sink_2",
              type: "sink",
              subType: "kafka",
              databaseName: MOCK_CONNECTION.databaseName,
              schemaName: MOCK_CONNECTION.schemaName,
            },
          ],
        }),
      }),
      // useShowCreate handler
      buildSqlQueryHandlerV2({
        queryKey: showCreateQueryKey({
          objectType: "connection",
          object: {
            databaseName: MOCK_CONNECTION.databaseName,
            schemaName: MOCK_CONNECTION.schemaName,
            name: MOCK_CONNECTION.name,
          },
        }),
        results: mapKyselyToTabular({
          columns: SHOW_CREATE_COLUMNS,
          rows: [
            {
              sql: "CREATE CONNECTION test_connection TO KAFKA (BROKER 'localhost:9092', SECURITY PROTOCOL PLAINTEXT);",
            },
          ],
        }),
      }),
    );

    renderComponent(
      <ConnectionDetails
        databaseName={MOCK_CONNECTION.databaseName}
        schemaName={MOCK_CONNECTION.schemaName}
        objectName={MOCK_CONNECTION.name}
        id={MOCK_CONNECTION.id}
      />,
    );

    await waitFor(() => {
      // Details loaded successfully
      expect(screen.getByText("Connection")).toBeVisible();

      // show create statement loaded successfully
      expect(screen.getByText(/CREATE CONNECTION/)).toBeVisible();

      // connection dependencies table loaded successfully
      expect(screen.getByText(/my_sink_2/)).toBeVisible();
    });
  });
});
