// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import React from "react";

import { SourceTable } from "~/api/materialize/source/sourceTables";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { renderComponent } from "~/test/utils";

import { sourceQueryKeys } from "./queries";
import SourceTables from "./SourceTables";

const sourceTableColumns = buildColumns([
  "id",
  "name",
  "databaseName",
  "schemaName",
  "status",
]);

function buildSourceTable(overrides?: Partial<SourceTable>): SourceTable {
  return {
    id: "u1",
    name: "test_table",
    databaseName: "materialize",
    schemaName: "public",
    status: "running",
    type: "postgres",
    ...overrides,
  };
}

const sourceTable = buildSourceTable();

const emptyResponse = buildSqlQueryHandlerV2({
  queryKey: sourceQueryKeys.sourceTables({ sourceId: sourceTable.id }),
  results: mapKyselyToTabular({
    columns: sourceTableColumns,
    rows: [],
  }),
});

const validResponse = buildSqlQueryHandlerV2({
  queryKey: sourceQueryKeys.sourceTables({ sourceId: sourceTable.id }),
  results: mapKyselyToTabular({
    columns: sourceTableColumns,
    rows: [buildSourceTable()],
  }),
});

describe("SourceTables", () => {
  it("shows the empty state when there are no results", async () => {
    server.use(emptyResponse);
    await renderComponent(<SourceTables sourceId={sourceTable.id} />);

    expect(await screen.findByText("No subsources")).toBeVisible();
  });

  it("await renderComponent the source list", async () => {
    server.use(validResponse);
    await renderComponent(<SourceTables sourceId={sourceTable.id} />);

    expect(await screen.findByText("test_table")).toBeVisible();
    expect(await screen.findByText("Running")).toBeVisible();
  });
});
