// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { Column } from "~/api/materialize/object-explorer/objectColumns";
import { DatabaseObject } from "~/api/materialize/objects";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { showCreateQueryKey } from "~/queries/showCreate";
import { allObjects } from "~/store/allObjects";
import { mockSubscribeState } from "~/test/mockSubscribe";
import {
  createProviderWrapper,
  MSW_HANDLER_LOADING_WAIT_TIME,
} from "~/test/utils";

import { ObjectExplorerDetailRoutes } from "./ObjectExplorerDetailRoutes";
import { objectExplorerQueryKeys } from "./queries";

const renderComponent = async () => {
  const Wrapper = await createProviderWrapper({});
  return render(
    <Wrapper>
      <ObjectExplorerDetailRoutes />
    </Wrapper>,
  );
};

function buildColumn(overrides?: Partial<Column>): Column {
  return {
    name: "user_id",
    type: "integer",
    nullable: false,
    columnComment: null,
    relationComment: null,
    ...overrides,
  };
}

export const databaseDetailsColumns = buildColumns([
  "id",
  "name",
  "owner",
  "createdAt",
]);

export const schemaDetailsColumns = buildColumns([
  "id",
  "name",
  "databaseName",
  { name: "createdAt", type_oid: MzDataType.timestamptz },
  { name: "isOwner", type_oid: MzDataType.bool },
  "owner",
]);

const databaseDetailsResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.databaseDetails({
    name: "materialize",
  }),
  results: mapKyselyToTabular({
    columns: databaseDetailsColumns,
    rows: [
      {
        name: "my_db",
        owner: "some_user",
        createdAt: new Date("1/1/2024"),
      },
    ],
  }),
});
const failedDatabaseDetailsResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.databaseDetails({
    name: "materialize",
  }),
  results: {
    notices: [],
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
  },
});

const schemaDetailsResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.schemaDetails({
    databaseName: "materialize",
    name: "test_schema",
  }),
  results: mapKyselyToTabular({
    columns: schemaDetailsColumns,
    rows: [
      {
        id: "u123",
        name: "test_schema",
        databaseName: "test_database",
        createdAt: new Date("1/1/2024"),
        isOwner: true,
        owner: "some_user",
      },
    ],
  }),
});

const failedSchemaDetailsResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.schemaDetails({
    databaseName: "materialize",
    name: "test_schema",
  }),
  results: {
    notices: [],
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
  },
});

export const objectDetailsColumns = buildColumns([
  "id",
  "name",
  "type",
  "schemaName",
  "databaseName",
  "owner",
  "createdAt",
]);

const OBJECT_ID = "u123";
const testTable = {
  id: OBJECT_ID,
  name: "test_table",
  type: "table",
  schemaName: "public",
  databaseName: "materialize",
  owner: "some_user",
  createdAt: new Date("1/1/2024"),
};
const testTableSubscribe: DatabaseObject = {
  databaseName: "materialize",
  databaseId: "u1",
  name: "test_table",
  schemaName: "public",
  schemaId: "u2",
  id: OBJECT_ID,
  objectType: "table",
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u1",
  clusterName: "quickstart",
};

const objectDetailsResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.objectDetails({
    databaseName: "materialize",
    schemaName: "public",
    name: "test_table",
  }),
  results: mapKyselyToTabular({
    columns: objectDetailsColumns,
    rows: [testTable],
  }),
});

const loadingObjectDetailsResponse = buildSqlQueryHandlerV2(
  {
    queryKey: objectExplorerQueryKeys.objectDetails({
      databaseName: "materialize",
      schemaName: "public",
      name: "test_table",
    }),
    results: [],
  },
  { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME }, // ensure we have time to assert on the spinner
);

const failedObjectDetailsResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.objectDetails({
    databaseName: "materialize",
    schemaName: "public",
    name: "test_table",
  }),
  results: {
    notices: [],
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
  },
});

const successfulShowCreateReponse = buildSqlQueryHandlerV2({
  queryKey: showCreateQueryKey({
    objectType: "table",
    object: {
      databaseName: "materialize",
      schemaName: "public",
      name: "test_table",
    },
  }),
  results: mapKyselyToTabular({
    columns: buildColumns(["sql"]),
    rows: [
      {
        sql: "CREATE TABLE test_table (id int)",
      },
    ],
  }),
});

const permissionErrorShowCreateReponse = buildSqlQueryHandlerV2({
  queryKey: showCreateQueryKey({
    objectType: "table",
    object: {
      databaseName: "materialize",
      schemaName: "public",
      name: "test_table",
    },
  }),
  results: {
    notices: [],
    error: {
      message: "permission denied for TABLE",
      code: ErrorCode.INSUFFICIENT_PRIVILEGE,
    },
  },
});

export const objectColumnsColumns = buildColumns([
  "id",
  "name",
  "type",
  { name: "nullable", type_oid: MzDataType.bool },
]);

const failedColumnsResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.columns({
    databaseName: "materialize",
    schemaName: "public",
    name: "test_table",
  }),
  results: {
    notices: [],
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
  },
});

const loadingColumnsResponse = buildSqlQueryHandlerV2(
  {
    queryKey: objectExplorerQueryKeys.columns({
      databaseName: "materialize",
      schemaName: "public",
      name: "test_table",
    }),
    results: [],
  },
  { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME }, // ensure we have time to assert on the spinner
);

const columnsResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.columns({
    databaseName: "materialize",
    schemaName: "public",
    name: "test_table",
  }),
  results: mapKyselyToTabular({
    columns: objectColumnsColumns,
    rows: [
      buildColumn(),
      buildColumn({
        name: "user_name",
        type: "text",
        nullable: true,
      }),
    ],
  }),
});

const isOwnerResponse = buildSqlQueryHandlerV2({
  queryKey: objectExplorerQueryKeys.isOwner({
    objectId: OBJECT_ID,
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

describe("ObjectExplorerDetailRoutes", () => {
  beforeEach(() => {
    const store = getStore();
    store.set(allObjects, mockSubscribeState({ data: [testTableSubscribe] }));
  });

  describe("DatabaseDetails", () => {
    beforeEach(() => {
      history.pushState(undefined, "", "/materialize");
    });

    it("shows a spinner initially", async () => {
      await renderComponent();
      expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    });

    it("shows the database details page", async () => {
      server.use(databaseDetailsResponse);
      await renderComponent();

      const content = await screen.findByTestId("main-content");
      expect(within(content).getByText("Database")).toBeVisible();
      expect(within(content).getByText("my_db")).toBeVisible();
      expect(within(content).getByText("some_user")).toBeVisible();
    });

    it("shows an error state when details fail to load", async () => {
      server.use(failedDatabaseDetailsResponse);
      await renderComponent();
      expect(
        await screen.findByText("An error occurred loading database details."),
      ).toBeVisible();
    });
  });

  describe("SchemaDetails", () => {
    beforeEach(() => {
      history.pushState(undefined, "", "/materialize/schemas/test_schema");
    });

    it("shows a spinner initially", async () => {
      server.use(
        buildSqlQueryHandlerV2(
          {
            queryKey: objectExplorerQueryKeys.schemaDetails({
              databaseName: "materialize",
              name: "test_schema",
            }),
            results: [],
          },
          { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME },
        ),
      );
      await renderComponent();
      expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    });

    it("shows the database details page", async () => {
      server.use(schemaDetailsResponse);
      await renderComponent();

      const content = await screen.findByTestId("main-content");
      expect(within(content).getByText("Schema")).toBeVisible();
      expect(within(content).getByText("test_schema")).toBeVisible();
      expect(within(content).getByText("some_user")).toBeVisible();
    });

    it("shows an error state when details fail to load", async () => {
      server.use(failedSchemaDetailsResponse);
      await renderComponent();
      expect(
        await screen.findByText("An error occurred loading schema details."),
      ).toBeVisible();
    });
  });

  describe("ObjectDetails", () => {
    beforeEach(() => {
      server.use(isOwnerResponse);
      history.pushState(
        undefined,
        "",
        `/materialize/schemas/public/tables/test_table/${OBJECT_ID}`,
      );
    });

    it("shows a spinner initially", async () => {
      server.use(loadingObjectDetailsResponse);
      await renderComponent();
      expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    });

    it("shows the details page", async () => {
      server.use(objectDetailsResponse, successfulShowCreateReponse);
      await renderComponent();

      const content = await screen.findByTestId("main-content");
      expect(within(content).getByText("Table")).toBeVisible();
      expect(within(content).getByText("test_table")).toBeVisible();
      expect(within(content).getByText("some_user")).toBeVisible();
      expect(
        within(content).getByText("CREATE TABLE test_table (id int)"),
      ).toBeVisible();
    });

    it("shows a notice for SHOW CREATE permisson error", async () => {
      server.use(objectDetailsResponse, permissionErrorShowCreateReponse);
      await renderComponent();

      const content = await screen.findByTestId("main-content");
      expect(within(content).getByText("Table")).toBeVisible();
      expect(within(content).getByText("test_table")).toBeVisible();
      expect(within(content).getByText("some_user")).toBeVisible();
      expect(
        within(content).getByText(
          "You don't have usage permission on this schema.",
        ),
      ).toBeVisible();
    });

    it("shows an error state when details fail to load", async () => {
      server.use(failedObjectDetailsResponse);
      await renderComponent();
      expect(
        await screen.findByText("An error occurred loading object details."),
      ).toBeVisible();
    });

    it("has an overflow menu with a delete button", async () => {
      server.use(objectDetailsResponse);
      const user = userEvent.setup();
      await renderComponent();
      await user.click(
        await screen.findByRole("button", { name: "More actions" }),
      );
      expect(
        await screen.findByRole("menuitem", { name: "Drop table" }),
      ).toBeVisible();
    });
  });

  describe("ObjectColumns", () => {
    beforeEach(() => {
      server.use(isOwnerResponse);
      history.pushState(
        undefined,
        "",
        `/materialize/schemas/public/tables/test_table/${OBJECT_ID}/columns`,
      );
    });

    it("shows a spinner initially", async () => {
      server.use(loadingColumnsResponse);
      await renderComponent();
      expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    });

    it("shows the columns page", async () => {
      server.use(columnsResponse);
      await renderComponent();

      expect(await screen.findByText("2 Columns")).toBeVisible();
      expect(await screen.findByText("user_id")).toBeVisible();
      expect(await screen.findByText("false")).toBeVisible();
      expect(await screen.findByText("integer")).toBeVisible();
      expect(await screen.findByText("user_name")).toBeVisible();
      expect(await screen.findByText("true")).toBeVisible();
      expect(await screen.findByText("text")).toBeVisible();
    });

    it("shows an error state when columns fail to load", async () => {
      server.use(failedColumnsResponse);
      await renderComponent();
      expect(
        await screen.findByText("An error occurred loading object columns."),
      ).toBeVisible();
    });
  });

  describe("ObjectIndexes", () => {
    beforeEach(() => {
      server.use(isOwnerResponse);
      history.pushState(
        undefined,
        "",
        `/materialize/schemas/public/tables/test_table/${OBJECT_ID}/indexes`,
      );
    });

    it("shows a spinner initially", async () => {
      server.use(
        buildSqlQueryHandlerV2(
          {
            queryKey: objectExplorerQueryKeys.indexes({
              databaseName: "materialize",
              schemaName: "public",
              name: "test_table",
            }),
            results: [],
          },
          { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME }, // ensure we have time to assert on the spinner
        ),
      );
      await renderComponent();
      expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    });

    it("shows the indexes page", async () => {
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: objectExplorerQueryKeys.indexes({
            databaseName: "materialize",
            schemaName: "public",
            name: "test_table",
          }),
          results: [
            mapKyselyToTabular({
              columns: buildColumns([
                {
                  name: "createdAt",
                  type_oid: MzDataType.timestamptz,
                },
              ]),
              rows: [
                {
                  id: "u22",
                  name: "test_table_primary_idx",
                  databaseName: "materialize",
                  schemaName: "public",
                  owner: "jun@materialize.com",
                  createdAt: new Date("Sep. 03, 1999").getTime().toString(),
                  indexedColumns: ["id"],
                },
              ],
            }),
          ],
        }),
      );
      await renderComponent();

      expect(
        await screen.findByText("test_table_primary_idx (id)"),
      ).toBeVisible();
      expect(await screen.findByText("jun@materialize.com")).toBeVisible();
      expect(await screen.findByText(/Sep. 03, 1999/)).toBeVisible();
    });

    it("shows an error state when columns fail to load", async () => {
      server.use(
        buildSqlQueryHandlerV2({
          queryKey: objectExplorerQueryKeys.indexes({
            databaseName: "materialize",
            schemaName: "public",
            name: "test_table",
          }),
          results: {
            error: {
              code: ErrorCode.INTERNAL_ERROR,
              message: "Something went wrong",
            },
            notices: [],
          },
        }),
      );

      await renderComponent();
      expect(
        await screen.findByText("An error occurred loading indexes."),
      ).toBeVisible();
    });
  });
});
