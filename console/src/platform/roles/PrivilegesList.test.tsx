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

import { ErrorCode } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { MSW_HANDLER_LOADING_WAIT_TIME, renderComponent } from "~/test/utils";

import { PrivilegesList } from "./PrivilegesList";
import { roleQueryKeys } from "./queries";

const privilegesColumns = buildColumns([
  "grantor",
  "grantee",
  "database",
  "schema",
  "name",
  "object_type",
  "privilege_type",
]);

describe("PrivilegesList", () => {
  it("shows loading state initially", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: roleQueryKeys.privileges({ roleName: "test_role" }),
          results: mapKyselyToTabular({
            columns: privilegesColumns,
            rows: [],
          }),
        },
        { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME },
      ),
    );

    renderComponent(<PrivilegesList roleName="test_role" />);

    await waitFor(() => {
      expect(screen.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  it("shows error state when privileges fail to load", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.privileges({ roleName: "test_role" }),
        results: {
          error: {
            message: "Something went wrong",
            code: ErrorCode.INTERNAL_ERROR,
          },
          notices: [],
        },
      }),
    );

    renderComponent(<PrivilegesList roleName="test_role" />);

    await waitFor(() => {
      expect(
        screen.getByText(
          "Failed to load privileges. Please try refreshing the page.",
        ),
      ).toBeVisible();
    });
  });

  it("shows empty state when only PUBLIC privileges exist", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.privileges({ roleName: "test_role" }),
        results: mapKyselyToTabular({
          columns: privilegesColumns,
          rows: [
            {
              grantor: "mz_system",
              grantee: "PUBLIC",
              database: "materialize",
              schema: "public",
              name: "public_table",
              object_type: "table",
              privilege_type: "SELECT",
            },
          ],
        }),
      }),
    );

    renderComponent(<PrivilegesList roleName="test_role" />);

    expect(
      await screen.findByText("No privileges assigned to this role."),
    ).toBeVisible();
  });

  it("renders the privileges table", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.privileges({ roleName: "test_role" }),
        results: mapKyselyToTabular({
          columns: privilegesColumns,
          rows: [
            {
              grantor: "mz_system",
              grantee: "test_role",
              database: "materialize",
              schema: "public",
              name: "my_table",
              object_type: "table",
              privilege_type: "SELECT",
            },
          ],
        }),
      }),
    );

    renderComponent(<PrivilegesList roleName="test_role" />);

    expect(await screen.findByText("my_table")).toBeVisible();
    expect(await screen.findByText("Table")).toBeVisible();
    expect(await screen.findByText("SELECT")).toBeVisible();
    expect(await screen.findByText("Direct")).toBeVisible();
  });

  it("shows inherited role name when privilege is inherited", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.privileges({ roleName: "test_role" }),
        results: mapKyselyToTabular({
          columns: privilegesColumns,
          rows: [
            {
              grantor: "mz_system",
              grantee: "parent_role",
              database: "materialize",
              schema: "public",
              name: "inherited_table",
              object_type: "table",
              privilege_type: "SELECT",
            },
          ],
        }),
      }),
    );

    renderComponent(<PrivilegesList roleName="test_role" />);

    expect(await screen.findByText("inherited_table")).toBeVisible();
    expect(await screen.findByText("parent_role")).toBeVisible();
  });

  it("filters out PUBLIC privileges", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.privileges({ roleName: "test_role" }),
        results: mapKyselyToTabular({
          columns: privilegesColumns,
          rows: [
            {
              grantor: "mz_system",
              grantee: "PUBLIC",
              database: "materialize",
              schema: "public",
              name: "public_table",
              object_type: "table",
              privilege_type: "SELECT",
            },
            {
              grantor: "mz_system",
              grantee: "test_role",
              database: "materialize",
              schema: "public",
              name: "private_table",
              object_type: "table",
              privilege_type: "SELECT",
            },
          ],
        }),
      }),
    );

    renderComponent(<PrivilegesList roleName="test_role" />);

    expect(await screen.findByText("private_table")).toBeVisible();
    expect(screen.queryByText("public_table")).not.toBeInTheDocument();
  });
});
