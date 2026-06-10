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

import { GrantedToRolesList } from "./GrantedToRolesList";
import { roleQueryKeys } from "./queries";

const membersColumns = buildColumns([
  "id",
  "roleName",
  "memberName",
  "childRoleName",
  "parentName",
  "childName",
]);

describe("GrantedToRolesList", () => {
  it("shows loading state initially", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: roleQueryKeys.members({
            roleName: "test_role",
            memberType: "roles",
          }),
          results: mapKyselyToTabular({
            columns: membersColumns,
            rows: [],
          }),
        },
        { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME },
      ),
    );

    renderComponent(<GrantedToRolesList roleName="test_role" />);

    await waitFor(() => {
      expect(screen.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  it("shows error state when roles fail to load", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.members({
          roleName: "test_role",
          memberType: "roles",
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

    renderComponent(<GrantedToRolesList roleName="test_role" />);

    await waitFor(() => {
      expect(
        screen.getByText(
          "Failed to load roles. Please try refreshing the page.",
        ),
      ).toBeVisible();
    });
  });

  it("shows empty state when no roles inherit from this role", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.members({
          roleName: "test_role",
          memberType: "roles",
        }),
        results: mapKyselyToTabular({
          columns: membersColumns,
          rows: [],
        }),
      }),
    );

    renderComponent(<GrantedToRolesList roleName="test_role" />);

    expect(
      await screen.findByText("No roles inherit from this role."),
    ).toBeVisible();
  });

  it("renders the roles table", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.members({
          roleName: "test_role",
          memberType: "roles",
        }),
        results: mapKyselyToTabular({
          columns: membersColumns,
          rows: [
            {
              id: "r1",
              roleName: "test_role",
              memberName: "child_role_1",
              childRoleName: "child_role_1",
              parentName: "test_role",
              childName: "child_role_1",
            },
            {
              id: "r2",
              roleName: "test_role",
              memberName: "child_role_2",
              childRoleName: "child_role_2",
              parentName: "test_role",
              childName: "child_role_2",
            },
          ],
        }),
      }),
    );

    renderComponent(<GrantedToRolesList roleName="test_role" />);

    expect(await screen.findByText("child_role_1")).toBeVisible();
    expect(await screen.findByText("child_role_2")).toBeVisible();
    expect(screen.getByTestId("granted-to-roles-table")).toBeVisible();
  });
});
