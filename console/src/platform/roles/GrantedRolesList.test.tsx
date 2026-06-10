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

import { GrantedRolesList } from "./GrantedRolesList";
import { roleQueryKeys } from "./queries";

const membersColumns = buildColumns([
  "id",
  "roleName",
  "memberName",
  "childRoleName",
  "parentName",
  "childName",
]);

describe("GrantedRolesList", () => {
  it("shows loading state initially", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: roleQueryKeys.grantedRoles({
            roleName: "test_role",
          }),
          results: mapKyselyToTabular({
            columns: membersColumns,
            rows: [],
          }),
        },
        { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME },
      ),
    );

    renderComponent(<GrantedRolesList roleName="test_role" />);

    await waitFor(() => {
      expect(screen.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  it("shows error state when granted roles fail to load", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.grantedRoles({
          roleName: "test_role",
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

    renderComponent(<GrantedRolesList roleName="test_role" />);

    await waitFor(() => {
      expect(
        screen.getByText(
          "Failed to load granted roles. Please try refreshing the page.",
        ),
      ).toBeVisible();
    });
  });

  it("shows empty state when role does not inherit from any roles", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.grantedRoles({
          roleName: "test_role",
        }),
        results: mapKyselyToTabular({
          columns: membersColumns,
          rows: [],
        }),
      }),
    );

    renderComponent(<GrantedRolesList roleName="test_role" />);

    expect(
      await screen.findByText(
        "This role does not inherit from any other roles.",
      ),
    ).toBeVisible();
  });

  it("renders the granted roles table", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.grantedRoles({
          roleName: "test_role",
        }),
        results: mapKyselyToTabular({
          columns: membersColumns,
          rows: [
            {
              id: "r1",
              roleName: "parent_role_1",
              memberName: "test_role",
              childRoleName: "test_role",
              parentName: "parent_role_1",
              childName: "test_role",
            },
            {
              id: "r2",
              roleName: "parent_role_2",
              memberName: "test_role",
              childRoleName: "test_role",
              parentName: "parent_role_2",
              childName: "test_role",
            },
          ],
        }),
      }),
    );

    renderComponent(<GrantedRolesList roleName="test_role" />);

    expect(await screen.findByText("parent_role_1")).toBeVisible();
    expect(await screen.findByText("parent_role_2")).toBeVisible();
    expect(screen.getByTestId("granted-roles-table")).toBeVisible();
  });
});
