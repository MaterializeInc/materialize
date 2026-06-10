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

import { roleQueryKeys } from "./queries";
import { UsersList } from "./UsersList";

const membersColumns = buildColumns([
  "id",
  "roleName",
  "memberName",
  "childRoleName",
  "parentName",
  "childName",
]);

describe("UsersList", () => {
  it("shows loading state initially", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: roleQueryKeys.members({
            roleName: "test_role",
            memberType: "users",
          }),
          results: mapKyselyToTabular({
            columns: membersColumns,
            rows: [],
          }),
        },
        { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME },
      ),
    );

    renderComponent(<UsersList roleName="test_role" />);

    await waitFor(() => {
      expect(screen.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  it("shows error state when users fail to load", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.members({
          roleName: "test_role",
          memberType: "users",
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

    renderComponent(<UsersList roleName="test_role" />);

    await waitFor(() => {
      expect(
        screen.getByText(
          "Failed to load users. Please try refreshing the page.",
        ),
      ).toBeVisible();
    });
  });

  it("shows empty state when no users are assigned", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.members({
          roleName: "test_role",
          memberType: "users",
        }),
        results: mapKyselyToTabular({
          columns: membersColumns,
          rows: [],
        }),
      }),
    );

    renderComponent(<UsersList roleName="test_role" />);

    expect(
      await screen.findByText("No users assigned to this role."),
    ).toBeVisible();
  });

  it("renders the users table", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.members({
          roleName: "test_role",
          memberType: "users",
        }),
        results: mapKyselyToTabular({
          columns: membersColumns,
          rows: [
            {
              id: "u1",
              roleName: "test_role",
              memberName: "user1@example.com",
              childRoleName: "user1@example.com",
              parentName: "test_role",
              childName: "user1@example.com",
            },
            {
              id: "u2",
              roleName: "test_role",
              memberName: "user2@example.com",
              childRoleName: "user2@example.com",
              parentName: "test_role",
              childName: "user2@example.com",
            },
          ],
        }),
      }),
    );

    renderComponent(<UsersList roleName="test_role" />);

    expect(await screen.findByText("user1@example.com")).toBeVisible();
    expect(await screen.findByText("user2@example.com")).toBeVisible();
    expect(screen.getByTestId("users-table")).toBeVisible();
  });
});
