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
import { allRoles } from "~/store/allRoles";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { MSW_HANDLER_LOADING_WAIT_TIME, renderComponent } from "~/test/utils";

import { userQueryKeys } from "./queries";
import { UsersListPage } from "./UsersListPage";

const usersColumns = buildColumns(["id", "user_name", "roles_granted"]);

const defaultUsers = [
  {
    id: "u1",
    user_name: "alice@example.com",
    roles_granted: ["admin", "developer"],
  },
  {
    id: "u2",
    user_name: "bob@example.com",
    roles_granted: ["developer"],
  },
  {
    id: "u3",
    user_name: "charlie@example.com",
    roles_granted: null,
  },
];

const defaultRoles = [
  { roleId: "r1", roleName: "admin", memberCount: 1, ownedObjectsCount: 0 },
  { roleId: "r2", roleName: "developer", memberCount: 2, ownedObjectsCount: 0 },
];

describe("UsersListPage", () => {
  it("shows loading state initially", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: userQueryKeys.list(),
          results: mapKyselyToTabular({
            columns: usersColumns,
            rows: [],
          }),
        },
        { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME },
      ),
    );

    renderComponent(<UsersListPage />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: defaultRoles }));
      },
    });

    await waitFor(() => {
      expect(screen.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  it("shows error state when users fail to load", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: userQueryKeys.list(),
        results: {
          error: {
            message: "Something went wrong",
            code: ErrorCode.INTERNAL_ERROR,
          },
          notices: [],
        },
      }),
    );

    renderComponent(<UsersListPage />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: defaultRoles }));
      },
    });

    await waitFor(() => {
      expect(
        screen.getByText(
          "Failed to load users. Please try refreshing the page.",
        ),
      ).toBeVisible();
    });
  });

  it("shows empty state when no users exist", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: userQueryKeys.list(),
        results: mapKyselyToTabular({
          columns: usersColumns,
          rows: [],
        }),
      }),
    );

    renderComponent(<UsersListPage />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: defaultRoles }));
      },
    });

    expect(await screen.findByText("No users found")).toBeVisible();
  });

  it("renders the users table with roles", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: userQueryKeys.list(),
        results: mapKyselyToTabular({
          columns: usersColumns,
          rows: defaultUsers,
        }),
      }),
    );

    renderComponent(<UsersListPage />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: defaultRoles }));
      },
    });

    expect(await screen.findByText("alice@example.com")).toBeVisible();
    expect(await screen.findByText("bob@example.com")).toBeVisible();
    expect(await screen.findByText("charlie@example.com")).toBeVisible();
    expect(screen.getByTestId("users-list-table")).toBeVisible();

    // Check roles are displayed as comma-separated text
    expect(screen.getByText("admin, developer")).toBeVisible();
    expect(screen.getByText("developer")).toBeVisible();
    expect(screen.getByText("No roles assigned")).toBeVisible();
  });
});
