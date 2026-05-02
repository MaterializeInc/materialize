// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { ErrorCode } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { allRoles } from "~/store/allRoles";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent, RenderWithPathname } from "~/test/utils";

import { roleQueryKeys } from "../queries";
import { CreateRoleModal } from "./CreateRoleModal";

const rolesListColumns = buildColumns(["roleName", "memberCount"]);

function renderCreateRoleModal(
  options?: Parameters<typeof renderComponent>[1],
) {
  return renderComponent(
    <RenderWithPathname>
      <Routes>
        <Route path="/roles">
          <Route path="new" element={<CreateRoleModal />} />
          <Route index element={<div>Roles List</div>} />
        </Route>
      </Routes>
    </RenderWithPathname>,
    {
      ...options,
      initialRouterEntries: ["/roles/new"],
    },
  );
}

describe("CreateRoleModal", () => {
  beforeEach(() => {
    // Mock roleQueryKeys.list() for the roles dropdown
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.list(),
        results: mapKyselyToTabular({
          columns: rolesListColumns,
          rows: [
            { roleName: "engineer", memberCount: 3 },
            { roleName: "analyst", memberCount: 2 },
          ],
        }),
      }),
    );
  });

  afterEach(() => {
    // Reset MSW handlers to prevent handler accumulation between tests
    server.resetHandlers();
  });

  it("renders modal with 'Create Role' title and wizard step", async () => {
    renderCreateRoleModal();

    await waitFor(() => {
      expect(screen.getByText("Create Role")).toBeVisible();
    });
    expect(screen.getByText("Configure your role")).toBeInTheDocument();
    expect(screen.getByText("Step 1")).toBeInTheDocument();
  });

  it("form submission success: fill name, submit, toast shown, navigates back", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.createRole({ name: "new-role" }),
        results: {
          ok: "CREATE ROLE",
          notices: [],
        },
      }),
    );

    const user = userEvent.setup();
    renderCreateRoleModal();

    await waitFor(() => {
      expect(screen.getByText("Create Role")).toBeVisible();
    });

    const nameInput = screen.getByPlaceholderText("e.g. db-engineer");
    await user.type(nameInput, "new-role");

    const submitButton = screen.getByRole("button", { name: /Create role/ });
    await user.click(submitButton);

    // Should navigate back to /roles
    await waitFor(() => {
      expect(screen.getByText("Roles List")).toBeVisible();
    });

    // Check that toast message appears (text is split by span)
    await waitFor(() => {
      expect(
        screen.getByText(/role was successfully created/),
      ).toBeInTheDocument();
    });
  });

  it("form submission error: submit fails, error shown, modal stays open", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.createRole({ name: "new-role" }),
        results: {
          error: {
            message: "permission denied",
            code: ErrorCode.INTERNAL_ERROR,
          },
          notices: [],
        },
      }),
    );

    const user = userEvent.setup();
    renderCreateRoleModal();

    await waitFor(() => {
      expect(screen.getByText("Create Role")).toBeVisible();
    });

    const nameInput = screen.getByPlaceholderText("e.g. db-engineer");
    await user.type(nameInput, "new-role");

    const submitButton = screen.getByRole("button", { name: /Create role/ });
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByText("permission denied")).toBeVisible();
    });

    // Modal should stay open (still on /roles/new)
    expect(screen.getByText("Create Role")).toBeVisible();
    expect(screen.getByTestId("pathname")).toHaveTextContent("/roles/new");
  });

  it("close/cancel button navigates back", async () => {
    const user = userEvent.setup();
    renderCreateRoleModal();

    await waitFor(() => {
      expect(screen.getByText("Create Role")).toBeVisible();
    });

    const cancelButton = screen.getByRole("button", { name: "Back" });
    await user.click(cancelButton);

    // Should navigate back to /roles
    await waitFor(() => {
      expect(screen.getByText("Roles List")).toBeVisible();
    });
  });

  it("submit button disabled when name is empty", async () => {
    renderCreateRoleModal();

    await waitFor(() => {
      expect(screen.getByText("Create Role")).toBeVisible();
    });

    const submitButton = screen.getByRole("button", { name: /Create role/ });
    expect(submitButton).toBeDisabled();
  });

  it("submit button shows loading state during submission", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: roleQueryKeys.createRole({ name: "new-role" }),
          results: {
            ok: "CREATE ROLE",
            notices: [],
          },
        },
        { waitTimeMs: 1000 },
      ),
    );

    const user = userEvent.setup();
    renderCreateRoleModal();

    await waitFor(() => {
      expect(screen.getByText("Create Role")).toBeVisible();
    });

    const nameInput = screen.getByPlaceholderText("e.g. db-engineer");
    await user.type(nameInput, "new-role");

    const submitButton = screen.getByRole("button", { name: /Create role/ });
    await user.click(submitButton);

    // Button should be disabled during submission
    await waitFor(() => {
      expect(submitButton).toBeDisabled();
    });
  });

  it("handles partial failures: role created but grants fail", async () => {
    const engineerRole = { id: "engineer", name: "engineer" };
    const analystRole = { id: "analyst", name: "analyst" };

    const user = userEvent.setup();
    await renderCreateRoleModal({
      initializeState: ({ set }) => {
        set(
          allRoles,
          mockSubscribeState({
            data: [
              {
                roleId: "r1",
                roleName: "engineer",
                memberCount: 3,
                ownedObjectsCount: 0,
              },
              {
                roleId: "r2",
                roleName: "analyst",
                memberCount: 2,
                ownedObjectsCount: 0,
              },
            ],
          }),
        );
      },
    });

    // Set up mocks AFTER renderComponent so query keys use the initialized store state
    server.use(
      // Mock successful role creation
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.createRole({ name: "new-role" }),
        results: { ok: "CREATE ROLE", notices: [] },
      }),
      // Mock successful inherited role grant for engineer
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.grantInheritedRole({
          name: "new-role",
          inheritedRole: engineerRole,
        }),
        results: { ok: "GRANT", notices: [] },
      }),
      // Mock failed inherited role grant for analyst
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.grantInheritedRole({
          name: "new-role",
          inheritedRole: analystRole,
        }),
        results: {
          error: {
            message: "permission denied for role analyst",
            code: ErrorCode.INTERNAL_ERROR,
          },
          notices: [],
        },
      }),
    );

    await waitFor(() => {
      expect(screen.getByText("Create Role")).toBeVisible();
    });

    const nameInput = screen.getByPlaceholderText("e.g. db-engineer");
    await user.type(nameInput, "new-role");

    // Select inherited roles using the correct aria-label
    const inheritedRolesSelect = screen.getByLabelText(
      "Select role to inherit",
    );

    // Select engineer and wait for it to appear as a selected tag
    await user.click(inheritedRolesSelect);
    await user.click(await screen.findByText("engineer"));
    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Remove engineer" }),
      ).toBeVisible();
    });

    // Select analyst and wait for it to appear as a selected tag
    await user.click(inheritedRolesSelect);
    await user.click(await screen.findByText("analyst"));
    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Remove analyst" }),
      ).toBeVisible();
    });

    const submitButton = screen.getByRole("button", { name: /Create role/ });
    await user.click(submitButton);

    await waitFor(() => {
      expect(
        screen.getByText(/Role created but 1 grant\(s\) failed/),
      ).toBeVisible();
    });

    // Modal stays open showing the partial failure details
    expect(screen.getByText("Create Role")).toBeVisible();

    // The error detail should be visible in the form
    expect(
      screen.getByText(/permission denied for role analyst/),
    ).toBeInTheDocument();
  });
});
