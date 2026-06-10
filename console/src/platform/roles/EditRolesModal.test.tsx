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

import { ErrorCode } from "~/api/materialize/types";
import { buildSqlQueryHandlerV2 } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { allRoles, RoleItem } from "~/store/allRoles";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent } from "~/test/utils";

import EditRolesModal from "./EditRolesModal";
import { roleQueryKeys } from "./queries";

const mockOnClose = vi.fn();

const mockRoles: RoleItem[] = [
  { roleId: "r1", roleName: "analyst", memberCount: 3, ownedObjectsCount: 0 },
  { roleId: "r2", roleName: "engineer", memberCount: 5, ownedObjectsCount: 0 },
  { roleId: "r3", roleName: "developer", memberCount: 2, ownedObjectsCount: 0 },
  { roleId: "r4", roleName: "viewer", memberCount: 1, ownedObjectsCount: 0 },
];

const defaultProps = {
  isOpen: true,
  onClose: mockOnClose,
  userName: "user@example.com",
  currentRoles: ["analyst", "engineer"],
};

// Test helpers
async function renderModal(roles: RoleItem[] = mockRoles) {
  return renderComponent(<EditRolesModal {...defaultProps} />, {
    initializeState: ({ set }) => {
      set(allRoles, mockSubscribeState({ data: roles }));
    },
  });
}

async function selectRole(roleName: string) {
  await userEvent.click(screen.getByLabelText("Select role to assign"));
  await userEvent.click(await screen.findByText(roleName));
}

async function clickSaveButton() {
  await userEvent.click(screen.getByRole("button", { name: "Save changes" }));
}

function mockGrantSuccess() {
  server.use(
    buildSqlQueryHandlerV2({
      queryKey: roleQueryKeys.grantRoleMember(),
      results: { ok: "GRANT ROLE", notices: [] },
    }),
  );
}

function mockGrantError(message: string) {
  server.use(
    buildSqlQueryHandlerV2({
      queryKey: roleQueryKeys.grantRoleMember(),
      results: {
        error: { message, code: ErrorCode.INTERNAL_ERROR },
        notices: [],
      },
    }),
  );
}

function mockRevokeSuccess() {
  server.use(
    buildSqlQueryHandlerV2({
      queryKey: roleQueryKeys.revokeRoleMember(),
      results: { ok: "REVOKE ROLE", notices: [] },
    }),
  );
}

function mockRevokeError(message: string) {
  server.use(
    buildSqlQueryHandlerV2({
      queryKey: roleQueryKeys.revokeRoleMember(),
      results: {
        error: { message, code: ErrorCode.INTERNAL_ERROR },
        notices: [],
      },
    }),
  );
}

describe("EditRolesModal", () => {
  beforeEach(() => {
    mockOnClose.mockReset();
  });

  it("renders modal, allows adding/removing roles, and saves successfully", async () => {
    mockGrantSuccess();
    mockRevokeSuccess();
    await renderModal();

    // Renders with user info and current roles
    expect(await screen.findByText("Edit Roles")).toBeInTheDocument();
    expect(
      await screen.findByText(/Assign and remove roles for user@example.com/),
    ).toBeInTheDocument();
    expect(await screen.findByText("analyst")).toBeInTheDocument();
    expect(await screen.findByText("engineer")).toBeInTheDocument();

    // Can add a new role
    await selectRole("developer");
    expect(screen.getByText("developer")).toBeVisible();
    expect(screen.getByLabelText("Remove developer")).toBeVisible();

    // Can remove an existing role
    await userEvent.click(screen.getByLabelText("Remove analyst"));
    expect(screen.queryByText("analyst")).not.toBeInTheDocument();

    // Saves successfully and shows toast
    await clickSaveButton();
    await waitFor(() => {
      expect(mockOnClose).toHaveBeenCalled();
    });
    expect(
      await screen.findByText(/Roles were updated for/),
    ).toBeInTheDocument();
    expect(screen.getByText("user@example.com")).toBeInTheDocument();
  });

  it("filters out already selected and system roles from dropdown", async () => {
    const rolesWithSystem: RoleItem[] = [
      ...mockRoles,
      {
        roleId: "sys1",
        roleName: "PUBLIC",
        memberCount: 0,
        ownedObjectsCount: 0,
      },
      {
        roleId: "sys2",
        roleName: "mz_system",
        memberCount: 0,
        ownedObjectsCount: 0,
      },
    ];

    await renderModal(rolesWithSystem);
    await userEvent.click(screen.getByLabelText("Select role to assign"));

    // Shows available roles
    expect(screen.getByText("developer")).toBeVisible();
    expect(screen.getByText("viewer")).toBeVisible();

    // Filters out already selected roles
    const options = Array.from(document.querySelectorAll('[role="option"]'));
    expect(
      options.find((opt) => opt.textContent === "analyst"),
    ).toBeUndefined();
    expect(
      options.find((opt) => opt.textContent === "engineer"),
    ).toBeUndefined();

    // Filters out system roles
    expect(screen.queryByText("PUBLIC")).not.toBeInTheDocument();
    expect(screen.queryByText("mz_system")).not.toBeInTheDocument();
  });

  it("closes modal via cancel or save with no changes", async () => {
    await renderModal();

    // Cancel closes modal
    await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(mockOnClose).toHaveBeenCalledTimes(1);
  });

  it("displays errors when grant or revoke fails", async () => {
    mockGrantError("permission denied");
    mockRevokeError("cannot revoke role");
    await renderModal();

    // Grant failure
    await selectRole("developer");
    await clickSaveButton();
    expect(
      await screen.findByText(/Failed to grant developer: permission denied/),
    ).toBeVisible();
  });
});
