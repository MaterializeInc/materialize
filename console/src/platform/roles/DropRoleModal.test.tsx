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
import { renderComponent } from "~/test/utils";

import DropRoleModal from "./DropRoleModal";
import { roleQueryKeys } from "./queries";

const mockOnClose = vi.fn();
const mockOnSuccess = vi.fn();

const defaultProps = {
  isOpen: true,
  onClose: mockOnClose,
  onSuccess: mockOnSuccess,
  roleName: "test_role",
  ownedObjectsCount: 0,
};

describe("DropRoleModal", () => {
  beforeEach(() => {
    mockOnClose.mockReset();
    mockOnSuccess.mockReset();
  });

  it("shows confirmation, drops role on click, and closes modal on success", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.dropRole({ name: "test_role" }),
        results: { ok: "DROP ROLE", notices: [] },
      }),
    );
    const user = userEvent.setup();
    renderComponent(<DropRoleModal {...defaultProps} />);

    // Shows confirmation message
    await waitFor(() => expect(screen.getByText("Drop Role")).toBeVisible());
    expect(
      screen.getByText(/Are you sure you want to drop the role/),
    ).toBeInTheDocument();
    expect(screen.getByText("test_role")).toBeVisible();

    // Drop button is enabled
    const dropButton = screen.getByRole("button", { name: "Drop role" });
    expect(dropButton).not.toBeDisabled();

    // Clicking drop closes modal on success
    await user.click(dropButton);
    await waitFor(() => expect(mockOnClose).toHaveBeenCalled());
    expect(mockOnSuccess).toHaveBeenCalled();
  });

  it("shows warning and disables drop when role owns objects", async () => {
    renderComponent(<DropRoleModal {...defaultProps} ownedObjectsCount={3} />);

    await waitFor(() => expect(screen.getByText("Drop Role")).toBeVisible());

    // Warning is shown
    expect(screen.getByText(/This role owns 3 objects/)).toBeVisible();

    // Instructions are shown
    expect(screen.getByText(/To drop this role, you need to:/)).toBeVisible();
    expect(screen.getByText(/REASSIGN OWNED BY/)).toBeVisible();

    // Drop button is hidden, only Close button is shown
    expect(
      screen.queryByRole("button", { name: "Drop role" }),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Close")).toBeVisible();
  });

  it("shows instructions when drop fails with dependency error", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.dropRole({ name: "test_role" }),
        results: {
          error: {
            message:
              "role test_role cannot be dropped because some objects depend on it",
            code: ErrorCode.DEPENDENT_OBJECTS_STILL_EXIST,
          },
          notices: [],
        },
      }),
    );
    const user = userEvent.setup();
    renderComponent(<DropRoleModal {...defaultProps} />);

    await user.click(await screen.findByRole("button", { name: "Drop role" }));

    // Error message is shown
    await waitFor(() => {
      expect(
        screen.getByText(/cannot be dropped because some objects depend on it/),
      ).toBeVisible();
    });

    expect(screen.getByText(/To drop this role, you need to:/)).toBeVisible();
    // Drop button is hidden after dependency error, only Close button is shown
    expect(
      screen.queryByRole("button", { name: "Drop role" }),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Close")).toBeVisible();
  });

  it("displays error message when drop fails with generic error", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.dropRole({ name: "test_role" }),
        results: {
          error: {
            message: "permission denied",
            code: ErrorCode.INSUFFICIENT_PRIVILEGE,
          },
          notices: [],
        },
      }),
    );
    const user = userEvent.setup();
    renderComponent(<DropRoleModal {...defaultProps} />);

    await user.click(await screen.findByRole("button", { name: "Drop role" }));

    expect(await screen.findByText("permission denied")).toBeVisible();
    // Drop button stays enabled for retry (not a dependency error)
    expect(
      screen.getByRole("button", { name: "Drop role" }),
    ).not.toBeDisabled();
  });
});
