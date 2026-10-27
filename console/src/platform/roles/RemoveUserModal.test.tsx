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

import { roleQueryKeys } from "./queries";
import RemoveUserModal from "./RemoveUserModal";

const mockOnClose = vi.fn();
const mockOnSuccess = vi.fn();

const defaultProps = {
  isOpen: true,
  onClose: mockOnClose,
  onSuccess: mockOnSuccess,
  roleName: "engineer",
  memberName: "user@example.com",
};

describe("RemoveUserModal", () => {
  beforeEach(() => {
    mockOnClose.mockReset();
    mockOnSuccess.mockReset();
  });

  it("renders the confirmation message with role and member names", async () => {
    renderComponent(<RemoveUserModal {...defaultProps} />);

    await waitFor(() => expect(screen.getByText("Remove Role")).toBeVisible());
    expect(
      screen.getByText(/Are you sure you want to remove/),
    ).toBeInTheDocument();
    expect(screen.getByText("user@example.com")).toBeVisible();
    expect(screen.getByText("engineer")).toBeVisible();
  });

  it("calls onClose when Cancel button is clicked", async () => {
    const user = userEvent.setup();
    renderComponent(<RemoveUserModal {...defaultProps} />);

    await user.click(await screen.findByRole("button", { name: "Cancel" }));
    expect(mockOnClose).toHaveBeenCalled();
  });

  it("displays error message when revoke fails", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.revokeRoleMember(),
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
    renderComponent(<RemoveUserModal {...defaultProps} />);

    await user.click(
      await screen.findByRole("button", { name: "Remove role" }),
    );
    expect(
      await screen.findByText(
        "There was an error removing the user: permission denied.",
      ),
    ).toBeVisible();
  });

  it("closes modal and calls onSuccess when revoke succeeds", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.revokeRoleMember(),
        results: {
          ok: "REVOKE ROLE",
          notices: [],
        },
      }),
    );
    const user = userEvent.setup();
    renderComponent(<RemoveUserModal {...defaultProps} />);

    await user.click(
      await screen.findByRole("button", { name: "Remove role" }),
    );
    expect(mockOnClose).toHaveBeenCalled();
  });
});
