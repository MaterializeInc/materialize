// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import React from "react";

import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { roleQueryKeys } from "~/platform/roles/queries";
import { allRoles, RoleItem } from "~/store/allRoles";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { MSW_HANDLER_LOADING_WAIT_TIME, renderComponent } from "~/test/utils";

import InheritedRolesSection from "./InheritedRolesSection";
import { useCreateRoleForm } from "./useCreateRoleForm";

const rolesColumns = buildColumns(["roleName", "memberCount"]);

const mockRoles: RoleItem[] = [
  { roleId: "r1", roleName: "analyst", memberCount: 3, ownedObjectsCount: 0 },
  { roleId: "r2", roleName: "engineer", memberCount: 5, ownedObjectsCount: 0 },
  { roleId: "r3", roleName: "developer", memberCount: 2, ownedObjectsCount: 0 },
  { roleId: "r4", roleName: "viewer", memberCount: 1, ownedObjectsCount: 0 },
  { roleId: "r5", roleName: "admin", memberCount: 4, ownedObjectsCount: 0 },
];

const FormWrapper = () => {
  const form = useCreateRoleForm();
  return <InheritedRolesSection form={form} />;
};

async function selectRoleFromDropdown(roleName: string) {
  const input = screen.getByLabelText("Select role to inherit");
  await userEvent.click(input);

  const option = await screen.findByText(roleName);
  await userEvent.click(option);
}

describe("InheritedRolesSection", () => {
  beforeEach(() => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.list(),
        results: mapKyselyToTabular({
          columns: rolesColumns,
          rows: mockRoles.map((role) => ({
            roleName: role.roleName,
            memberCount: role.memberCount,
          })),
        }),
      }),
    );
  });

  it("renders section with dropdown", async () => {
    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: mockRoles }));
      },
    });

    expect(
      await screen.findByText("Configure privileges on this role"),
    ).toBeVisible();
    expect(screen.getByText("Inherit from")).toBeVisible();
    expect(screen.getByLabelText("Select role to inherit")).toBeVisible();
  });

  it("populates dropdown with available roles", async () => {
    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: mockRoles }));
      },
    });

    const input = screen.getByLabelText("Select role to inherit");
    await userEvent.click(input);

    await waitFor(() => {
      expect(screen.getByText("analyst")).toBeVisible();
      expect(screen.getByText("engineer")).toBeVisible();
      expect(screen.getByText("developer")).toBeVisible();
      expect(screen.getByText("viewer")).toBeVisible();
      expect(screen.getByText("admin")).toBeVisible();
    });
  });

  it("adds selected role as chip with remove button", async () => {
    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: mockRoles }));
      },
    });

    await selectRoleFromDropdown("analyst");

    await waitFor(() => {
      expect(screen.getByText("analyst")).toBeVisible();
      expect(screen.getByLabelText("Remove analyst")).toBeVisible();
    });
  });

  it("removes role on X button click", async () => {
    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: mockRoles }));
      },
    });

    await selectRoleFromDropdown("developer");

    await waitFor(() => {
      expect(screen.getByText("developer")).toBeVisible();
    });

    const removeButton = screen.getByLabelText("Remove developer");
    await userEvent.click(removeButton);

    await waitFor(() => {
      expect(screen.queryByText("developer")).not.toBeInTheDocument();
    });
  });

  it("supports adding multiple roles", async () => {
    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: mockRoles }));
      },
    });

    await selectRoleFromDropdown("analyst");
    await selectRoleFromDropdown("engineer");
    await selectRoleFromDropdown("viewer");

    await waitFor(() => {
      expect(screen.getByText("analyst")).toBeVisible();
      expect(screen.getByText("engineer")).toBeVisible();
      expect(screen.getByText("viewer")).toBeVisible();
      expect(screen.getByLabelText("Remove analyst")).toBeVisible();
      expect(screen.getByLabelText("Remove engineer")).toBeVisible();
      expect(screen.getByLabelText("Remove viewer")).toBeVisible();
    });
  });

  it("resets dropdown after adding role", async () => {
    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: mockRoles }));
      },
    });

    await selectRoleFromDropdown("analyst");

    await waitFor(() => {
      const input = screen.getByLabelText("Select role to inherit");
      expect(input).toHaveValue("");
    });
  });

  it("shows loading state while fetching roles", async () => {
    server.use(
      buildSqlQueryHandlerV2(
        {
          queryKey: roleQueryKeys.list(),
          results: mapKyselyToTabular({
            columns: rolesColumns,
            rows: mockRoles.map((role) => ({
              roleName: role.roleName,
              memberCount: role.memberCount,
            })),
          }),
        },
        { waitTimeMs: MSW_HANDLER_LOADING_WAIT_TIME },
      ),
    );

    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(
          allRoles,
          mockSubscribeState({ data: [], snapshotComplete: false }),
        );
      },
    });

    await waitFor(() => {
      const input = screen.getByLabelText("Select role to inherit");
      expect(input).toBeInTheDocument();
    });
  });

  it("shows empty state when no roles available", async () => {
    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: [] }));
      },
    });

    const input = screen.getByLabelText("Select role to inherit");
    await userEvent.click(input);

    await waitFor(() => {
      const options = Array.from(document.querySelectorAll('[role="option"]'));
      expect(options).toHaveLength(0);
    });
  });

  it("filters added roles from dropdown options", async () => {
    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: mockRoles }));
      },
    });

    await selectRoleFromDropdown("analyst");
    await selectRoleFromDropdown("engineer");

    await waitFor(() => {
      expect(screen.getByText("analyst")).toBeVisible();
      expect(screen.getByText("engineer")).toBeVisible();
    });

    // Verify added roles not in dropdown
    const input = screen.getByLabelText("Select role to inherit");
    await userEvent.click(input);

    await waitFor(() => {
      expect(screen.getByText("developer")).toBeVisible();
      expect(screen.getByText("viewer")).toBeVisible();
      expect(screen.getByText("admin")).toBeVisible();

      const options = Array.from(document.querySelectorAll('[role="option"]'));
      const analystOption = options.find(
        (opt) => opt.textContent === "analyst",
      );
      const engineerOption = options.find(
        (opt) => opt.textContent === "engineer",
      );
      expect(analystOption).toBeUndefined();
      expect(engineerOption).toBeUndefined();
    });
  });

  it("filters out system roles (PUBLIC, mz_*)", async () => {
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
      {
        roleId: "sys3",
        roleName: "mz_support",
        memberCount: 0,
        ownedObjectsCount: 0,
      },
    ];

    await renderComponent(<FormWrapper />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: rolesWithSystem }));
      },
    });

    const input = screen.getByLabelText("Select role to inherit");
    await userEvent.click(input);

    await waitFor(() => {
      // System roles hidden
      expect(screen.queryByText("PUBLIC")).not.toBeInTheDocument();
      expect(screen.queryByText("mz_system")).not.toBeInTheDocument();
      expect(screen.queryByText("mz_support")).not.toBeInTheDocument();

      // Regular roles visible
      expect(screen.getByText("analyst")).toBeVisible();
      expect(screen.getByText("engineer")).toBeVisible();
    });
  });
});
