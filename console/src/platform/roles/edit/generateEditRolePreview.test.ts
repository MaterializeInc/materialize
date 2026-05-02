// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ExistingRoleState } from "~/api/materialize/roles/editRole";
import { CreateRoleFormState } from "~/platform/roles/create/types";

import {
  generateEditRoleSql,
  generateEditRoleTerraform,
} from "./generateEditRolePreview";

const emptyState: CreateRoleFormState = {
  name: "test_role",
  inheritedRoles: [],
  privileges: [],
};

const existingState: ExistingRoleState = {
  inheritedRoles: [{ id: "r1", name: "parent_role" }],
  privileges: [
    {
      objectType: "database",
      objectId: "d1",
      objectName: "my_db",
      privileges: ["USAGE"],
    },
  ],
};

describe("generateEditRolePreview", () => {
  it("shows loading message when existingState is null", () => {
    expect(generateEditRoleSql("test", null, emptyState)).toBe(
      "-- Loading role data...",
    );
    expect(generateEditRoleTerraform("test", null, emptyState)).toBe(
      "# Loading role data...",
    );
  });

  it("shows current state when no changes", () => {
    const newState: CreateRoleFormState = {
      name: "test_role",
      inheritedRoles: existingState.inheritedRoles,
      privileges: existingState.privileges,
    };

    const sql = generateEditRoleSql("test_role", existingState, newState);
    const terraform = generateEditRoleTerraform(
      "test_role",
      existingState,
      newState,
    );

    // Should show existing state without NEW/REMOVE markers
    expect(sql).toContain("CREATE ROLE");
    expect(sql).toContain("GRANT");
    expect(sql).not.toContain("-- NEW:");
    expect(sql).not.toContain("-- REMOVE:");

    expect(terraform).toContain("materialize_role");
    expect(terraform).not.toContain("# NEW:");
  });

  it("marks new grants and revokes when changes exist", () => {
    const newState: CreateRoleFormState = {
      name: "test_role",
      inheritedRoles: [{ id: "r2", name: "new_parent" }], // Changed from parent_role
      privileges: [
        {
          objectType: "database",
          objectId: "d2",
          objectName: "other_db",
          privileges: ["CREATE"],
        },
      ],
    };

    const sql = generateEditRoleSql("test_role", existingState, newState);
    const terraform = generateEditRoleTerraform(
      "test_role",
      existingState,
      newState,
    );

    // SQL should show NEW and REMOVE markers
    expect(sql).toContain("-- NEW:");
    expect(sql).toContain("-- REMOVE:");
    expect(sql).toContain("new_parent");
    expect(sql).toContain("REVOKE");

    // Terraform should show NEW marker
    expect(terraform).toContain("# NEW:");
    expect(terraform).toContain("new_parent");
  });
});
