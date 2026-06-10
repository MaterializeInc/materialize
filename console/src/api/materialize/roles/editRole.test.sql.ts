// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildInheritedRoleGrantQuery } from "./createRole";
import {
  buildRevokeInheritedRoleQuery,
  buildRevokePrivilegeQuery,
  calculateInheritedRoleDiff,
  calculatePrivilegeDiff,
} from "./editRole";

describe("editRole", () => {
  it("calculates diffs and executes grant/revoke operations", async () => {
    await testdrive(`
      > DROP ROLE IF EXISTS edit_test_role
      > DROP ROLE IF EXISTS keep_parent
      > DROP ROLE IF EXISTS remove_parent
      > DROP ROLE IF EXISTS add_parent
      > DROP DATABASE IF EXISTS edit_test_db CASCADE
      > CREATE ROLE keep_parent
      > CREATE ROLE remove_parent
      > CREATE ROLE add_parent
      > CREATE ROLE edit_test_role
      > CREATE DATABASE edit_test_db
      > GRANT keep_parent TO edit_test_role
      > GRANT remove_parent TO edit_test_role
      > GRANT USAGE, CREATE ON DATABASE edit_test_db TO edit_test_role
    `);

    // Test inherited role diff
    const inheritedDiff = calculateInheritedRoleDiff(
      [
        { id: "u1", name: "keep_parent" },
        { id: "u2", name: "remove_parent" },
      ],
      [
        { id: "u1", name: "keep_parent" },
        { id: "u3", name: "add_parent" },
      ],
    );

    expect(inheritedDiff.toGrant).toHaveLength(1);
    expect(inheritedDiff.toGrant[0].name).toBe("add_parent");
    expect(inheritedDiff.toRevoke).toHaveLength(1);
    expect(inheritedDiff.toRevoke[0].name).toBe("remove_parent");

    // Test privilege diff (remove CREATE, keep USAGE)
    const privilegeDiff = calculatePrivilegeDiff(
      [
        {
          objectType: "database",
          objectId: "d1",
          objectName: "edit_test_db",
          privileges: ["USAGE", "CREATE"],
        },
      ],
      [
        {
          objectType: "database",
          objectId: "d1",
          objectName: "edit_test_db",
          privileges: ["USAGE"],
        },
      ],
    );

    expect(privilegeDiff.toGrant).toHaveLength(0);
    expect(privilegeDiff.toRevoke).toHaveLength(1);
    expect(privilegeDiff.toRevoke[0].privileges).toEqual(["CREATE"]);

    // Execute all operations
    await executeSqlHttp(
      buildInheritedRoleGrantQuery({
        inheritedRoleName: "add_parent",
        roleName: "edit_test_role",
      }),
    );
    await executeSqlHttp(
      buildRevokeInheritedRoleQuery({
        inheritedRoleName: "remove_parent",
        roleName: "edit_test_role",
      }),
    );
    await executeSqlHttp(
      buildRevokePrivilegeQuery({
        grant: privilegeDiff.toRevoke[0],
        roleName: "edit_test_role",
      }),
    );

    // Verify inherited roles: keep_parent and add_parent (not remove_parent)
    await testdrive(
      `
      > SELECT role.name as role
        FROM mz_role_members rm
        JOIN mz_roles role ON rm.role_id = role.id
        JOIN mz_roles member ON rm.member = member.id
        WHERE member.name = 'edit_test_role'
        ORDER BY role.name
      add_parent
      keep_parent
      > SELECT privilege_type
        FROM mz_internal.mz_show_all_privileges
        WHERE grantee = 'edit_test_role'
          AND object_type = 'database'
          AND name = 'edit_test_db'
        ORDER BY privilege_type
      USAGE
    `,
      { noReset: true },
    );
  });
});
