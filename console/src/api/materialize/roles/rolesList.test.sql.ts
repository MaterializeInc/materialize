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

import { buildRolesListQuery } from "./rolesList";

describe("buildRolesListQuery", () => {
  it("works as expected", async () => {
    // Use email suffix to create a user that can login
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP TABLE IF EXISTS rl_owned_table;
      DROP ROLE IF EXISTS "rl_user@test.com", rl_child_role, rl_parent_role;
      CREATE ROLE rl_parent_role;
      CREATE ROLE rl_child_role;
      CREATE ROLE "rl_user@test.com";
      GRANT rl_parent_role TO rl_child_role;
      GRANT rl_parent_role TO "rl_user@test.com";
      CREATE TABLE rl_owned_table (id INT);
      ALTER TABLE rl_owned_table OWNER TO rl_child_role;
    `);

    const query = buildRolesListQuery().compile();
    const result = await executeSqlHttp(query);

    // memberCount should be 1 because rl_user@test.com is a user (can login via email)
    expect(result.rows).toContainEqual(
      expect.objectContaining({
        roleName: "rl_parent_role",
        memberCount: 1n,
        ownedObjectsCount: 0n,
      }),
    );
    // memberCount should be 0 because rl_child_role is a role (not a user with LOGIN)
    // ownedObjectsCount should be 1 because rl_child_role owns rl_owned_table
    expect(result.rows).toContainEqual(
      expect.objectContaining({
        roleName: "rl_child_role",
        memberCount: 0n,
        ownedObjectsCount: 1n,
      }),
    );

    // Should not include system roles
    const roleNames = result.rows.map((r) => r.roleName);
    expect(roleNames).not.toContain("mz_system");
    expect(roleNames).not.toContain("mz_support");
  });
});
