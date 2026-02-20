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

import { buildRoleMembersQuery } from "./roleMembers";

describe("buildRoleMembersQuery", () => {
  it("returns members filtered by memberType", async () => {
    // Use email suffix to create a user that can login
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP ROLE IF EXISTS rm_user, "rm_user@test.com", rm_child_1, rm_child_2, rm_parent;
      CREATE ROLE rm_parent;
      CREATE ROLE rm_child_1;
      CREATE ROLE rm_child_2;
      CREATE ROLE "rm_user@test.com";
      GRANT rm_parent TO rm_child_1;
      GRANT rm_parent TO rm_child_2;
      GRANT rm_parent TO "rm_user@test.com";
    `);

    // No filter - returns all members
    const allMembersQuery = buildRoleMembersQuery({
      roleName: "rm_parent",
    }).compile();
    const allMembersResult = await executeSqlHttp(allMembersQuery);

    expect(allMembersResult.rows).toHaveLength(3);

    // memberType: "roles" - returns only roles (cannot login)
    const rolesOnlyQuery = buildRoleMembersQuery({
      roleName: "rm_parent",
      memberType: "roles",
    }).compile();
    const rolesOnlyResult = await executeSqlHttp(rolesOnlyQuery);

    expect(rolesOnlyResult.rows).toHaveLength(2);
    const roleNames = rolesOnlyResult.rows.map((r) => r.memberName);
    expect(roleNames).toContain("rm_child_1");
    expect(roleNames).toContain("rm_child_2");
    expect(roleNames).not.toContain("rm_user@test.com");

    // memberType: "users" - returns only users (can login via email)
    const usersOnlyQuery = buildRoleMembersQuery({
      roleName: "rm_parent",
      memberType: "users",
    }).compile();
    const usersOnlyResult = await executeSqlHttp(usersOnlyQuery);

    expect(usersOnlyResult.rows).toHaveLength(1);
    expect(usersOnlyResult.rows[0].memberName).toBe("rm_user@test.com");
  });

  it("returns empty result for role with no members", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP ROLE IF EXISTS rm_empty;
      CREATE ROLE rm_empty;
    `);

    const query = buildRoleMembersQuery({
      roleName: "rm_empty",
    }).compile();
    const result = await executeSqlHttp(query);

    expect(result.rows).toHaveLength(0);
  });
});
