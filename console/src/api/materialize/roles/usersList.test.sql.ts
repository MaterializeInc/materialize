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

import { buildUsersListQuery } from "./usersList";

describe("buildUsersListQuery", () => {
  it("returns users with their assigned roles", async () => {
    // Use email suffix to create users that can login
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP ROLE IF EXISTS ul_user_1, ul_user_2, "ul_user_1@test.com", "ul_user_2@test.com", ul_admin, ul_reader;
      CREATE ROLE "ul_user_1@test.com";
      CREATE ROLE "ul_user_2@test.com";
      CREATE ROLE ul_admin;
      CREATE ROLE ul_reader;
      GRANT ul_admin TO "ul_user_1@test.com";
      GRANT ul_reader TO "ul_user_1@test.com";
      GRANT ul_reader TO "ul_user_2@test.com";
    `);

    const query = buildUsersListQuery().compile();
    const result = await executeSqlHttp(query);

    // Find our test users
    const user1 = result.rows.find((r) => r.user_name === "ul_user_1@test.com");
    const user2 = result.rows.find((r) => r.user_name === "ul_user_2@test.com");

    expect(user1).toBeDefined();
    expect(user1?.roles_granted).toContain("ul_admin");
    expect(user1?.roles_granted).toContain("ul_reader");
    expect(user1?.roles_granted).toHaveLength(2);

    expect(user2).toBeDefined();
    expect(user2?.roles_granted).toContain("ul_reader");
    expect(user2?.roles_granted).toHaveLength(1);

    // Should not include non-login roles (ul_admin, ul_reader are roles, not users)
    const userNames = result.rows.map((r) => r.user_name);
    expect(userNames).not.toContain("ul_admin");
    expect(userNames).not.toContain("ul_reader");

    // Should not include system roles (mz_*)
    const systemRoles = userNames.filter((name) => name.startsWith("mz_"));
    expect(systemRoles).toHaveLength(0);
  });
});
