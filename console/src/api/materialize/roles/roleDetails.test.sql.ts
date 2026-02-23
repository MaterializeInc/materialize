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

import { buildRoleDetailsQuery } from "./roleDetails";

describe("buildRoleDetailsQuery", () => {
  it("returns role details with users, grantedToRoles and grantedRoles", async () => {
    // Use email suffix to create a user that can login

    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP ROLE IF EXISTS rd_user, "rd_user@test.com", rd_member_1, rd_member_2, rd_target, rd_parent;
      CREATE ROLE rd_parent;
      CREATE ROLE rd_target;
      CREATE ROLE rd_member_1;
      CREATE ROLE rd_member_2;
      CREATE ROLE "rd_user@test.com";
      GRANT rd_parent TO rd_target;
      GRANT rd_target TO rd_member_1;
      GRANT rd_target TO rd_member_2;
      GRANT rd_target TO "rd_user@test.com";
    `);

    const query = buildRoleDetailsQuery({ roleName: "rd_target" }).compile();
    const result = await executeSqlHttp(query);

    expect(result.rows).toHaveLength(1);
    const roleDetails = result.rows[0];

    expect(roleDetails.name).toBe("rd_target");

    // Users (can login via email)
    expect(roleDetails.users).toContain("rd_user@test.com");
    expect(roleDetails.users).toHaveLength(1);

    // Roles granted to (cannot login)
    expect(roleDetails.grantedToRoles).toContain("rd_member_1");
    expect(roleDetails.grantedToRoles).toContain("rd_member_2");
    expect(roleDetails.grantedToRoles).toHaveLength(2);

    // Roles this role inherits from
    expect(roleDetails.grantedRoles).toContain("rd_parent");
    expect(roleDetails.grantedRoles).toHaveLength(1);
  });

  it("returns multiple granted roles", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP ROLE IF EXISTS rd_multi, rd_base_1, rd_base_2, rd_base_3;

      > CREATE ROLE rd_base_1;
      > CREATE ROLE rd_base_2;
      > CREATE ROLE rd_base_3;
      > CREATE ROLE rd_multi;

      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      GRANT rd_base_1 TO rd_multi;
      GRANT rd_base_2 TO rd_multi;
      GRANT rd_base_3 TO rd_multi;
    `);

    const query = buildRoleDetailsQuery({
      roleName: "rd_multi",
    }).compile();
    const result = await executeSqlHttp(query);

    expect(result.rows).toHaveLength(1);
    const roleDetails = result.rows[0];

    expect(roleDetails.name).toBe("rd_multi");
    expect(roleDetails.grantedRoles).toContain("rd_base_1");
    expect(roleDetails.grantedRoles).toContain("rd_base_2");
    expect(roleDetails.grantedRoles).toContain("rd_base_3");
    expect(roleDetails.grantedRoles).toHaveLength(3);
    expect(roleDetails.users).toBeNull();
    expect(roleDetails.grantedToRoles).toBeNull();
  });

  it("returns null arrays when role has no members or granted roles", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP ROLE IF EXISTS rd_standalone;

      > CREATE ROLE rd_standalone;
    `);

    const query = buildRoleDetailsQuery({
      roleName: "rd_standalone",
    }).compile();
    const result = await executeSqlHttp(query);

    expect(result.rows).toHaveLength(1);
    const roleDetails = result.rows[0];

    expect(roleDetails.name).toBe("rd_standalone");
    expect(roleDetails.users).toBeNull();
    expect(roleDetails.grantedToRoles).toBeNull();
    expect(roleDetails.grantedRoles).toBeNull();
  });

  it("returns empty result for non-existent role", async () => {
    const query = buildRoleDetailsQuery({
      roleName: "non_existent_role",
    }).compile();
    const result = await executeSqlHttp(query);

    expect(result.rows).toHaveLength(0);
  });
});
