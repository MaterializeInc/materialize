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

import { buildCreateRoleStatements } from "./createRole";

describe("buildCreateRoleStatements", () => {
  it("creates a role with no inherited roles or privileges", async () => {
    await testdrive(`
      > DROP ROLE IF EXISTS test_role_basic
    `);

    const statements = buildCreateRoleStatements({
      name: "test_role_basic",
      inheritedRoles: [],
      privileges: [],
    });

    for (const statement of statements) {
      await executeSqlHttp(statement);
    }

    await testdrive(
      `
      > SELECT name FROM mz_roles WHERE name = 'test_role_basic'
      test_role_basic
    `,
      { noReset: true },
    );
  });

  it("creates a role with inherited roles", async () => {
    await testdrive(`
      > DROP ROLE IF EXISTS test_role_inherited
      > DROP ROLE IF EXISTS parent_role_1
      > DROP ROLE IF EXISTS parent_role_2
      > CREATE ROLE parent_role_1
      > CREATE ROLE parent_role_2
    `);

    const statements = buildCreateRoleStatements({
      name: "test_role_inherited",
      inheritedRoles: [
        { id: "u1", name: "parent_role_1" },
        { id: "u2", name: "parent_role_2" },
      ],
      privileges: [],
    });

    for (const statement of statements) {
      await executeSqlHttp(statement);
    }

    await testdrive(
      `
      > SELECT name FROM mz_roles WHERE name = 'test_role_inherited'
      test_role_inherited
      > SELECT role.name as role, member.name as member
        FROM mz_role_members rm
        JOIN mz_roles role ON rm.role_id = role.id
        JOIN mz_roles member ON rm.member = member.id
        WHERE member.name = 'test_role_inherited'
        ORDER BY role.name
      parent_role_1 test_role_inherited
      parent_role_2 test_role_inherited
    `,
      { noReset: true },
    );
  });

  it("creates a role with inherited roles and multiple privileges", async () => {
    await testdrive(`
      > DROP ROLE IF EXISTS test_role_full
      > DROP ROLE IF EXISTS base_role
      > DROP DATABASE IF EXISTS full_test_db CASCADE
      > CREATE ROLE base_role
      > CREATE DATABASE full_test_db
    `);

    const statements = buildCreateRoleStatements({
      name: "test_role_full",
      inheritedRoles: [{ id: "u1", name: "base_role" }],
      privileges: [
        {
          objectType: "database",
          objectId: "d1",
          objectName: "full_test_db",
          privileges: ["USAGE", "CREATE"],
        },
      ],
    });

    for (const statement of statements) {
      await executeSqlHttp(statement);
    }

    await testdrive(
      `
      > SELECT name FROM mz_roles WHERE name = 'test_role_full'
      test_role_full
      > SELECT role.name as role, member.name as member
        FROM mz_role_members rm
        JOIN mz_roles role ON rm.role_id = role.id
        JOIN mz_roles member ON rm.member = member.id
        WHERE member.name = 'test_role_full'
      base_role test_role_full
      > SELECT privilege_type
        FROM mz_internal.mz_show_all_privileges
        WHERE grantee = 'test_role_full'
          AND object_type = 'database'
          AND name = 'full_test_db'
        ORDER BY privilege_type
      CREATE
      USAGE
    `,
      { noReset: true },
    );
  });
});
