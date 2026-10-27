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

import { queryBuilder } from "../db";
import { buildDropRoleStatement } from "./dropRole";

describe("buildDropRoleStatement", () => {
  it("drops an existing role", async () => {
    await testdrive(`
      > DROP ROLE IF EXISTS test_drop_role
      > CREATE ROLE test_drop_role
    `);

    // Verify role exists
    await testdrive(
      `
      > SELECT name FROM mz_roles WHERE name = 'test_drop_role'
      test_drop_role
    `,
      { noReset: true },
    );

    const statement = buildDropRoleStatement({
      roleName: "test_drop_role",
    }).compile(queryBuilder);

    await executeSqlHttp(statement);

    // Verify role is gone
    await testdrive(
      `
      > SELECT count(*) FROM mz_roles WHERE name = 'test_drop_role'
      0
    `,
      { noReset: true },
    );
  });

  it("fails when role has dependent privileges", async () => {
    await testdrive(`
      > DROP DATABASE IF EXISTS drop_test_db CASCADE
      > DROP ROLE IF EXISTS test_drop_role_deps
      > CREATE ROLE test_drop_role_deps
      > CREATE DATABASE drop_test_db
      > GRANT USAGE ON DATABASE drop_test_db TO test_drop_role_deps
    `);

    const statement = buildDropRoleStatement({
      roleName: "test_drop_role_deps",
    }).compile(queryBuilder);

    await expect(executeSqlHttp(statement)).rejects.toThrow(
      /cannot be dropped because some objects depend on it/,
    );
  });
});
