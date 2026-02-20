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

import { buildRolePrivilegesQuery } from "./rolePrivileges";

describe("buildRolePrivilegesQuery", () => {
  it("returns privileges for a role", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      GRANT CREATEROLE ON SYSTEM TO materialize;
      GRANT CREATEDB ON SYSTEM TO materialize;
      DROP DATABASE IF EXISTS priv_test_db;
      DROP ROLE IF EXISTS priv_test_role;

      > CREATE ROLE priv_test_role;
      > CREATE DATABASE priv_test_db;

      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      GRANT USAGE ON DATABASE priv_test_db TO priv_test_role;
    `);

    const query = buildRolePrivilegesQuery({ roleName: "priv_test_role" });
    const result = await executeSqlHttp(query);

    expect(result.rows.length).toBeGreaterThan(0);
    const dbPrivilege = result.rows.find(
      (row) => row.name === "priv_test_db" && row.object_type === "database",
    );
    expect(dbPrivilege).toBeDefined();
    expect(dbPrivilege?.grantee).toBe("priv_test_role");
    expect(dbPrivilege?.privilege_type).toBe("USAGE");
  });
});
