// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "~/api/materialize/db";
import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildGrantRoleMemberStatement } from "./grantRoleMember";
import { buildRoleMembersQuery } from "./roleMembers";

describe("buildGrantRoleMemberStatement", () => {
  it("grants a role to a member", async () => {
    await testdrive(
      `
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      DROP ROLE IF EXISTS grant_test_member;
      DROP ROLE IF EXISTS grant_test_role;
      GRANT CREATEROLE ON SYSTEM TO materialize;
      CREATE ROLE grant_test_role;
      CREATE ROLE grant_test_member;
    `,
      { noReset: true },
    );

    const beforeResult = await executeSqlHttp(
      buildRoleMembersQuery({ roleName: "grant_test_role" }).compile(),
    );
    expect(beforeResult.rows).toHaveLength(0);

    const query = buildGrantRoleMemberStatement({
      roleName: "grant_test_role",
      memberName: "grant_test_member",
    }).compile(queryBuilder);
    await executeSqlHttp(query);

    const afterResult = await executeSqlHttp(
      buildRoleMembersQuery({ roleName: "grant_test_role" }).compile(),
    );
    expect(afterResult.rows).toHaveLength(1);
    expect(afterResult.rows[0]?.memberName).toBe("grant_test_member");
  });
});
