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

import { buildRevokeRoleMemberStatement } from "./revokeRoleMember";
import { buildRoleMembersQuery } from "./roleMembers";

describe("buildRevokeRoleMemberStatement", () => {
  it("revokes a role from a member", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      GRANT CREATEROLE ON SYSTEM TO materialize;
      DROP ROLE IF EXISTS revoke_test_member, revoke_test_role;

      > CREATE ROLE revoke_test_role;
      > CREATE ROLE revoke_test_member;

      $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
      GRANT revoke_test_role TO revoke_test_member;
    `);

    const beforeResult = await executeSqlHttp(
      buildRoleMembersQuery({ roleName: "revoke_test_role" }).compile(),
    );
    expect(beforeResult.rows).toHaveLength(1);
    expect(beforeResult.rows[0]?.memberName).toBe("revoke_test_member");

    const query = buildRevokeRoleMemberStatement({
      roleName: "revoke_test_role",
      memberName: "revoke_test_member",
    }).compile(queryBuilder);
    await executeSqlHttp(query);

    const afterResult = await executeSqlHttp(
      buildRoleMembersQuery({ roleName: "revoke_test_role" }).compile(),
    );
    expect(afterResult.rows).toHaveLength(0);
  });
});
