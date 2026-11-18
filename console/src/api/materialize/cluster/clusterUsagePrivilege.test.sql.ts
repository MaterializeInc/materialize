// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { inject } from "vitest";

import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildClusterUsagePrivilegeQuery } from "./clusterUsagePrivilege";

const CLUSTER_NAME = "serving.blue green";
const ROLE_NAME = "usage_privilege_test_role";

describe("buildClusterUsagePrivilegeQuery", () => {
  it("follows a USAGE grant on the cluster, whatever characters its name holds", async () => {
    await testdrive(`
      > CREATE CLUSTER "${CLUSTER_NAME}" (SIZE 'scale=1,workers=1', REPLICATION FACTOR 0);
    `);
    const system = await getMaterializeClient();
    try {
      const {
        rows: [cluster],
      } = await system.query("select id from mz_clusters where name = $1", [
        CLUSTER_NAME,
      ]);
      const query = buildClusterUsagePrivilegeQuery(cluster.id).compile();
      // NOTE: This environment runs with RBAC checks off, which the query
      // reads as access to every cluster. Only the internal port takes
      // mz_system, and the external one creates the role on first login.
      await system.query("ALTER SYSTEM SET enable_rbac_checks = true");
      const role = await getMaterializeClient({
        ...inject("materializeEndpoints").externalSql,
        user: ROLE_NAME,
      });
      try {
        const canUse = async () =>
          (await role.query(query.sql, [...query.parameters])).rows;

        expect(await canUse()).toEqual([{ canUse: false }]);

        await system.query(
          `GRANT USAGE ON CLUSTER "${CLUSTER_NAME}" TO ${ROLE_NAME}`,
        );
        expect(await canUse()).toEqual([{ canUse: true }]);
      } finally {
        await role.end();
      }
    } finally {
      await system.query("ALTER SYSTEM SET enable_rbac_checks = false");
      await system.end();
    }
  });

  it("returns no row for a cluster that doesn't exist", async () => {
    const result = await executeSqlHttp(
      buildClusterUsagePrivilegeQuery("u999999").compile(),
    );

    expect(result.rows).toEqual([]);
  });
});
