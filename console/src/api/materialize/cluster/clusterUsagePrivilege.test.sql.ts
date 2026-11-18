// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildClusterUsagePrivilegeQuery } from "./clusterUsagePrivilege";

// NOTE: RBAC checks are off in this environment, so every role can use every
// cluster and a denial can't be produced here.
describe("buildClusterUsagePrivilegeQuery", () => {
  it("says the role can use a cluster, whatever characters its name holds", async () => {
    await testdrive(`
      > CREATE CLUSTER "serving.blue green" (SIZE 'scale=1,workers=1', REPLICATION FACTOR 0);
    `);
    const client = await getMaterializeClient();
    const {
      rows: [cluster],
    } = await client.query(
      "select id from mz_clusters where name = 'serving.blue green'",
    );

    const result = await executeSqlHttp(
      buildClusterUsagePrivilegeQuery(cluster.id).compile(),
    );

    expect(result.rows).toEqual([{ canUse: true }]);
  });

  it("returns no row for a cluster that doesn't exist", async () => {
    const result = await executeSqlHttp(
      buildClusterUsagePrivilegeQuery("u999999").compile(),
    );

    expect(result.rows).toEqual([]);
  });
});
