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

import { buildMaxMySqlConnectionsQuery } from "./maxMySqlConnections";

describe("maxMySqlConnectionsQuery", () => {
  it("generates a valid query", async () => {
    await testdrive(`> SET cluster to quickstart;`);
    const query = buildMaxMySqlConnectionsQuery().compile(queryBuilder);
    const result = await executeSqlHttp(query);
    const max = parseInt(result.rows[0].max_mysql_connections);
    expect(max).toBeGreaterThanOrEqual(0);
  });
});
