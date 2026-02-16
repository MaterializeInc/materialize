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

import { buildDatabaseDetailsQuery } from "./databaseDetails";

describe("buildDatabaseDetailsQuery", () => {
  it("works as expected", async () => {
    // reset Materialize state
    await testdrive(`> SET cluster to quickstart;`);
    const query = buildDatabaseDetailsQuery({
      name: "materialize",
    }).compile();
    const result = await executeSqlHttp(query);
    expect(result.rows).toEqual([
      {
        id: expect.stringMatching("^u.*"),
        name: "materialize",
        owner: "mz_system",
        createdAt: expect.any(Date),
      },
    ]);
  });
});
