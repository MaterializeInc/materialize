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

import { buildObjectIndexesQuery } from "./objectIndexes";

describe("buildObjectIndexesQuery", () => {
  it("works as expected", async () => {
    await testdrive(`
      > CREATE TABLE test_table (id int)
      > CREATE INDEX test_index ON test_table (id)
      > CREATE DEFAULT INDEX ON test_table
    `);
    const query = buildObjectIndexesQuery({
      databaseName: "materialize",
      schemaName: "public",
      name: "test_table",
    }).compile();
    const result = await executeSqlHttp(query);

    expect(result.rows).toEqual([
      {
        id: expect.any(String),
        name: "test_index",
        databaseName: "materialize",
        schemaName: "public",
        owner: expect.any(String),
        createdAt: expect.any(Date),
        indexedColumns: ["id"],
      },
      {
        id: expect.any(String),
        name: "test_table_primary_idx",
        databaseName: "materialize",
        schemaName: "public",
        owner: expect.any(String),
        createdAt: expect.any(Date),
        indexedColumns: ["id"],
      },
    ]);
  });
});
