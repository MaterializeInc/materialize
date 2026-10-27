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

import { buildObjectColumnsQuery } from "./objectColumns";

describe("buildObjectColumnsQuery", () => {
  it("works as expected", async () => {
    await testdrive(`
      > CREATE TABLE test_table (id int)
      > CREATE INDEX test_index ON test_table (id)
    `);
    const query = buildObjectColumnsQuery({
      databaseName: "materialize",
      schemaName: "public",
      name: "test_table",
    }).compile();
    const result = await executeSqlHttp(query);
    expect(result.rows).toEqual([
      {
        name: "id",
        nullable: true,
        type: "integer",
        columnComment: null,
        relationComment: null,
      },
    ]);
  });

  it("works with comments", async () => {
    await testdrive(`
      > CREATE TABLE test_table_with_comments( id int,  name text NOT NULL );
      > COMMENT ON TABLE test_table_with_comments IS 'Table for storing user information';
      > COMMENT ON COLUMN test_table_with_comments.id IS 'Primary key identifier';
      > COMMENT ON COLUMN test_table_with_comments.name IS 'Column for storing user information';
    `);
    const query = buildObjectColumnsQuery({
      databaseName: "materialize",
      schemaName: "public",
      name: "test_table_with_comments",
    }).compile();
    const result = await executeSqlHttp(query);
    expect(result.rows).toEqual([
      {
        name: "id",
        nullable: true,
        type: "integer",
        columnComment: "Primary key identifier",
        relationComment: "Table for storing user information",
      },
      {
        name: "name",
        nullable: false,
        type: "text",
        columnComment: "Column for storing user information",
        relationComment: "Table for storing user information",
      },
    ]);
  });
});
