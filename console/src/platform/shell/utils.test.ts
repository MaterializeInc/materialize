// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getSelectedSchemaOption } from "./utils";

describe("getSelectedSchemaOption", () => {
  it("should return null if no valid schema exists in the search path", () => {
    const schemas = [
      { id: "s1", name: "schema1", databaseName: "db1", databaseId: "d1" },
      { id: "s1", name: "schema1", databaseName: "db2", databaseId: "d1" },
    ];
    const searchPath = "benji,wenji";

    const database = "db1";

    expect(getSelectedSchemaOption(searchPath, database, schemas)).toEqual(
      null,
    );
  });

  it("should return null if the database doesn't exist", () => {
    const schemas = [
      { id: "s1", name: "schema1", databaseName: "db1", databaseId: "d1" },
      { id: "s1", name: "schema1", databaseName: "db2", databaseId: "d1" },
    ];
    const searchPath = "schema1";

    const database = "invalid_database";

    expect(getSelectedSchemaOption(searchPath, database, schemas)).toEqual(
      null,
    );
  });

  it("should return null if the database is exists but the schema doesn't", () => {
    const schemas = [
      { id: "s1", name: "schema1", databaseName: "db1", databaseId: "d1" },
      { id: "s1", name: "schema1", databaseName: "db2", databaseId: "d1" },
    ];
    const searchPath = "invalid_schema";

    const database = "db2";

    expect(getSelectedSchemaOption(searchPath, database, schemas)).toEqual(
      null,
    );
  });

  it("should return null if the schema exists but doesn't exist in the database", () => {
    const schemas = [
      { id: "s1", name: "schema2", databaseName: "db1", databaseId: "d1" },
      { id: "s1", name: "schema1", databaseName: "db2", databaseId: "d1" },
    ];
    const searchPath = "schema2";

    const database = "db2";

    expect(getSelectedSchemaOption(searchPath, database, schemas)).toEqual(
      null,
    );
  });

  it("should return the schema option if the database exists and the schema exists in the database", () => {
    const schemas = [
      { id: "s1", name: "schema1", databaseName: "db1", databaseId: "d1" },
      { id: "s1", name: "schema1", databaseName: "db2", databaseId: "d1" },
    ];
    const searchPath = "schema1";

    const database = "db1";

    expect(getSelectedSchemaOption(searchPath, database, schemas)).toEqual({
      id: "db1.schema1",
      name: "schema1",
      databaseName: "db1",
    });
  });
});
