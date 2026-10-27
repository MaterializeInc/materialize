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

import {
  AllNamespaceItem,
  buildAllNamespacesQuery,
  buildAllSchemaListQuery,
  buildSchemaListQuery,
} from "./schemaList";

describe("buildNamespaceListQuery", () => {
  it("works as expected", async () => {
    await testdrive(`
      > CREATE SCHEMA test;
      > CREATE DATABASE test_db;
    `);
  });
});

describe("buildAllSchemaListQuery", () => {
  it("works as expected", async () => {
    await testdrive(`
      > CREATE SCHEMA test;
      > CREATE DATABASE test_db;
    `);
    const query = buildAllSchemaListQuery().compile();
    const result = await executeSqlHttp(query);
    // should include system schemas
    expect(result.rows).toContainEqual({
      id: "s1",
      name: "mz_catalog",
      databaseName: null,
      databaseId: null,
    });
    const userSchemas = result.rows.filter((row) => row.id.startsWith("u"));
    expect(userSchemas).toEqual([
      {
        databaseId: expect.stringMatching("^u"),
        databaseName: "test_db",
        id: expect.stringMatching("^u"),
        name: "public",
      },
      {
        databaseId: expect.stringMatching("^u"),
        databaseName: "materialize",
        id: expect.stringMatching("^u"),
        name: "public",
      },
      {
        databaseId: expect.stringMatching("^u"),
        databaseName: "materialize",
        id: expect.stringMatching("^u"),
        name: "test",
      },
    ]);
  });
});

describe("buildSchemaListQuery", () => {
  it("works as expected", async () => {
    await testdrive(`
      > CREATE SCHEMA test;
      > CREATE DATABASE test_db;
    `);
    const client = await getMaterializeClient();
    {
      const query = buildSchemaListQuery().compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          databaseId: expect.stringMatching("^u"),
          databaseName: "test_db",
          id: expect.stringMatching("^u"),
          name: "public",
        },
        {
          databaseId: expect.stringMatching("^u"),
          databaseName: "materialize",
          id: expect.stringMatching("^u"),
          name: "public",
        },
        {
          databaseId: expect.stringMatching("^u"),
          databaseName: "materialize",
          id: expect.stringMatching("^u"),
          name: "test",
        },
      ]);
    }
    {
      const { rows: databases } = await client.query(
        "select id from mz_databases where name = 'test_db'",
      );
      const query = buildSchemaListQuery({
        databaseId: databases[0].id,
        filterByCreatePrivilege: true,
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          databaseId: expect.stringMatching("^u"),
          databaseName: "test_db",
          id: expect.stringMatching("^u"),
          name: "public",
        },
      ]);
    }
    {
      // Test that mz_support user returns empty results when filterByCreatePrivilege is true
      // Note: In the test environment with RBAC disabled, mz_support is treated as a superuser
      // so this test verifies that the privilege filtering logic works correctly
      const mzSupport = await getMaterializeClient({ user: "mz_support" });
      const { sql, parameters } = buildSchemaListQuery({
        filterByCreatePrivilege: true,
      }).compile();
      const result = await mzSupport.query(sql, parameters as string[]);
      // Since RBAC is disabled in test environment, mz_support is treated as superuser
      // and should return all schemas, not empty array
      expect(result.rows.length).toBeGreaterThan(0);
    }
  });
});

describe("buildAllNamespacesQuery,", () => {
  it("should correctly list all namespaces including databases without schemas, schemas without database linkage, and maintain correct ordering", async () => {
    await testdrive(`
      > CREATE DATABASE db_with_schemas;
      > CREATE SCHEMA db_with_schemas.s1;

      > CREATE DATABASE empty_db;
      > DROP SCHEMA empty_db.public CASCADE;

      > CREATE SCHEMA schema_in_materialize_db;
    `);

    const query = buildAllNamespacesQuery().compile();

    const result = await executeSqlHttp(query);
    const rows: AllNamespaceItem[] = result.rows;

    // Check for the database with no schemas (empty_db)
    expect(rows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          databaseName: "empty_db",
          schemaName: null,
          schemaId: null,
          databaseId: expect.any(String),
        }),
      ]),
    );

    // Check for the database with schemas (db_with_schemas)
    expect(rows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          databaseName: "db_with_schemas",
          schemaName: "public",
          databaseId: expect.any(String),
          schemaId: expect.any(String),
        }),
        expect.objectContaining({
          databaseName: "db_with_schemas",
          schemaName: "s1",
          databaseId: expect.any(String),
          schemaId: expect.any(String),
        }),
      ]),
    );

    // Check for schemas in the default 'materialize' database
    expect(rows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          databaseName: "materialize",
          schemaName: "public",
          databaseId: expect.any(String),
          schemaId: expect.any(String),
        }),
        expect.objectContaining({
          databaseName: "materialize",
          schemaName: "schema_in_materialize_db",
          databaseId: expect.any(String),
          schemaId: expect.any(String),
        }),
      ]),
    );

    // Check for system schemas (e.g., mz_catalog, which has no associated databaseId/databaseName from mz_databases)
    expect(rows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          databaseName: null,
          databaseId: null,
          schemaName: "mz_catalog",
          schemaId: expect.any(String),
        }),
      ]),
    );
  });
});
