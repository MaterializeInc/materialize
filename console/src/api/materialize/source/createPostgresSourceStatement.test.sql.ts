// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "~/api/materialize/db";
import createPostgresSourceStatement from "~/api/materialize/source/createPostgresSourceStatement";
import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("createPostgresSourceStatement", () => {
  it("successfully creates a source", async () => {
    await testdrive(
      `
        $ postgres-execute connection=postgres://postgres:postgres@postgres
        ALTER USER postgres WITH replication;
        DROP SCHEMA IF EXISTS public CASCADE;
        CREATE SCHEMA public;
        CREATE TABLE t (c INTEGER);
        ALTER TABLE t REPLICA IDENTITY FULL;
        DROP PUBLICATION IF EXISTS mz_source;
        CREATE PUBLICATION mz_source FOR ALL TABLES;

        > CREATE SECRET pgpass AS 'postgres'
        > CREATE CONNECTION pg TO POSTGRES (
            HOST postgres,
            DATABASE postgres,
            USER postgres,
            PASSWORD SECRET pgpass
          )
        > CREATE DATABASE test_db
        > SET DATABASE TO test_db
        > CREATE SCHEMA test_schema
      `,
    );
    {
      const query = createPostgresSourceStatement({
        name: "pg_source",
        connection: {
          id: "u1",
          name: "pg",
          databaseName: "materialize",
          schemaName: "public",
        },
        databaseName: "test_db",
        schemaName: "test_schema",
        cluster: {
          id: "u1",
          name: "quickstart",
        },
        publication: "mz_source",
        allTables: true,
        tables: [],
      }).compile(queryBuilder);
      await executeSqlHttp(query);

      await testdrive(
        `
        > SELECT database_name, schema_name, name
          FROM mz_internal.mz_object_fully_qualified_names
          WHERE name = 'pg_source'
        test_db test_schema pg_source
      `,
        { noReset: true },
      );
    }
    {
      const query = createPostgresSourceStatement({
        name: "pg_source2",
        connection: {
          id: "u1",
          name: "pg",
          databaseName: "materialize",
          schemaName: "public",
        },
        databaseName: "test_db",
        schemaName: "test_schema",
        cluster: {
          id: "u1",
          name: "quickstart",
        },
        publication: "mz_source",
        allTables: false,
        tables: [{ name: "t", alias: "alias_t" }],
      }).compile(queryBuilder);
      await executeSqlHttp(query);

      await testdrive(
        `
        > SELECT database_name, schema_name, name
          FROM mz_internal.mz_object_fully_qualified_names
          WHERE name = 'alias_t'
        test_db test_schema alias_t
      `,
        { noReset: true },
      );
    }
  });
});
