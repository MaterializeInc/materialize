// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "~/api/materialize/db";
import createMySqlSourceStatement from "~/api/materialize/source/createMySqlSourceStatement";
import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("createMySqlSourceStatement", () => {
  it("successfully creates a source", async () => {
    await testdrive(
      `
        $ mysql-connect name=mysql url=mysql://root@mysql password=p@ssw0rd
        $ mysql-execute name=mysql
        DROP DATABASE IF EXISTS public;
        CREATE DATABASE public;
        USE public;

        CREATE TABLE t (c INTEGER);

        > CREATE SECRET mypass AS 'p@ssw0rd'
        > CREATE CONNECTION my TO MYSQL (
            HOST mysql,
            USER root,
            PASSWORD SECRET mypass
          )
        > CREATE DATABASE test_db
        > SET DATABASE TO test_db
        > CREATE SCHEMA test_schema
      `,
    );
    {
      const query = createMySqlSourceStatement({
        name: "my_source",
        connection: {
          id: "u1",
          name: "my",
          databaseName: "materialize",
          schemaName: "public",
        },
        databaseName: "test_db",
        schemaName: "test_schema",
        cluster: {
          id: "u1",
          name: "quickstart",
        },
        allTables: true,
        tables: [],
      }).compile(queryBuilder);
      await executeSqlHttp(query);

      await testdrive(
        `
        > SELECT database_name, schema_name, name
          FROM mz_internal.mz_object_fully_qualified_names
          WHERE name = 'my_source'
        test_db test_schema my_source
      `,
        { noReset: true },
      );
    }
    {
      const query = createMySqlSourceStatement({
        name: "my_source2",
        connection: {
          id: "u1",
          name: "my",
          databaseName: "materialize",
          schemaName: "public",
        },
        databaseName: "test_db",
        schemaName: "test_schema",
        cluster: {
          id: "u1",
          name: "quickstart",
        },
        allTables: false,
        tables: [{ schemaName: "public", name: "t", alias: "alias_t" }],
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
