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

import { createMySqlConnectionStatement } from "./createMySqlConnection";

describe("createMySqlSourceStatement", () => {
  it("successfully creates a source", async () => {
    await testdrive(
      `
        $ mysql-connect name=mysql url=mysql://root@mysql password=p@ssw0rd
        $ mysql-execute name=mysql
        DROP DATABASE IF EXISTS public;
        CREATE DATABASE public;
        USE public;

        > CREATE SECRET mysql_password AS 'p@ssw0rd'
        > CREATE CONNECTION my TO MYSQL (
            HOST mysql,
            USER root,
            PASSWORD SECRET mysql_password
          )
        > CREATE DATABASE test_db
        > SET DATABASE TO test_db
        > CREATE SCHEMA test_schema
      `,
    );
    {
      const query = createMySqlConnectionStatement({
        name: "mysql_connection",
        databaseName: "test_db",
        schemaName: "test_schema",
        host: "mysql",
        user: "root",
        port: "3306",
        password: {
          secretName: "mysql_password",
          databaseName: "materialize",
          schemaName: "public",
        },
        sslMode: "required",
      }).compile(queryBuilder);
      await executeSqlHttp(query);

      await testdrive(
        `
        > SELECT database_name, schema_name, name
          FROM mz_internal.mz_object_fully_qualified_names
          WHERE name = 'mysql_connection'
        test_db test_schema mysql_connection
      `,
        { noReset: true },
      );
    }
  });
});
