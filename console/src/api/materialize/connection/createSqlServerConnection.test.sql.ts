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

import { createSqlServerConnectionStatement } from "./createSqlServerConnection";

describe("createSqlServerConnectionStatement", () => {
  it("successfully creates a connection", async () => {
    await testdrive(
      `
        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=RPSsql12345

        $ sql-server-execute name=sql-server split-lines=false
        IF EXISTS (SELECT name FROM sys.databases WHERE name = N'test_sql_server')
        BEGIN
            ALTER DATABASE test_sql_server SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE test_sql_server;
        END;

        $ sql-server-execute name=sql-server
        CREATE DATABASE test_sql_server COLLATE Latin1_General_100_CI_AI_SC_UTF8;
        USE test_sql_server;
        EXEC sys.sp_cdc_enable_db;
        ALTER DATABASE test_sql_server SET ALLOW_SNAPSHOT_ISOLATION ON;

        > CREATE SECRET sqlserver_password AS 'RPSsql12345'
        > CREATE DATABASE test_db
        > SET DATABASE TO test_db
        > CREATE SCHEMA test_schema
      `,
    );
    {
      const query = createSqlServerConnectionStatement({
        name: "sqlserver_connection",
        databaseName: "test_db",
        schemaName: "test_schema",
        host: "sql-server",
        sqlServerDatabaseName: "test_sql_server",
        user: "sa",
        port: "1433",
        password: {
          secretName: "sqlserver_password",
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
          WHERE name = 'sqlserver_connection'
        test_db test_schema sqlserver_connection
      `,
        { noReset: true },
      );
    }
  });
});
