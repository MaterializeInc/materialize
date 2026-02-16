// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "~/api/materialize/db";
import createSqlServerSourceStatement from "~/api/materialize/source/createSqlServerSourceStatement";
import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("createSqlServerSourceStatement", () => {
  it("successfully creates a source", { timeout: 60_000 }, async () => {
    await testdrive(
      `
        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=RPSsql12345

        $ sql-server-execute name=sql-server split-lines=false
        IF EXISTS (SELECT name FROM sys.databases WHERE name = N'test_sql_server_source')
        BEGIN
            ALTER DATABASE test_sql_server_source SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE test_sql_server_source;
        END;

        $ sql-server-execute name=sql-server
        CREATE DATABASE test_sql_server_source COLLATE Latin1_General_100_CI_AI_SC_UTF8;
        USE test_sql_server_source;
        EXEC sys.sp_cdc_enable_db;
        ALTER DATABASE test_sql_server_source SET ALLOW_SNAPSHOT_ISOLATION ON;
        CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(100));
        EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't1', @role_name = NULL;

        > CREATE SECRET sqlserver_password AS 'RPSsql12345'
        > CREATE CONNECTION sqlserver_conn TO SQL SERVER (
            HOST 'sql-server',
            PORT 1433,
            DATABASE 'test_sql_server_source',
            USER 'sa',
            PASSWORD SECRET sqlserver_password
          )
        > CREATE DATABASE test_db
        > SET DATABASE TO test_db
        > CREATE SCHEMA test_schema
      `,
    );
    {
      const query = createSqlServerSourceStatement({
        name: "mssql_source",
        connection: {
          id: "u1",
          name: "sqlserver_conn",
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
          WHERE name = 'mssql_source'
        test_db test_schema mssql_source
      `,
        { noReset: true },
      );
    }
    {
      const query = createSqlServerSourceStatement({
        name: "mssql_source2",
        connection: {
          id: "u1",
          name: "sqlserver_conn",
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
        tables: [{ schemaName: "dbo", name: "t1", alias: "alias_t1" }],
      }).compile(queryBuilder);
      await executeSqlHttp(query);

      await testdrive(
        `
        > SELECT database_name, schema_name, name
          FROM mz_internal.mz_object_fully_qualified_names
          WHERE name = 'alias_t1'
        test_db test_schema alias_t1
      `,
        { noReset: true },
      );
    }
  });
});
