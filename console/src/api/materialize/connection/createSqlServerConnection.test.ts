// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "..";
import { createSqlServerConnectionStatement } from "./createSqlServerConnection";

describe("createSqlServerConnectionStatement", () => {
  it("generates a valid statement with all fields", () => {
    const statement = createSqlServerConnectionStatement({
      name: "sql_server_connection",
      databaseName: "materialize",
      schemaName: "public",
      host: "example.com",
      sqlServerDatabaseName: "sqlserver",
      user: "user",
      port: "1433",
      password: {
        secretName: "secret_1",
        databaseName: "materialize",
        schemaName: "public",
      },
      sslMode: "require",
      sslCertificateAuthority: {
        secretName: "secret_1",
        databaseName: "materialize",
        schemaName: "public",
      },
    }).compile(queryBuilder);
    expect(statement.sql).toEqual(`
CREATE CONNECTION "materialize"."public"."sql_server_connection" TO SQL SERVER
(
HOST 'example.com',
DATABASE 'sqlserver',
USER 'user',
PORT 1433,
PASSWORD SECRET "materialize"."public"."secret_1",
SSL MODE 'require',
SSL CERTIFICATE AUTHORITY SECRET "materialize"."public"."secret_1"
);`);
  });

  it("filters out undefined connection creation parameters", () => {
    const statement = createSqlServerConnectionStatement({
      name: "sql_server_connection",
      databaseName: "materialize",
      schemaName: "public",
      host: "example.com",
      sqlServerDatabaseName: "sqlserver",
      user: "user",
      port: undefined,
      password: undefined,
      sslMode: undefined,
      sslCertificateAuthority: undefined,
    }).compile(queryBuilder);
    expect(statement.sql).toEqual(
      `
CREATE CONNECTION "materialize"."public"."sql_server_connection" TO SQL SERVER
(
HOST 'example.com',
DATABASE 'sqlserver',
USER 'user'
);`,
    );
  });
});
