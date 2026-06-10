// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "..";
import { createPostgresConnectionStatement } from "./createPostgresConnection";

describe("createPostgresConnectionStatement", () => {
  it("generates a valid statement", () => {
    const statement = createPostgresConnectionStatement({
      name: "pg_connection",
      databaseName: "materialize",
      schemaName: "public",
      host: "example.com",
      pgDatabaseName: "postgres",
      user: "user",
      port: "5432",
      password: {
        secretName: "secret_1",
        databaseName: "materialize",
        schemaName: "public",
      },
      sslMode: "require",
      sslKey: {
        secretName: "secret_1",
        databaseName: "materialize",
        schemaName: "public",
      },
      sslCertificate: { secretTextValue: "MIICzjCCAbeg..." },
      sslCertificateAuthority: {
        secretName: "secret_1",
        databaseName: "materialize",
        schemaName: "public",
      },
    }).compile(queryBuilder);
    expect(statement.sql).toEqual(`
CREATE CONNECTION "materialize"."public"."pg_connection" TO POSTGRES
(
HOST 'example.com',
DATABASE 'postgres',
USER 'user',
PORT 5432,
PASSWORD SECRET "materialize"."public"."secret_1",
SSL MODE 'require',
SSL KEY SECRET "materialize"."public"."secret_1",
SSL CERTIFICATE 'MIICzjCCAbeg...',
SSL CERTIFICATE AUTHORITY SECRET "materialize"."public"."secret_1"
);`);
  });

  it("filters out undefined connection creation parameters", () => {
    const statement = createPostgresConnectionStatement({
      name: "pg_connection",
      databaseName: "materialize",
      schemaName: "public",
      host: "example.com",
      pgDatabaseName: "postgres",
      user: "user",
      port: undefined,
      password: undefined,
      sslMode: undefined,
      sslKey: undefined,
      sslCertificate: undefined,
      sslCertificateAuthority: undefined,
    }).compile(queryBuilder);
    expect(statement.sql).toEqual(
      `
CREATE CONNECTION "materialize"."public"."pg_connection" TO POSTGRES
(
HOST 'example.com',
DATABASE 'postgres',
USER 'user'
);`,
    );
  });
});
