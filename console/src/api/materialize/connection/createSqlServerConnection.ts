// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { buildFullyQualifiedObjectName } from "..";
import { buildOptionsFragments } from "./buildOptionsFragments";
import createConnection from "./createConnection";
import { Secret, TextSecret } from "./types";

export interface CreateSqlServerConnectionParameters {
  name: string;
  databaseName: string;
  schemaName: string;
  host: string;
  sqlServerDatabaseName: string;
  user: string;
  port?: string;
  password?: Secret | TextSecret;
  sslMode?: string;
  sslCertificateAuthority?: Secret | TextSecret;
}

export const createSqlServerConnectionStatement = (
  params: CreateSqlServerConnectionParameters,
) => {
  const name = buildFullyQualifiedObjectName(params);

  const options: [string, string | Secret | TextSecret | undefined][] = [
    ["HOST", params.host],
    ["DATABASE", params.sqlServerDatabaseName],
    ["USER", params.user],
    ["PORT", params.port],
    ["PASSWORD", params.password],
    ["SSL MODE", params.sslMode],
    ["SSL CERTIFICATE AUTHORITY", params.sslCertificateAuthority],
  ];

  return sql`
CREATE CONNECTION ${name} TO SQL SERVER
(
${sql.join(buildOptionsFragments(options), sql`,\n`)}
);`;
};

export async function createSqlServerConnection({
  params,
  environmentdHttpAddress,
}: {
  params: CreateSqlServerConnectionParameters;
  environmentdHttpAddress: string;
}) {
  const createConnectionQuery = createSqlServerConnectionStatement(params);

  return createConnection({
    connectionName: params.name,
    schemaName: params.schemaName,
    databaseName: params.databaseName,
    createConnectionQuery,
    environmentdHttpAddress,
  });
}

export default createSqlServerConnection;
