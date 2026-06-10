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

export interface CreatePostgresConnectionParameters {
  name: string;
  databaseName: string;
  schemaName: string;
  host: string;
  pgDatabaseName: string;
  user: string;
  port?: string;
  password?: Secret | TextSecret;
  sslMode?: string;
  sslKey?: Secret | TextSecret;
  sslCertificate?: Secret | TextSecret;
  sslCertificateAuthority?: Secret | TextSecret;
}

export const createPostgresConnectionStatement = (
  params: CreatePostgresConnectionParameters,
) => {
  const name = buildFullyQualifiedObjectName(params);

  const options: [string, string | Secret | TextSecret | undefined][] = [
    ["HOST", params.host],
    ["DATABASE", params.pgDatabaseName],
    ["USER", params.user],
    ["PORT", params.port],
    ["PASSWORD", params.password],
    ["SSL MODE", params.sslMode],
    ["SSL KEY", params.sslKey],
    ["SSL CERTIFICATE", params.sslCertificate],
    ["SSL CERTIFICATE AUTHORITY", params.sslCertificateAuthority],
  ];

  return sql`
CREATE CONNECTION ${name} TO POSTGRES
(
${sql.join(buildOptionsFragments(options), sql`,\n`)}
);`;
};

export async function createPostgresConnection({
  params,
  environmentdHttpAddress,
}: {
  params: CreatePostgresConnectionParameters;
  environmentdHttpAddress: string;
}) {
  const createConnectionQuery = createPostgresConnectionStatement(params);

  return createConnection({
    connectionName: params.name,
    schemaName: params.schemaName,
    databaseName: params.databaseName,
    createConnectionQuery,
    environmentdHttpAddress,
  });
}

export default createPostgresConnection;
