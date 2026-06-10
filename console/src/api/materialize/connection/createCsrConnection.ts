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

export interface CreateCsrConnectionParameters {
  name: string;
  url: string;
  databaseName: string;
  schemaName: string;
  username?: string | Secret | TextSecret;
  password?: Secret;
  sslKey?: Secret;
  sslCertificate?: Secret | TextSecret;
  sslCertificateAuthority?: Secret | TextSecret;
}

export const createCsrConnectionStatement = (
  params: CreateCsrConnectionParameters,
) => {
  const name = buildFullyQualifiedObjectName(params);

  const options: [string, string | Secret | TextSecret | undefined][] = [
    ["URL", params.url],
    ["USERNAME", params.username],
    ["PASSWORD", params.password],
    ["SSL KEY", params.sslKey],
    ["SSL CERTIFICATE", params.sslCertificate],
    ["SSL CERTIFICATE AUTHORITY", params.sslCertificateAuthority],
  ];

  return sql`
CREATE CONNECTION ${name} TO CONFLUENT SCHEMA REGISTRY (
${sql.join(buildOptionsFragments(options), sql`,\n`)}
);`;
};

export function createCsrConnection({
  params,
  environmentdHttpAddress,
}: {
  params: CreateCsrConnectionParameters;
  environmentdHttpAddress: string;
}) {
  const createConnectionQuery = createCsrConnectionStatement(params);

  return createConnection({
    connectionName: params.name,
    schemaName: params.schemaName,
    databaseName: params.databaseName,
    createConnectionQuery,
    environmentdHttpAddress,
  });
}
