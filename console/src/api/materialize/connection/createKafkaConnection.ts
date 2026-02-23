// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { buildFullyQualifiedObjectName, escapedLiteral as lit } from "..";
import { buildOptionsFragments } from "./buildOptionsFragments";
import createConnection from "./createConnection";
import { Secret, TextSecret } from "./types";

export type Broker = {
  hostPort: string;
};

export type BasicBroker = Broker & {
  type: "basic";
};

export type SSLAuth = {
  type: "SSL";
  sslCertificate: Secret | TextSecret;
  sslKey: Secret;
  sslCertificateAuthority?: Secret | TextSecret;
};

export type SASLAuth = {
  type: "SASL";
  saslMechanism: SASLMechanism;
  saslUsername: string | Secret | TextSecret;
  saslPassword: Secret;
  sslCertificateAuthority?: Secret | TextSecret;
};

export type Auth = SSLAuth | SASLAuth;

export type Brokers = BasicBroker[];

export type CreateKafkaConnectionParameters = {
  name: string;
  schemaName: string;
  databaseName: string;
  auth?: Auth;
  brokers: Brokers;
};

export const SASL_MECHANISMS = {
  PLAIN: "Plain",
  "SCRAM-SHA-256": "SCRAM-SHA-256",
  "SCRAM-SHA-512": "SCRAM-SHA-512",
} as const;

export type SASLMechanism = keyof typeof SASL_MECHANISMS;

function createBrokersStatement(brokers: Brokers) {
  if (brokers.length === 1 && brokers[0].type === "basic") {
    return sql`BROKER ${lit(brokers[0].hostPort)}`;
  }

  const brokerFragments = brokers.map((broker) => {
    switch (broker.type) {
      case "basic":
        // At some point, we might want to support other types here
        return sql`${lit(broker.hostPort)}`;
    }
  });

  return sql`BROKERS (
${sql.join(brokerFragments, sql`,\n`)}
)`;
}

function createAuthStatement(auth?: Auth) {
  if (!auth) {
    return null;
  }

  const authOptions: [string, string | Secret | TextSecret | undefined][] =
    auth.type === "SASL"
      ? [
          ["SASL MECHANISMS", auth.saslMechanism],
          ["SASL USERNAME", auth.saslUsername],
          ["SASL PASSWORD", auth.saslPassword],
          ["SSL CERTIFICATE AUTHORITY", auth.sslCertificateAuthority],
        ]
      : [
          ["SSL KEY", auth.sslKey],
          ["SSL CERTIFICATE", auth.sslCertificate],
          ["SSL CERTIFICATE AUTHORITY", auth.sslCertificateAuthority],
        ];

  return buildOptionsFragments(authOptions);
}

export function createKafkaConnectionStatement(
  params: CreateKafkaConnectionParameters,
) {
  const name = buildFullyQualifiedObjectName(params);
  const authStatement = createAuthStatement(params.auth);
  const brokersStatement = createBrokersStatement(params.brokers);

  const options = sql.join(
    [brokersStatement, ...(authStatement ?? [])].filter(Boolean),
    sql`,\n`,
  );

  return sql`
CREATE CONNECTION ${name} TO KAFKA (
${options}
);`;
}

export async function createKafkaConnection({
  params,
  environmentdHttpAddress,
}: {
  params: CreateKafkaConnectionParameters;
  environmentdHttpAddress: string;
}) {
  const createConnectionQuery = createKafkaConnectionStatement(params);

  return createConnection({
    connectionName: params.name,
    schemaName: params.schemaName,
    databaseName: params.databaseName,
    createConnectionQuery,
    environmentdHttpAddress,
  });
}

export default createKafkaConnection;
