// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "..";
import { createKafkaConnectionStatement } from "./createKafkaConnection";

describe("createKafkaConnectionStatement", () => {
  it("single broker", () => {
    const statement = createKafkaConnectionStatement({
      name: "kafka_connection",
      databaseName: "materialize",
      schemaName: "public",
      brokers: [{ type: "basic", hostPort: "broker1:9092" }],
    }).compile(queryBuilder);
    expect(statement.sql).toEqual(`
CREATE CONNECTION "materialize"."public"."kafka_connection" TO KAFKA (
BROKER 'broker1:9092'
);`);
  });

  it("multiple brokers", () => {
    const statement = createKafkaConnectionStatement({
      name: "kafka_connection",
      databaseName: "materialize",
      schemaName: "public",
      brokers: [
        { type: "basic", hostPort: "broker1:9092" },
        { type: "basic", hostPort: "broker2:9092" },
      ],
    }).compile(queryBuilder);
    expect(statement.sql).toEqual(`
CREATE CONNECTION "materialize"."public"."kafka_connection" TO KAFKA (
BROKERS (
'broker1:9092',
'broker2:9092'
)
);`);
  });

  it("SASL authentication", () => {
    const statement = createKafkaConnectionStatement({
      name: "kafka_connection",
      databaseName: "materialize",
      schemaName: "public",
      brokers: [{ type: "basic", hostPort: "broker1:9092" }],
      auth: {
        type: "SASL",
        saslMechanism: "PLAIN",
        saslUsername: { secretTextValue: "user" },
        saslPassword: {
          secretName: "kafka_password",
          databaseName: "materialize",
          schemaName: "public",
        },
        sslCertificateAuthority: { secretTextValue: "MIICzjCCAbeg..." },
      },
    }).compile(queryBuilder);
    expect(statement.sql).toEqual(`
CREATE CONNECTION "materialize"."public"."kafka_connection" TO KAFKA (
BROKER 'broker1:9092',
SASL MECHANISMS 'PLAIN',
SASL USERNAME 'user',
SASL PASSWORD SECRET "materialize"."public"."kafka_password",
SSL CERTIFICATE AUTHORITY 'MIICzjCCAbeg...'
);`);
  });

  it("SSL authentication", () => {
    const statement = createKafkaConnectionStatement({
      name: "kafka_connection",
      databaseName: "materialize",
      schemaName: "public",
      brokers: [{ type: "basic", hostPort: "broker1:9092" }],
      auth: {
        type: "SSL",
        sslKey: {
          secretName: "kafka_ssl_key",
          databaseName: "materialize",
          schemaName: "public",
        },
        sslCertificate: { secretTextValue: "MIICzjCCAbeg..." },
        sslCertificateAuthority: { secretTextValue: "MIICzjCCAbeg..." },
      },
    }).compile(queryBuilder);
    expect(statement.sql).toEqual(`
CREATE CONNECTION "materialize"."public"."kafka_connection" TO KAFKA (
BROKER 'broker1:9092',
SSL KEY SECRET "materialize"."public"."kafka_ssl_key",
SSL CERTIFICATE 'MIICzjCCAbeg...',
SSL CERTIFICATE AUTHORITY 'MIICzjCCAbeg...'
);`);
  });
});
