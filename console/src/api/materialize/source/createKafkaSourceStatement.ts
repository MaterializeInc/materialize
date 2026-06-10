// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import {
  buildFullyQualifiedObjectName,
  escapedLiteral as lit,
} from "~/api/materialize";
import { Cluster } from "~/api/materialize/cluster/useConnectorClusters";
import { Connection } from "~/api/materialize/connection/useConnections";

export interface CreateKafkaSourceParameters {
  name: string;
  connection: Connection;
  databaseName: string;
  schemaName: string;
  cluster: Cluster;
  topic: string;
  keyFormat: KafkaFormat;
  valueFormat: KafkaFormat;
  formatConnection: Connection | null;
  envelope: KafkaEnvelope;
}

export const formatOptions = [
  { id: "avro" as const, name: "Avro" },
  { id: "protobuf" as const, name: "Protobuf" },
  { id: "json" as const, name: "JSON" },
  { id: "text" as const, name: "Text" },
  { id: "bytes" as const, name: "Bytes" },
];

export type KafkaFormat = (typeof formatOptions)[number]["id"];

export const ENVELOPE_OPTIONS = [
  { id: "none" as const, name: "None" },
  { id: "upsert" as const, name: "Upsert" },
  { id: "debezium" as const, name: "Debezium" },
];

export type KafkaEnvelope = (typeof ENVELOPE_OPTIONS)[number]["id"];

// Envelope options given by https://materialize.com/docs/sql/create-source/kafka/#supported-formats
export const ENVELOPE_OPTIONS_BY_FORMAT = {
  avro: ENVELOPE_OPTIONS,
  protobuf: [ENVELOPE_OPTIONS[0], ENVELOPE_OPTIONS[1]],
  json: [ENVELOPE_OPTIONS[0], ENVELOPE_OPTIONS[1]],
  text: [ENVELOPE_OPTIONS[0], ENVELOPE_OPTIONS[1]],
  bytes: [ENVELOPE_OPTIONS[0], ENVELOPE_OPTIONS[1]],
};

function createFormatSpecStatement(
  format: KafkaFormat,
  formatConnection: Connection | null,
) {
  let formatSpec = sql``;

  // These explicit checks are here to prevent sql inject, because formats comes from
  // user input and it's not quoted, so we can't easily escape it.
  if (!formatOptions.map((o) => o.id).includes(format)) {
    throw new Error(`Invalid format ${format} specified`);
  }
  if (format === "avro" || format === "protobuf") {
    if (!formatConnection) {
      throw new Error("Format must have a schema registry connection.");
    }
    formatSpec = sql` USING CONFLUENT SCHEMA REGISTRY CONNECTION ${buildFullyQualifiedObjectName(
      formatConnection,
    )}`;
  }

  return sql`FORMAT ${sql.raw(format.toUpperCase())}${formatSpec}`;
}

const createKafkaSourceStatement = (params: CreateKafkaSourceParameters) => {
  if (!params.cluster) {
    throw new Error("You must specify cluster");
  }

  const name = buildFullyQualifiedObjectName(params);
  const connectionName = buildFullyQualifiedObjectName(params.connection);

  return sql`
CREATE SOURCE ${name}
IN CLUSTER ${sql.id(params.cluster?.name)}
FROM KAFKA CONNECTION ${connectionName} (TOPIC ${lit(params.topic)})
KEY ${createFormatSpecStatement(params.keyFormat, params.formatConnection)}
VALUE ${createFormatSpecStatement(params.valueFormat, params.formatConnection)}
ENVELOPE ${sql.raw(params.envelope.toUpperCase())};`;
};

export default createKafkaSourceStatement;
