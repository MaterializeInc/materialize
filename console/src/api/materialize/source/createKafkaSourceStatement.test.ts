// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "..";
import createKafkaSourceStatement from "./createKafkaSourceStatement";

describe("createKafkaSourceStatement", () => {
  it("all options", () => {
    const { sql, parameters } = createKafkaSourceStatement({
      name: "kafka_connection",
      databaseName: "materialize",
      schemaName: "public",
      connection: {
        id: "u10",
        name: "kafka_connection",
        databaseName: "db1",
        schemaName: "sc1",
        type: "kafka",
      },
      cluster: { id: "u9", name: "source_cluster" },
      topic: "topic_name",
      keyFormat: "avro",
      valueFormat: "avro",
      formatConnection: {
        id: "u11",
        name: "kafka_csr_connection",
        databaseName: "db2",
        schemaName: "sc2",
        type: "kafka_csr_connection",
      },
      envelope: "upsert",
    }).compile(queryBuilder);
    expect({ sql, parameters }).toMatchSnapshot();
  });

  it("no csr", () => {
    const { sql, parameters } = createKafkaSourceStatement({
      name: "kafka_connection",
      databaseName: "materialize",
      schemaName: "public",
      connection: {
        id: "u10",
        name: "kafka_connection",
        databaseName: "db1",
        schemaName: "sc1",
        type: "kafka",
      },
      cluster: { id: "u9", name: "source_cluster" },
      topic: "topic_name",
      keyFormat: "text",
      valueFormat: "text",
      formatConnection: null,
      envelope: "none",
    }).compile(queryBuilder);
    expect({ sql, parameters }).toMatchSnapshot();
  });
});
