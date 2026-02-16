// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildSinkListQuery } from "~/api/materialize/sink/sinkList";
import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("buildSinkListQuery", () => {
  it("with no filters", async () => {
    await testdrive(`
      $ kafka-create-topic topic=object-details
      > CREATE MATERIALIZED VIEW my_mv AS SELECT 1;
      > CREATE CONNECTION my_connection
        TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
      > CREATE SINK my_sink
        FROM my_mv
        INTO KAFKA CONNECTION my_connection (TOPIC 'testdrive-object-details-\${testdrive.seed}')
        FORMAT JSON
        ENVELOPE DEBEZIUM
    `);
    const query = buildSinkListQuery({}).compile();
    const result = await executeSqlHttp(query);
    expect(result.rows).toEqual([
      {
        clusterId: "u1",
        clusterName: "quickstart",
        connectionId: expect.stringMatching("^u"),
        connectionName: "my_connection",
        createdAt: expect.any(Date),
        databaseName: "materialize",
        error: null,
        id: expect.stringMatching("^u"),
        isOwner: true,
        kafkaTopic: expect.stringMatching("^testdrive-object-details-"),
        name: "my_sink",
        schemaName: "public",
        size: null,
        status: expect.stringMatching("running|created|starting"),
        type: "kafka",
      },
    ]);
  });
});
