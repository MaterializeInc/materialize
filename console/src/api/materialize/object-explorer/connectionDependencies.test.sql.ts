// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getIdByName } from "~/test/sql/getIdByName";
import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildConnectionDependenciesQuery } from "./connectionDependencies";

const namespace = {
  databaseName: "materialize",
  schemaName: "public",
};

describe("buildConnectionDependencies", () => {
  it("should fetch dependent 2 sinks and 1 source", async () => {
    await testdrive(`
      $ kafka-create-topic topic=object-details
      > CREATE MATERIALIZED VIEW my_mv AS SELECT 1;
      > CREATE CONNECTION my_connection
        TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
      > CREATE SOURCE my_source
        FROM KAFKA CONNECTION my_connection (TOPIC 'testdrive-object-details-\${testdrive.seed}')
        FORMAT TEXT
        ENVELOPE NONE
      > CREATE SINK my_sink
        FROM my_mv
        INTO KAFKA CONNECTION my_connection (TOPIC 'testdrive-object-details-\${testdrive.seed}')
        FORMAT JSON
        ENVELOPE DEBEZIUM
      > CREATE SINK my_sink_2
        FROM my_mv
        INTO KAFKA CONNECTION my_connection (TOPIC 'testdrive-object-details-\${testdrive.seed}')
        FORMAT JSON
        ENVELOPE DEBEZIUM
    `);
    {
      const id = await getIdByName({
        ...namespace,
        name: "my_connection",
      });

      const query = buildConnectionDependenciesQuery({
        connectionId: id,
      }).compile();

      const result = await executeSqlHttp(query);

      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_sink",
          type: "sink",
          subType: "kafka",
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
        },
        {
          id: expect.stringMatching("^u"),
          name: "my_sink_2",
          type: "sink",
          subType: "kafka",
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
        },
        {
          id: expect.stringMatching("^u"),
          name: "my_source",
          type: "source",
          subType: "kafka",
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
        },
      ]);
    }
  });
});
