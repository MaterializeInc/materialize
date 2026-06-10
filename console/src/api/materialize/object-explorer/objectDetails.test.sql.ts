// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildObjectDetailsQuery } from "./objectDetails";

const namespace = {
  databaseName: "materialize",
  schemaName: "public",
};

describe("buildObjectDetailsQuery", () => {
  it("works as expected", async () => {
    await testdrive(`
      > CREATE MATERIALIZED VIEW my_mv AS SELECT 1;
      > CREATE VIEW my_view AS SELECT 1;
      > CREATE SECRET my_secret AS 'postgres'
      > CREATE CONNECTION my_connection
        TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
      > CREATE SOURCE my_source FROM WEBHOOK BODY FORMAT TEXT
      > CREATE TABLE my_table (a int)
      > CREATE INDEX my_index on my_table (a)
      > CREATE SINK my_sink
        FROM my_mv
        INTO KAFKA CONNECTION my_connection (TOPIC 'object-details-\${testdrive.seed}')
        FORMAT JSON
        ENVELOPE DEBEZIUM
      > CREATE SCHEMA other
      > SET schema TO other
      > CREATE TABLE filtered (a int)
    `);
    {
      const query = buildObjectDetailsQuery({
        ...namespace,
        name: "my_mv",
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_mv",
          type: "materialized-view",
          isSourceTable: false,
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
          owner: "materialize",
          createdAt: expect.any(Date),
          clusterId: "u1",
          clusterName: "quickstart",
        },
      ]);
    }
    {
      const query = buildObjectDetailsQuery({
        ...namespace,
        name: "my_view",
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_view",
          type: "view",
          isSourceTable: false,
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
          owner: "materialize",
          createdAt: expect.any(Date),
          clusterId: null,
          clusterName: null,
        },
      ]);
    }
    {
      const query = buildObjectDetailsQuery({
        ...namespace,
        name: "my_secret",
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_secret",
          type: "secret",
          isSourceTable: false,
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
          owner: "materialize",
          createdAt: expect.any(Date),
          clusterId: null,
          clusterName: null,
        },
      ]);
    }
    {
      const query = buildObjectDetailsQuery({
        ...namespace,
        name: "my_connection",
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_connection",
          type: "connection",
          isSourceTable: false,
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
          owner: "materialize",
          createdAt: expect.any(Date),
          clusterId: null,
          clusterName: null,
        },
      ]);
    }
    {
      const query = buildObjectDetailsQuery({
        ...namespace,
        name: "my_source",
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_source",
          type: "table",
          isSourceTable: true,
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
          owner: "materialize",
          createdAt: expect.any(Date),
          clusterId: "u1",
          clusterName: "quickstart",
        },
      ]);
    }
    {
      const query = buildObjectDetailsQuery({
        ...namespace,
        name: "my_table",
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_table",
          type: "table",
          isSourceTable: false,
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
          owner: "materialize",
          createdAt: expect.any(Date),
          clusterId: null,
          clusterName: null,
        },
      ]);
    }
    {
      const query = buildObjectDetailsQuery({
        ...namespace,
        name: "my_index",
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_index",
          type: "index",
          isSourceTable: false,
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
          owner: "materialize",
          createdAt: expect.any(Date),
          clusterId: "u1",
          clusterName: "quickstart",
        },
      ]);
    }
    {
      const query = buildObjectDetailsQuery({
        ...namespace,
        name: "my_sink",
      }).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: expect.stringMatching("^u"),
          name: "my_sink",
          type: "sink",
          isSourceTable: false,
          databaseName: namespace.databaseName,
          schemaName: namespace.schemaName,
          owner: "materialize",
          createdAt: expect.any(Date),
          clusterId: "u1",
          clusterName: "quickstart",
        },
      ]);
    }
  });
});
