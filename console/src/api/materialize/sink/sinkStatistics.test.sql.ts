// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildSinkStatisticsQuery } from "~/api/materialize/sink/sinkStatistics";
import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("buildSinkStatisticsQuery", () => {
  it(
    "gets current statistics on a single-replica cluster",
    { timeout: 15_000 },
    async () => {
      await testdrive(`
      $ postgres-execute connection=postgres://mz_system:materialize@\${testdrive.materialize-internal-sql-addr}
      ALTER SYSTEM SET kafka_default_metadata_fetch_interval = 100
      ALTER SYSTEM SET storage_statistics_collection_interval = 100
      ALTER SYSTEM SET storage_statistics_interval = 200

      $ kafka-create-topic topic=object-details
      > CREATE TABLE my_table (colname text);
      > CREATE MATERIALIZED VIEW my_mv AS SELECT * FROM my_table;
      > CREATE CONNECTION my_connection
        TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
      > CREATE SINK my_sink
        FROM my_mv
        INTO KAFKA CONNECTION my_connection (TOPIC 'testdrive-object-details-\${testdrive.seed}')
        FORMAT JSON
        ENVELOPE DEBEZIUM
      > INSERT INTO my_table VALUES ('test_msg');

      # wait for statistics to show up
      $ set-sql-timeout duration=10s
      # TODO: the join below works around a bug in v0.102.0, remove when that gets fixed
      > SELECT messages_staged, messages_committed FROM mz_internal.mz_sink_statistics join mz_sinks using(id)
      1 1
    `);

      const client = await getMaterializeClient();
      const {
        rows: [sink],
      } = await client.query("select id from mz_sinks where name = 'my_sink'");
      const query = buildSinkStatisticsQuery(sink.id).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          id: sink.id,
          replicaId: "u1",
          replicaName: "r1",
          bytesStaged: 46,
          bytesCommitted: 46,
          messagesStaged: 1,
          messagesCommitted: 1,
        },
      ]);
    },
  );

  it(
    "gets statistics for a sink on a multi-replica cluster",
    { timeout: 25_000 },
    async () => {
      await testdrive(
        `
        $ postgres-execute connection=postgres://mz_system:materialize@\${testdrive.materialize-internal-sql-addr}
        ALTER SYSTEM SET kafka_default_metadata_fetch_interval = 100
        ALTER SYSTEM SET storage_statistics_collection_interval = 100
        ALTER SYSTEM SET storage_statistics_interval = 200

        # Create a cluster with multiple replicas
        > CREATE CLUSTER multirep_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=1'), r3 (SIZE 'scale=2,workers=1'));

        $ kafka-create-topic topic=multi_rep_object_details

        > CREATE TABLE my_table_multi (colname text);
        > CREATE MATERIALIZED VIEW my_mv_multi AS SELECT * FROM my_table_multi;
        > CREATE CONNECTION my_connection_multi
          TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
        > CREATE SINK my_sink_multi
          IN CLUSTER multirep_cluster
          FROM my_mv_multi
          INTO KAFKA CONNECTION my_connection_multi (TOPIC 'testdrive-multi_rep_object_details-\${testdrive.seed}')
          FORMAT JSON
          ENVELOPE DEBEZIUM
        > INSERT INTO my_table_multi VALUES ('test_msg');

        # Wait for statistics to show up
        $ set-sql-timeout duration=20s
        > SELECT messages_staged, messages_committed FROM mz_internal.mz_sink_statistics JOIN mz_sinks using(id) where name = 'my_sink_multi'
        1 1
      `,
      );
      const client = await getMaterializeClient();
      const {
        rows: [sink],
      } = await client.query(
        "select id from mz_sinks where name = 'my_sink_multi'",
      );
      const query = buildSinkStatisticsQuery(sink.id).compile();
      const result = await executeSqlHttp(query);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual(
        expect.objectContaining({
          bytesStaged: 46,
          bytesCommitted: 46,
          messagesStaged: 1,
          messagesCommitted: 1,
          // Accept any string for id, replicaId, and replicaName
          id: expect.any(String),
          replicaId: expect.any(String),
          replicaName: expect.any(String),
        }),
      );
    },
  );
});
