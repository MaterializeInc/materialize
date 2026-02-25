// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildFirstReplicaSourceStatisticsTable } from "~/api/materialize/expressionBuilders";
import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("buildFirstReplicaSourceStatisticsTable", () => {
  it(
    "returns statistics for single-replica source",
    { timeout: 15_000 },
    async () => {
      await testdrive(
        `
        $ postgres-execute connection=postgres://mz_system:materialize@\${testdrive.materialize-internal-sql-addr}
        ALTER SYSTEM SET default_timestamp_interval = 100
        ALTER SYSTEM SET storage_statistics_collection_interval = 100
        ALTER SYSTEM SET storage_statistics_interval = 200

        $ kafka-create-topic topic=single_rep_test
        $ kafka-ingest format=bytes topic=single_rep_test
        one
        two

        > CREATE CONNECTION kafka_conn TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
        > CREATE SOURCE single_rep_source
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-single_rep_test-\${testdrive.seed}')
          ENVELOPE NONE

        # Wait for statistics to show up
        $ set-sql-timeout duration=10s
        > SELECT offset_known, offset_committed FROM mz_internal.mz_source_statistics JOIN mz_sources using(id) WHERE name = 'single_rep_source'
        2 2
      `,
      );

      const client = await getMaterializeClient();
      const {
        rows: [source],
      } = await client.query(
        "SELECT id FROM mz_sources WHERE name = 'single_rep_source'",
      );

      // Test with version >= 0.148.0 (current version)
      const replicaStatsTable =
        buildFirstReplicaSourceStatisticsTable().compile();
      const query = `
          SELECT stat.id, stat.messages_received, stat.bytes_received
          FROM (${replicaStatsTable.sql}) AS stat
          WHERE stat.id = '${source.id}'
        `;

      const result = await executeSqlHttp({
        sql: query,
        parameters: replicaStatsTable.parameters,
        query: replicaStatsTable.query,
      });

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual(
        expect.objectContaining({
          id: source.id,
          messages_received: expect.any(BigInt),
          bytes_received: expect.any(BigInt),
        }),
      );
    },
  );

  it(
    "returns only first replica statistics on multi-replica cluster",
    { timeout: 25_000 },
    async () => {
      await testdrive(
        `
        $ postgres-execute connection=postgres://mz_system:materialize@\${testdrive.materialize-internal-sql-addr}
        ALTER SYSTEM SET enable_multi_replica_sources = true
        ALTER SYSTEM SET default_timestamp_interval = 100
        ALTER SYSTEM SET storage_statistics_collection_interval = 100
        ALTER SYSTEM SET storage_statistics_interval = 200

        # Create a cluster with multiple replicas
        > CREATE CLUSTER multi_rep_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=1'));

        $ kafka-create-topic topic=multi_rep_test
        $ kafka-ingest format=bytes topic=multi_rep_test
        one
        two
        three

        > CREATE CONNECTION kafka_conn_multi TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
        > CREATE SOURCE multi_rep_source
          IN CLUSTER multi_rep_cluster
          FROM KAFKA CONNECTION kafka_conn_multi (TOPIC 'testdrive-multi_rep_test-\${testdrive.seed}')
          ENVELOPE NONE

        # Wait for statistics to show up on multiple replicas
        $ set-sql-timeout duration=20s
        > SELECT count(DISTINCT replica_id) FROM mz_internal.mz_source_statistics JOIN mz_sources USING(id) WHERE name = 'multi_rep_source'
        2
      `,
      );

      const client = await getMaterializeClient();
      const {
        rows: [source],
      } = await client.query(
        "SELECT id FROM mz_sources WHERE name = 'multi_rep_source'",
      );

      // Test that buildFirstReplicaSourceStatisticsTable returns only one row
      const replicaStatsTable =
        buildFirstReplicaSourceStatisticsTable().compile();
      const query = `
          SELECT stat.id, stat.messages_received, stat.replica_id
          FROM (${replicaStatsTable.sql}) AS stat
          WHERE stat.id = '${source.id}'
        `;

      const result = await executeSqlHttp({
        sql: query,
        parameters: replicaStatsTable.parameters,
        query: replicaStatsTable.query,
      });

      // Should return exactly one row (first replica)
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual(
        expect.objectContaining({
          id: source.id,
          messages_received: expect.any(BigInt),
          replica_id: expect.any(String),
        }),
      );
    },
  );
});
