// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import PostgresInterval from "postgres-interval";

import { buildSourceStatisticsQuery } from "~/api/materialize/source/sourceStatistics";
import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("buildSourceStatisticsQuery", () => {
  it(
    "gets current statistics on a single-replica cluster",
    { timeout: 15_000 },
    async () => {
      await testdrive(
        `
        $ postgres-execute connection=postgres://mz_system:materialize@\${testdrive.materialize-internal-sql-addr}
        ALTER SYSTEM SET default_timestamp_interval = 100
        ALTER SYSTEM SET storage_statistics_collection_interval = 100
        ALTER SYSTEM SET storage_statistics_interval = 200

        $ kafka-create-topic topic=input

        $ kafka-ingest format=bytes topic=input
        one
        two
        three

        > CREATE CONNECTION kafka_conn TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

        > CREATE SOURCE kafka
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-input-\${testdrive.seed}')
          ENVELOPE NONE

        # wait for statistics to show up
        $ set-sql-timeout duration=10s
        # TODO: the join belowworks around a bug in v0.102.0, remove when that gets fixed
        > SELECT offset_known, offset_committed FROM mz_internal.mz_source_statistics join mz_sources using(id)
        3 3
      `,
      );
      const client = await getMaterializeClient();
      const {
        rows: [source],
      } = await client.query("select id from mz_sources where name = 'kafka'");
      const query = buildSourceStatisticsQuery(source.id).compile();
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        {
          bytesReceived: 144,
          id: source.id,
          messagesReceived: 3,
          replicaId: "u1",
          replicaName: "r1",
          offsetDelta: 0,
          rehydrationLatency: expect.any(PostgresInterval),
          updatesCommitted: 3,
          snapshotRecordsKnown: 3,
          snapshotRecordsStaged: 3,
          updatesStaged: 3,
        },
      ]);
    },
  );

  it(
    "gets statistics for only the first replica on a multi-replica cluster",
    { timeout: 25_000 },
    async () => {
      await testdrive(
        `
        $ postgres-execute connection=postgres://mz_system:materialize@\${testdrive.materialize-internal-sql-addr}
        # Enable the feature flag to allow sources on multi-replica clusters
        ALTER SYSTEM SET enable_multi_replica_sources = true
        ALTER SYSTEM SET default_timestamp_interval = 100
        ALTER SYSTEM SET storage_statistics_collection_interval = 100
        ALTER SYSTEM SET storage_statistics_interval = 200

        # Create a cluster with multiple replicas
        > CREATE CLUSTER multirep_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=1'), r3 (SIZE 'scale=1,workers=2'));

        $ kafka-create-topic topic=multi_rep_input

        $ kafka-ingest format=bytes topic=multi_rep_input
        one
        two
        three

        > CREATE CONNECTION kafka_conn_multi TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

        # Create the source IN the multi-replica cluster
        > CREATE SOURCE kafka_multi
          IN CLUSTER multirep_cluster
          FROM KAFKA CONNECTION kafka_conn_multi (TOPIC 'testdrive-multi_rep_input-\${testdrive.seed}')
          ENVELOPE NONE

        # Wait for statistics to show up on multiple replicas with offsets fully committed.
        # This confirms that stats are being generated for more than one replica and that
        # offset_committed has caught up to offset_known, so offsetDelta will be 0.
        $ set-sql-timeout duration=20s
        > SELECT count(distinct replica_id) FROM mz_internal.mz_source_statistics JOIN mz_sources using(id) where name = 'kafka_multi' AND offset_committed = 3 AND offset_known = 3
        3
      `,
      );
      const client = await getMaterializeClient();
      const {
        rows: [source],
      } = await client.query(
        "select id from mz_sources where name = 'kafka_multi'",
      );
      const query = buildSourceStatisticsQuery(source.id).compile();
      const result = await executeSqlHttp(query);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual(
        expect.objectContaining({
          bytesReceived: 144,
          messagesReceived: 3,
          offsetDelta: 0,
          rehydrationLatency: expect.any(PostgresInterval),
          snapshotRecordsKnown: 3,
          snapshotRecordsStaged: 3,
          // Accept any number for updates fields as they may vary based on timing and cluster state
          updatesCommitted: expect.any(Number),
          updatesStaged: expect.any(Number),
          // Accept any string for id, replicaId, and replicaName
          id: expect.any(String),
          replicaId: expect.any(String),
          replicaName: expect.any(String),
        }),
      );
    },
  );
});
