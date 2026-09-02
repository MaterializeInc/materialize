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
    { timeout: 45_000 },
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
    { timeout: 60_000 },
    async () => {
      await testdrive(
        `
        $ postgres-execute connection=postgres://mz_system:materialize@\${testdrive.materialize-internal-sql-addr}
        ALTER SYSTEM SET default_timestamp_interval = 100
        ALTER SYSTEM SET storage_statistics_collection_interval = 100
        ALTER SYSTEM SET storage_statistics_interval = 200

        # Create a cluster with multiple replicas
        > CREATE CLUSTER multirep_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=1'));

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

        # Wait until both replicas have registered statistics rows for the source.
        # Two distinct replica_ids is all we need to exercise the console query's
        # per-source collapse (asserted below), and it is a stable, monotonic signal:
        # once a replica's row appears it does not disappear.
        #
        # We deliberately do NOT wait for offset_committed/offset_known to converge to
        # a specific value on both replicas. offset_committed is derived from the
        # shared persist output frontier (see src/storage-client/src/statistics.rs),
        # and the standby replica observes that frontier with effectively unbounded lag
        # under CI load. Gating on that convergence is what made this test flaky and got
        # it repeatedly re-tuned (set-sql-timeout 60s->120s->30s, jest 90s->180s->60s;
        # PR #35982, #36098) without ever being a real fix. The collapse behavior under
        # test does not depend on the offsets having converged.
        $ set-sql-timeout duration=30s
        > SELECT count(distinct replica_id) FROM mz_internal.mz_source_statistics_with_history JOIN mz_sources using(id) WHERE name = 'kafka_multi'
        2
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

      // The multi-replica-specific behavior under test is the per-source collapse:
      // even though two replicas report statistics for `kafka_multi`, the console
      // query (distinctOn ss.id) must return a single row attributed to a single
      // replica -- not one row per replica, and not values summed across replicas.
      //
      // We intentionally do not assert concrete statistic values (offsets, message
      // counts, rehydration latency) here. Those depend on cross-replica convergence
      // -- eventually-consistent with unbounded lag under CI load -- and are already
      // covered deterministically by the single-replica test above. Re-asserting them
      // against a live multi-replica cluster is what made this test flaky.
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual(
        expect.objectContaining({
          id: source.id,
          replicaId: expect.any(String),
          replicaName: expect.any(String),
        }),
      );
    },
  );
});
