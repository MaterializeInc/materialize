# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/database-issues/issues/8810.

$ set-arg-default single-replica-cluster=quickstart

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_interval = '1s';
ALTER SYSTEM SET storage_statistics_collection_interval = '1s';

$ kafka-create-topic topic=data

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')

> SELECT rehydration_latency < '5' FROM mz_internal.mz_source_statistics
  JOIN mz_sources USING (id)
  WHERE name = 'src'
true

# Wait for some time to ensure a regression would increase the rehydration
# latency to a noticeable amount.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="5s"

> CREATE TABLE tbl FROM SOURCE src

# Wait for the source statistics to be refreshed. This happens every 1s, so
# waiting for 2s should be safe.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

> SELECT rehydration_latency < '5' FROM mz_internal.mz_source_statistics
  JOIN mz_sources USING (id)
  WHERE name = 'src'
true
