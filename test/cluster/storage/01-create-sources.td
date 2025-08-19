# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

# Create sources and verify they can ingest data while `environmentd` is online.

$ kafka-create-topic topic=remote1
$ kafka-create-topic topic=remote2

$ kafka-ingest format=bytes topic=remote1
one
$ kafka-ingest format=bytes topic=remote2
one

> CREATE CLUSTER storage_cluster REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd1:2100', 'clusterd2:2100'],
      STORAGE ADDRESSES ['clusterd1:2103', 'clusterd2:2103'],
      COMPUTECTL ADDRESSES ['clusterd1:2101', 'clusterd2:2101'],
      COMPUTE ADDRESSES ['clusterd1:2102', 'clusterd2:2102'],
      WORKERS 4
    )
  )

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE remote1
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-remote1-${testdrive.seed}')
> CREATE TABLE remote1_tbl FROM SOURCE remote1 (REFERENCE "testdrive-remote1-${testdrive.seed}")
  FORMAT TEXT
> CREATE SOURCE remote2
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-remote2-${testdrive.seed}')
> CREATE TABLE remote2_tbl FROM SOURCE remote2 (REFERENCE "testdrive-remote2-${testdrive.seed}")
  FORMAT TEXT
> COMMIT

> CREATE SOURCE webhook_text IN CLUSTER storage_cluster FROM WEBHOOK
  BODY FORMAT TEXT;

$ webhook-append database=materialize schema=public name=webhook_text
a

> SELECT * from remote1_tbl
one
> SELECT * from remote2_tbl
one

# The `CREATE TABLE ... FROM SOURCE` commands caused a recreation of the
# respective source dataflows, during which we might have lost the
# statistics about committed updates from the snapshot. Ingest some more data
# to ensure we see some `updates_committed`.

$ kafka-ingest format=bytes topic=remote1
two
$ kafka-ingest format=bytes topic=remote2
two

> SELECT s.name,
  SUM(u.updates_committed) > 0,
  SUM(u.messages_received) >= 2,
  SUM(u.offset_known),
  SUM(u.offset_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('remote1', 'remote2')
  GROUP BY s.id, s.name
remote1 true true 2 2
remote2 true true 2 2

> SELECT s.name,
  SUM(u.updates_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('webhook_text')
  GROUP BY s.id, s.name
webhook_text 1
