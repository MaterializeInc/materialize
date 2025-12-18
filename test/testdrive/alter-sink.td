# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE TABLE pre_alter (pre_name string NOT NULL);
> INSERT INTO pre_alter VALUES ('fish');

> CREATE TABLE post_alter (post_name string, post_value int);
# This value should be ignored by the sink because the alter will happen after
# this record has been inserted and we don't re-emit a snapshot of the new
# collection when it changes.
> INSERT INTO post_alter VALUES ('ignored', 0);

! CREATE SINK sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mz_tables
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM;
contains: creating a sink directly on a catalog object not yet supported

> CREATE SINK sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pre_alter
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM;

! ALTER SINK sink SET FROM mz_tables;
contains: creating a sink directly on a catalog object not yet supported

$ kafka-verify-data format=json sink=materialize.public.sink key=false
{"before": null, "after": {"pre_name": "fish"}}

> ALTER SINK sink SET FROM post_alter;

# The sink will start sinking updates from `post_alter` at the timestamp that
# the previous dataflow happens to stop. This happens pretty quickly but we
# wait a few seconds more for good measure to avoid flaking.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=4s

> INSERT INTO post_alter VALUES ('chips', 42);

$ kafka-verify-data format=json sink=materialize.public.sink key=false
{"before": null, "after": {"post_name": "chips", "post_value": 42}}


# Test that backward incompatible schema changes lead to an error

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE TABLE post_alter_incompatible (post_value int NOT NULL);

> CREATE SINK incompatible_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pre_alter
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-sink-incompatible-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=avro sink=materialize.public.incompatible_sink sort-messages=true
{"before": null, "after": {"row": {"pre_name": "fish"}}}

> ALTER SINK incompatible_sink SET FROM post_alter_incompatible;

> SELECT st.error LIKE '%schema being registered is incompatible with an earlier schema%'
  FROM mz_sinks s JOIN mz_internal.mz_sink_statuses st ON s.id = st.id
  WHERE s.name = 'incompatible_sink';
true

# Create a cluster with no replicas so sources can't make progress. This will ensure `ALTER SINK` hangs forever until we cancel it.
> CREATE CLUSTER no_replicas REPLICAS ()

> CREATE SOURCE counter
  IN CLUSTER no_replicas
  FROM LOAD GENERATOR COUNTER (UP TO 100);

> CREATE SINK wedged_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM counter
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM;

$ set-from-sql var=backend-pid
SELECT CAST(pg_backend_pid() AS text);

$ postgres-execute background=true connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
SELECT mz_unsafe.mz_sleep(3);
SELECT pg_cancel_backend(CAST(${backend-pid} AS int4));

! ALTER SINK wedged_sink SET FROM post_alter;
contains:canceling statement due to user request


# There is a meaningful difference in having an object created after the sink
# already exists, see incident-131:
> CREATE TABLE created_post_alter (created_post_name string, created_post_value int);

> ALTER SINK sink SET FROM created_post_alter;

# The sink will start sinking updates from `post_alter` at the timestamp that
# the previous dataflow happens to stop. This happens pretty quickly but we
# wait a few seconds more for good measure to avoid flaking.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

> INSERT INTO created_post_alter VALUES ('hundred', 99);

$ kafka-verify-data format=json sink=materialize.public.sink key=false
{"before": null, "after": {"created_post_name": "hundred", "created_post_value": 99}}
