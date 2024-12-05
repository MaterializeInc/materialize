# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# We will create topics with 100 partitions and we create two sinks that
# publishes a single record twice, each time with a different Avro schema ID due
# to changed comments.
#
# With the v0 partitioning scheme this will lead to at least two different
# partitions being used, which is the issue we're trying to avoid. The chance of
# the v0 partitioning scheme correctly assigning the same partition four times
# in a row is one in 100 million, so while flaky by design this is low enough to
# be negligible.
#
# With the v1 partitioning scheme this will never lead to two different
# partitions being used.
$ kafka-create-topic topic=v0 partitions=100
$ kafka-create-topic topic=v1 partitions=100

$ set-arg-default default-storage-size=1
$ set-arg-default single-replica-cluster=quickstart

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

# This is the row that will be published with the v0 scheme
> CREATE TABLE data (key text, value text);
> INSERT INTO data VALUES('v0', NULL);

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET default_sink_partition_strategy = 'v0';

# v0 Schema - Execution 1
> COMMENT ON COLUMN data.key IS 'v01';
> CREATE SINK v01
  IN CLUSTER ${arg.single-replica-cluster}
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-v0-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

# v0 Schema - Execution 2
> COMMENT ON COLUMN data.key IS 'v02';
> CREATE SINK v02
  IN CLUSTER ${arg.single-replica-cluster}
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-v0-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

# v0 Scheme - Execution 3
> COMMENT ON COLUMN data.key IS 'v03';
> CREATE SINK v03
  IN CLUSTER ${arg.single-replica-cluster}
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-v0-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

# v0 Scheme - Execution 4
> COMMENT ON COLUMN data.key IS 'v04';
> CREATE SINK v04
  IN CLUSTER ${arg.single-replica-cluster}
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-v0-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

$ kafka-verify-data format=avro sink=materialize.public.v01
{"key": {"string": "v0"}} {"key": {"string": "v0"}, "value": null}
{"key": {"string": "v0"}} {"key": {"string": "v0"}, "value": null}
{"key": {"string": "v0"}} {"key": {"string": "v0"}, "value": null}
{"key": {"string": "v0"}} {"key": {"string": "v0"}, "value": null}

# Now we will ingest the raw data and confirm that the v0 scheme moved records
# around.
> CREATE SOURCE sink_verify
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-v0-${testdrive.seed}');

> CREATE TABLE sink_verify_tbl FROM SOURCE sink_verify (REFERENCE "testdrive-v0-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION
  ENVELOPE NONE;

> SELECT COUNT(DISTINCT partition) = 1 FROM sink_verify_tbl
false

# This is the row that will be published with the v1 schema
> DROP TABLE data CASCADE;
> CREATE TABLE data (key text, value text);
> INSERT INTO data VALUES ('v1', NULL);

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET default_sink_partition_strategy = 'v1';

# v1 Schema - Execution 1
> COMMENT ON COLUMN data.key IS 'v11';
> CREATE SINK v11
  IN CLUSTER ${arg.single-replica-cluster}
  FROM data
  INTO KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-v1-${testdrive.seed}',
    TOPIC METADATA REFRESH INTERVAL '2s'
  )
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

# v1 Schema - Execution 2
> COMMENT ON COLUMN data.key IS 'v12';
> CREATE SINK v12
  IN CLUSTER ${arg.single-replica-cluster}
  FROM data
  INTO KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-v1-${testdrive.seed}',
    TOPIC METADATA REFRESH INTERVAL '2s'
  )
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

$ kafka-verify-data format=avro sink=materialize.public.v11
{"key": {"string": "v1"}} {"key": {"string": "v1"}, "value": null} partition=75
{"key": {"string": "v1"}} {"key": {"string": "v1"}, "value": null} partition=75

# Test that Kafka sinks discover new partitions in a timely fashion and start
# routing data to the new partitions.

$ kafka-add-partitions topic=v1 total-partitions=200

# Wait out twice the topic metadata refresh duration to virtually guarantee that
# the Kafka sinks have received the updated partition information.

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

> INSERT INTO data VALUES ('v1')

# Even though the key is the same as before, the data is sent to a new
# partition.

$ kafka-verify-data format=avro sink=materialize.public.v11
{"key": {"string": "v1"}} {"key": {"string": "v1"}, "value": null} partition=175
{"key": {"string": "v1"}} {"key": {"string": "v1"}, "value": null} partition=175
