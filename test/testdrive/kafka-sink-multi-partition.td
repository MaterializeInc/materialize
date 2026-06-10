# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

# We will create topics with 100 partitions and we create two sinks that
# publishes a single record twice, each time with a different Avro schema ID due
# to changed comments.
#
# Records are expected to be routed to the same partition, regardless of comment.

$ kafka-create-topic topic=v1 partitions=100

$ set-arg-default default-storage-size=scale=1,workers=1
$ set-arg-default single-replica-cluster=quickstart

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

# This is the row that will be published
> CREATE TABLE data (key text, value text);
> INSERT INTO data VALUES ('v1', NULL);

# Execution 1
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

# Execution 2
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
