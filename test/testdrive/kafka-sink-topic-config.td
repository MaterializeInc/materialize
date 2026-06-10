# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

$ set-arg-default single-replica-cluster=quickstart

#
# Test topic configuration options at Kafka Sink CREATE time
#

> CREATE MATERIALIZED VIEW v1 (f1) AS VALUES (1);

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SINK topic_replication
  IN CLUSTER ${arg.single-replica-cluster}
  FROM v1
  INTO KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-kafka-replication-${testdrive.seed}',
    TOPIC REPLICATION FACTOR 1
  )
  KEY(f1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ kafka-verify-topic sink=materialize.public.topic_replication topic-config={} replication-factor=1


> CREATE SINK topic_partition
  IN CLUSTER ${arg.single-replica-cluster}
  FROM v1
  INTO KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-kafka-partition-${testdrive.seed}',
    TOPIC PARTITION COUNT 3
  )
  KEY(f1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ kafka-verify-topic sink=materialize.public.topic_partition partition-count=3

> CREATE SINK topic_config
  IN CLUSTER ${arg.single-replica-cluster}
  FROM v1
  INTO KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-kafka-config-${testdrive.seed}',
    TOPIC CONFIG MAP['cleanup.policy' => 'compact']
  )
  KEY(f1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ kafka-verify-topic sink=materialize.public.topic_config partition-count=1 topic-config={"cleanup.policy": "compact"}

# The config map contains unknown config names, but the CREATE SINK currently still succeeds
> CREATE SINK topic_config_unknown
  IN CLUSTER ${arg.single-replica-cluster}
  FROM v1
  INTO KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-kafka-config-unknown-${testdrive.seed}',
    TOPIC CONFIG MAP['abc' => 'def', 'ghi''' => 'jkl''']
  )
  KEY(f1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ skip-if
SELECT '${arg.uses-redpanda}'::BOOL

> SELECT status, error FROM mz_internal.mz_sink_statuses WHERE name = 'topic_config_unknown';
stalled "kafka: Error creating topic testdrive-kafka-config-unknown-${testdrive.seed} for sink: Admin operation error: InvalidConfig (Broker: Configuration is invalid)"

# Test whether MZ can alter the progress topic configuration when the relevant option is enabled

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_sink_ensure_topic_config = 'alter'

$ kafka-create-topic topic=kafka-progress partitions=1 compaction=false

$ kafka-verify-topic topic=testdrive-kafka-progress-${testdrive.seed} topic-config={"cleanup.policy": "delete"} replication-factor=1 partition-count=1

> CREATE CONNECTION kafka_conn_progress
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT, PROGRESS TOPIC 'testdrive-kafka-progress-${testdrive.seed}');

> CREATE SINK topic_config_check
  IN CLUSTER ${arg.single-replica-cluster}
  FROM v1
  INTO KAFKA CONNECTION kafka_conn_progress (
    TOPIC 'testdrive-kafka-config-check-${testdrive.seed}',
    TOPIC PARTITION COUNT 3
  )
  KEY(f1)
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify-topic topic=testdrive-kafka-progress-${testdrive.seed} topic-config={"cleanup.policy": "compact", "segment.bytes": "134217728"} replication-factor=1 partition-count=1
