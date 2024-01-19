# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SOURCE tpch
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.1) FOR ALL TABLES;

# Pre-create the topic so we can create the source based on it
$ kafka-create-topic topic=progress-fixed partitions=1

> CREATE CONNECTION kafka_fixed TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    PROGRESS TOPIC 'testdrive-progress-fixed-${testdrive.seed}',
    SECURITY PROTOCOL PLAINTEXT
  );

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SINK sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM supplier
  INTO KAFKA CONNECTION kafka_fixed (TOPIC 'testdrive-supplier-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE SOURCE progress_check
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_fixed (TOPIC 'testdrive-progress-fixed-${testdrive.seed}')
  FORMAT JSON ENVELOPE NONE

> SELECT COUNT(*) FROM progress_check WHERE data->'frontier' = '[]'::jsonb;
1
