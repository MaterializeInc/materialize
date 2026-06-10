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

> BEGIN
> CREATE SOURCE tpch
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.1, UP TO 1000);

> CREATE TABLE customer FROM SOURCE tpch (REFERENCE customer);
> CREATE TABLE lineitem FROM SOURCE tpch (REFERENCE lineitem);
> CREATE TABLE nation FROM SOURCE tpch (REFERENCE nation);
> CREATE TABLE orders FROM SOURCE tpch (REFERENCE orders);
> CREATE TABLE part FROM SOURCE tpch (REFERENCE part);
> CREATE TABLE partsupp FROM SOURCE tpch (REFERENCE partsupp);
> CREATE TABLE region FROM SOURCE tpch (REFERENCE region);
> CREATE TABLE supplier FROM SOURCE tpch (REFERENCE supplier);
> COMMIT

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

# Wait for the sink to create the topic
$ kafka-wait-topic topic=testdrive-supplier-${testdrive.seed} partitions=1

> BEGIN
> CREATE SOURCE progress_check
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_fixed (TOPIC 'testdrive-progress-fixed-${testdrive.seed}')

> CREATE TABLE progress_check_tbl FROM SOURCE progress_check (REFERENCE "testdrive-progress-fixed-${testdrive.seed}")
  FORMAT JSON ENVELOPE NONE
> COMMIT

> SELECT COUNT(*) FROM progress_check_tbl WHERE data->'frontier' = '[]'::jsonb;
1
