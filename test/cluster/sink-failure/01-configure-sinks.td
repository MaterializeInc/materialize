# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true

> DROP SOURCE IF EXISTS mz_source CASCADE;
> DROP SECRET IF EXISTS pgpass CASCADE;

> CREATE CLUSTER storage REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['storage:2100'],
      STORAGE ADDRESSES ['storage:2103'],
      COMPUTECTL ADDRESSES ['storage:2101'],
      COMPUTE ADDRESSES ['storage:2102'],
      WORKERS 4
    )
  )

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE MATERIALIZED VIEW v1 AS
  SELECT 1 as col

> CREATE SINK snk
  IN CLUSTER storage
  FROM v1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-snk-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
