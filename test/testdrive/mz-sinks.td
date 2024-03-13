# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Verify that envelope types are correctly reported in mz_sinks

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE TABLE mz_sinks_table (name string);

> CREATE SINK mz_sinks_debezium
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mz_sinks_table
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-mz-sinks-debezium-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> CREATE MATERIALIZED VIEW mz_sinks_table_keyed AS
    SELECT name, count(name) from mz_sinks_table
    GROUP BY name;

> CREATE SINK mz_sinks_upsert
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mz_sinks_table_keyed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-mz-upsert-debezium-${testdrive.seed}')
  KEY (name)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> SELECT envelope_type FROM mz_sinks WHERE name = 'mz_sinks_debezium'
debezium

> SELECT envelope_type FROM mz_sinks WHERE name = 'mz_sinks_upsert'
upsert

> SELECT format FROM mz_sinks WHERE name = 'mz_sinks_upsert'
avro
