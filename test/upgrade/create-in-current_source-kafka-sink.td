# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE MATERIALIZED VIEW static_view (f1) AS VALUES (1),(2),(3);

> CREATE CONNECTION IF NOT EXISTS kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SINK upgrade_kafka_sink FROM static_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'upgrade-kafka-sink')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
