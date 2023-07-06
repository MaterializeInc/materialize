# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test expected population of mz_audit_events after some DDL statements.

$ set-regex match=u\d+ replacement=<GID>

$ kafka-create-topic topic=test

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE kafka_src
  FROM KAFKA CONNECTION kafka_conn
  (TOPIC 'testdrive-test-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS

> SELECT event_type, object_type, details - 'replica_id', user FROM mz_audit_events ORDER BY id DESC LIMIT 3
create  cluster "{\"id\":\"<GID>\",\"name\":\"materialize_public_kafka_src\"}"  materialize
create  cluster-replica "{\"cluster_id\":\"<GID>\",\"cluster_name\":\"materialize_public_kafka_src\",\"disk\":false,\"logical_size\":\"${arg.default-storage-size}\",\"replica_name\":\"linked\"}" materialize
create  source  "{\"database\":\"materialize\",\"id\":\"<GID>\",\"item\":\"kafka_src\",\"schema\":\"public\",\"size\":\"${arg.default-storage-size}\",\"type\":\"kafka\"}"  materialize

> CREATE SOURCE counter_src
  FROM LOAD GENERATOR COUNTER;

> SELECT event_type, object_type, details - 'replica_id', user FROM mz_audit_events ORDER BY id DESC LIMIT 3
create  cluster "{\"id\":\"<GID>\",\"name\":\"materialize_public_counter_src\"}"  materialize
create  cluster-replica "{\"cluster_id\":\"<GID>\",\"cluster_name\":\"materialize_public_counter_src\",\"disk\":false,\"logical_size\":\"${arg.default-storage-size}\",\"replica_name\":\"linked\"}" materialize
create  source  "{\"database\":\"materialize\",\"id\":\"<GID>\",\"item\":\"counter_src\",\"schema\":\"public\",\"size\":\"${arg.default-storage-size}\",\"type\":\"load-generator\"}"  materialize
