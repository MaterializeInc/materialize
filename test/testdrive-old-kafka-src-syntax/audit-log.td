# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1

# Test expected population of mz_audit_events after some DDL statements.

$ set-regex match=u\d+ replacement=<GID>

$ kafka-create-topic topic=test

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER audit_log_kafka_src SIZE = '${arg.default-storage-size}';

> CREATE SOURCE kafka_src
  IN CLUSTER audit_log_kafka_src
  FROM KAFKA CONNECTION kafka_conn
  (TOPIC 'testdrive-test-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS;

> SELECT event_type, object_type, details - 'replica_id' - 'billed_as' - 'internal', user FROM mz_audit_events ORDER BY id DESC LIMIT 4
create source "{\"cluster_id\":\"<GID>\",\"database\":\"materialize\",\"id\":\"<GID>\",\"item\":\"kafka_src\",\"schema\":\"public\",\"type\":\"kafka\"}" materialize
create source "{\"cluster_id\":null,\"database\":\"materialize\",\"id\":\"<GID>\",\"item\":\"kafka_src_progress\",\"schema\":\"public\",\"type\":\"progress\"}" materialize
create cluster-replica "{\"cluster_id\":\"<GID>\",\"cluster_name\":\"audit_log_kafka_src\",\"logical_size\":\"${arg.default-storage-size}\",\"reason\":\"manual\",\"replica_name\":\"r1\"}" materialize
create cluster "{\"id\":\"<GID>\",\"name\":\"audit_log_kafka_src\"}" materialize

> CREATE CLUSTER audit_log_counter_src SIZE = '${arg.default-storage-size}';

> CREATE SOURCE counter_src (counter)
  IN CLUSTER audit_log_counter_src
  FROM LOAD GENERATOR COUNTER (UP TO 100);

> SELECT event_type, object_type, details - 'replica_id' - 'billed_as' - 'internal', user FROM mz_audit_events ORDER BY id DESC LIMIT 4
create source "{\"cluster_id\":\"<GID>\",\"database\":\"materialize\",\"id\":\"<GID>\",\"item\":\"counter_src\",\"schema\":\"public\",\"type\":\"load-generator\"}" materialize
create source "{\"cluster_id\":null,\"database\":\"materialize\",\"id\":\"<GID>\",\"item\":\"counter_src_progress\",\"schema\":\"public\",\"type\":\"progress\"}" materialize
create cluster-replica "{\"cluster_id\":\"<GID>\",\"cluster_name\":\"audit_log_counter_src\",\"logical_size\":\"${arg.default-storage-size}\",\"reason\":\"manual\",\"replica_name\":\"r1\"}" materialize
create cluster "{\"id\":\"<GID>\",\"name\":\"audit_log_counter_src\"}" materialize
