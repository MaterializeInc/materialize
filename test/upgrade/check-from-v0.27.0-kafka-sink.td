# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE SINK upgrade_kafka_sink;
"materialize.public.upgrade_kafka_sink" "CREATE SINK \"materialize\".\"public\".\"upgrade_kafka_sink\" FROM \"materialize\".\"public\".\"static_view\" INTO KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" (TOPIC = 'upgrade-kafka-sink') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION \"materialize\".\"public\".\"csr_conn\" ENVELOPE DEBEZIUM"

# Test that the "disk" option on the linked cluster defaults to false
> SELECT cluster, replica FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'materialize_public_upgrade_kafka_sink'
materialize_public_upgrade_kafka_sink  linked
> SELECT event_type, object_type, details - 'id' - 'cluster_id' - 'replica_id'
  FROM mz_audit_events
  WHERE details->>'name' = 'materialize_public_upgrade_kafka_sink' OR details->>'cluster_name' = 'materialize_public_upgrade_kafka_sink'
create  cluster          "{\"name\":\"materialize_public_upgrade_kafka_sink\"}"
create  cluster-replica  "{\"cluster_name\":\"materialize_public_upgrade_kafka_sink\",\"disk\":false,\"logical_size\":\"${arg.default-storage-size}\",\"replica_name\":\"linked\"}"
