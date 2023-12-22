# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

>[version<8000] SHOW CREATE SINK upgrade_kafka_sink;
"materialize.public.upgrade_kafka_sink" "CREATE SINK \"materialize\".\"public\".\"upgrade_kafka_sink\" FROM \"materialize\".\"public\".\"static_view\" INTO KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" (TOPIC = 'upgrade-kafka-sink') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION \"materialize\".\"public\".\"csr_conn\" ENVELOPE DEBEZIUM"

# Later versions of MZ do not create linked clusters
$ skip-if
SELECT mz_version_num() >= 8100;

# Test that the "disk" option on the linked cluster defaults to false
>SELECT cluster, replica FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'materialize_public_upgrade_kafka_sink'
materialize_public_upgrade_kafka_sink  linked

# NOTE: Due to the ('N' -> 'uN') migration of replica IDs in the audit log, we
# currently see additional `drop` and `create` events for each replica that
# existed before the migration. Those events don't have a user attached, so we
# can recognize them and filter them out.
> SELECT event_type, object_type, details - 'id' - 'cluster_id' - 'replica_id' - 'billed_as' - 'internal'
  FROM mz_audit_events
  WHERE (
    details->>'name' = 'materialize_public_upgrade_kafka_sink' OR
    details->>'cluster_name' = 'materialize_public_upgrade_kafka_sink'
  ) AND user IS NOT NULL
create  cluster          "{\"name\":\"materialize_public_upgrade_kafka_sink\"}"
create  cluster-replica  "{\"cluster_name\":\"materialize_public_upgrade_kafka_sink\",\"disk\":true,\"logical_size\":\"${arg.default-storage-size}\",\"replica_name\":\"linked\"}"
