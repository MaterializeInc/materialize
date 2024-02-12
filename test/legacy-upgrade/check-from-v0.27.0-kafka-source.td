# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

> SELECT * FROM kafka_source;

a b
---
1 2
2 3

$ kafka-ingest format=avro topic=upgrade-kafka-source-${arg.upgrade-from-version} schema=${schema} timestamp=1
{"a": 10, "b": 20}
{"a": 20, "b": 30}

> SELECT * FROM kafka_source;

a b
---
1 2
2 3
10 20
20 30

$ set-from-sql var=expected-user
SELECT user FROM linked_cluster_audit_event_user ORDER BY priority DESC LIMIT 1

# Later versions of MZ do not create linked clusters
$ skip-if
SELECT mz_version_num() >= 8100;

# Test that the "disk" option on the linked cluster defaults to false
> SELECT cluster, replica FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'materialize_public_kafka_source'
materialize_public_kafka_source  linked

# NOTE: Due to the ('N' -> 'uN') migration of replica IDs in the audit log, we
# currently see additional `drop` and `create` events for each replica that
# existed before the migration. Those events don't have a user attached, so we
# can recognize them and filter them out.
> SELECT event_type, object_type, details - 'id' - 'cluster_id' - 'replica_id' - 'billed_as' - 'internal', user
  FROM mz_audit_events
  WHERE (
    details->>'name' = 'materialize_public_kafka_source' OR
    details->>'cluster_name' = 'materialize_public_kafka_source'
  ) AND user IS NOT NULL
create  cluster          "{\"name\":\"materialize_public_kafka_source\"}"  ${expected-user}
create  cluster-replica  "{\"cluster_name\":\"materialize_public_kafka_source\",\"disk\":true,\"logical_size\":\"${arg.default-storage-size}\",\"replica_name\":\"linked\"}"  ${expected-user}
