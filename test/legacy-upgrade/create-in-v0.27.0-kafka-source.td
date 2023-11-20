# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The linked cluster for the source will be created by the mz_system user
# when upgrading to v0.39.
> CREATE TABLE IF NOT EXISTS linked_cluster_audit_event_user (priority int, user text)
> INSERT INTO linked_cluster_audit_event_user VALUES (27, 'mz_system')

$ set schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=upgrade-kafka-source-${arg.upgrade-from-version}

$ kafka-ingest format=avro topic=upgrade-kafka-source-${arg.upgrade-from-version} schema=${schema} timestamp=1
{"a": 1, "b": 2}
{"a": 2, "b": 3}

>[version>=7800] CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    SECURITY PROTOCOL PLAINTEXT
  )

>[version<7800] CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (
    BROKER '${testdrive.kafka-addr}'
  )

> CREATE SOURCE kafka_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upgrade-kafka-source-${arg.upgrade-from-version}-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

> SELECT COUNT(*) FROM kafka_source
2
