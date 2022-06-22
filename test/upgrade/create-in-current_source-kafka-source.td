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

$ kafka-create-topic topic=upgrade-kafka-source-${arg.upgrade-from-version}

$ kafka-ingest format=avro topic=upgrade-kafka-source-${arg.upgrade-from-version} schema=${schema} timestamp=1
{"a": 1, "b": 2}
{"a": 2, "b": 3}

> CREATE MATERIALIZED SOURCE kafka_source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-upgrade-kafka-source-${arg.upgrade-from-version}-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

> SELECT COUNT(*) FROM kafka_source
2
