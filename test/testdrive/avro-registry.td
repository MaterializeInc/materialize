# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests that hit the Confluent Schema Registry.

# Verify the error message is useful when a schema is not present in the
# registry.

! CREATE SOURCE noexist
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-noexist-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM
contains:No value schema found

$ set schema-v1={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ set schema-v2={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long", "default": 42}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema-v1} publish=true timestamp=1
{"before": null, "after": {"row": {"a": 1}}}

> CREATE MATERIALIZED SOURCE data_v1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

> SELECT * FROM data_v1
a
---
1

$ kafka-ingest format=avro topic=data schema=${schema-v2} publish=true timestamp=3
{"before": null, "after": {"row": {"a": 2, "b": -1}}}

> CREATE MATERIALIZED SOURCE data_v2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

> SELECT * FROM data_v1
a
---
1
2

> SELECT * FROM data_v2
a b
----
1 42
2 -1

$ set valid-key-schema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "a", "type": "long"}
    ]
  }

$ kafka-ingest topic=data publish=true timestamp=5
  format=avro schema=${schema-v1} key-format=avro key-schema=${valid-key-schema}
{"a": 1} {"before": null, "after": {"row": {"a": 1}}}

> CREATE MATERIALIZED SOURCE data_v3
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

> SELECT * FROM data_v3
a b
----
1 42
1 42
2 -1

# Make sure this query gives WRONG results,
# which should prove that we are respecting primary
# key information (and optimizing by transforming
# a reduce on a primary key to a map)

> SELECT a, count(*) FROM data_v3
  GROUP BY a
a count
-------
1 1
1 1
2 1

# When ignoring source keys, it should give correct results.

> CREATE MATERIALIZED SOURCE data_v4
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (ignore_source_keys = true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

> SELECT a, count(*) FROM data_v4
  GROUP BY a
a count
-------
1 2
2 1
