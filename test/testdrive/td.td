# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Tests that testdrive is working as expected

# uncomment to test that timeouts happen in the time desired.
# Note that the duration string format can be anything parsable
# by the parse_duration create
#$ set-sql-timeout duration=2minutes
#$ set-sql-timeout duration=default
#> select * from nonexistent

# test that hashing rows works and is consistent

$ set schema={
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
              {"name": "u64", "type": "long"},
              {"name": "str", "type": "string"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=100
{"before": null, "after": {"u64": 1, "str": "foo"}}
{"before": null, "after": {"u64": 2, "str": "bar"}}
{"before": null, "after": {"u64": 3, "str": "foo"}}
{"before": null, "after": {"u64": 1, "str": "bar"}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=101
{"before": null, "after": null}

> CREATE MATERIALIZED SOURCE src1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SELECT * from src1
4 values hashing to 7dd470c8470b085df13552e191a244ab
