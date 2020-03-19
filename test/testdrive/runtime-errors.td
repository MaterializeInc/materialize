# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
              {"name": "id", "type": "string"},
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"id": "valid1", "a": 2, "b": 1}}
{"before": null, "after": {"id": "valid2", "a": 17, "b": 5}}

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW multiply AS SELECT id, a * b AS product FROM data

> CREATE MATERIALIZED VIEW divide AS SELECT id, a / b AS quotient FROM data

> CREATE MATERIALIZED VIEW both AS
  SELECT * FROM multiply NATURAL JOIN divide

> SELECT * FROM both
valid1  2   2
valid2  85  3

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=2
{"before": null, "after": {"id": "bad1", "a": 7, "b": 0}}

> SELECT * FROM multiply
valid1  2
valid2  85
bad1    0

! SELECT * FROM divide
division by zero

! SELECT * FROM both
division by zero

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=3
{"before": {"id": "bad1", "a": 7, "b": 0}, "after": null}

> SELECT * FROM both
valid1  2   2
valid2  85  3
