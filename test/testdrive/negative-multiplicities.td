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
              {"name": "a", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1}}

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM data
1

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": {"a": 1}, "after": null}
{"before": {"a": 1}, "after": null}

> SELECT count(*) FROM data
-1

! SELECT * FROM data
Negative multiplicity: -1

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": {"a": 1}, "after": null}

> SELECT count(*) FROM data
-2

! SELECT * FROM data
Negative multiplicity: -2
