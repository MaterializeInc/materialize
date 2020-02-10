# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test exercises decimals at the boundary (e.g., by sending them through
# pgwire). Operations on decimals are more thoroughly tested in decimal.slt.

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
              {
                "name": "a",
                "type": {
                  "type": "bytes",
                  "scale": 2,
                  "precision": 15,
                  "logicalType": "decimal"
                }
              }
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": [7, 2]}}
{"before": null, "after": {"a": [186]}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=2
{"before": null, "after": null}

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW data_view as SELECT * from data

> SELECT * FROM data_view
17.94
-0.70
