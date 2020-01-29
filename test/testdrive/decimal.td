# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

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

> CREATE SOURCE data FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}'
  USING SCHEMA '${schema}'

> CREATE MATERIALIZED VIEW data_view as SELECT * from data

> SELECT * FROM data_view
17.94
-0.70
