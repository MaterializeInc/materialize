# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# This test exercises dates and times at the boundary (e.g., by sending them
# through pgwire). Operations on dates and times are more thoroughly tested in
# dates-times.slt.

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
                "name": "d",
                "type": {
                  "type": "int",
                  "logicalType": "date"
                }
              },
              {
                "name": "ts",
                "type": {
                  "type": "long",
                  "logicalType": "timestamp-micros"
                }
              }
            ]
          },
          "null"
        ]
      },
      {
        "name": "after",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {
                "name": "d",
                "type": {
                  "type": "int",
                  "logicalType": "date"
                }
              },
              {
                "name": "ts",
                "type": {
                  "type": "long",
                  "logicalType": "timestamp-micros"
                }
              }
            ]
          },
          "null"
        ]
      }
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"d": 18135, "ts": 1566865029000000}}
{"before": null, "after": {"d": 0, "ts": 65000000}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=10
{"before": null, "after": null}

> CREATE SOURCE data FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}' USING SCHEMA '${schema}'

> PEEK data
2019-08-27 "2019-08-27 00:17:09.000877"
1970-01-01 "1970-01-01 00:01:05"
