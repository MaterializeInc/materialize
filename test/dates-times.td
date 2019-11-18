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
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"d": 18135, "ts": 1566865029000000}}
{"before": null, "after": {"d": 0, "ts": 65000000}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=10
{"before": null, "after": null}

> CREATE SOURCE data FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}'
  USING SCHEMA '${schema}'

> PEEK data
2019-08-27 "2019-08-27 00:17:09.000877"
1970-01-01 "1970-01-01 00:01:05"

> select INTERVAL '1' SECOND
"0:00:01"

> select INTERVAL '2' MINUTE
"0:02:00"

> select INTERVAL '3' HOUR
"3:00:00"

> select INTERVAL '1' DAY
"24:00:00"

> select INTERVAL '6' MONTH
"0-6"

> select INTERVAL '10' YEAR
"10-0"

> select TIMESTAMP WITH TIME ZONE '1989-06-01 10:10:10.410+04:00'
"1989-06-01 06:10:10.410 UTC"

> select TIMESTAMPTZ '1989-06-01 9:10:10.410+07:00'
"1989-06-01 02:10:10.410 UTC"

> select TIMESTAMP WITHOUT TIME ZONE '1989-06-01 10:10:10.410+04:00'
"1989-06-01 10:10:10.410"

> select TIMESTAMP '1989-06-01 10:10:10.410+04:00'
"1989-06-01 10:10:10.410"

> select TIMESTAMPTZ '1989-06-01 9:10:10.410+7'
"1989-06-01 02:10:10.410 UTC"

> select TIMESTAMPTZ '1989-06-01 9:10:10.410+700'
"1989-06-01 02:10:10.410 UTC"
