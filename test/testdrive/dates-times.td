# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test exercises dates and times at the boundary (e.g., by sending them
# through pgwire). Operations on dates and times are more thoroughly tested in
# dates-times.slt.

$ set schema={
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
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"d": 18135, "ts": 1566865029000000}
{"d": 0, "ts": 65000000}

> CREATE SOURCE data FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED VIEW data_view as SELECT * from data

> SELECT * FROM data_view
2019-08-27 "2019-08-27 00:17:09"
1970-01-01 "1970-01-01 00:01:05"

# Since the latest record in the topic is 10, the logical timestamp at which
# we read should be 10 - 1.
# This is no longer true after timestamping - commenting out
#> SELECT mz_now() FROM data_view LIMIT 1
#9

> SELECT INTERVAL '1' SECOND
"00:00:01"

> SELECT INTERVAL '2' MINUTE
"00:02:00"

> SELECT INTERVAL '3' HOUR
"03:00:00"

> SELECT INTERVAL '1' DAY
"1 day"

> SELECT INTERVAL '6' MONTH
"6 months"

> SELECT INTERVAL '10' YEAR
"10 years"

> SELECT TIMESTAMP WITH TIME ZONE '1989-06-01 10:10:10.410+04:00'
"1989-06-01 06:10:10.410 UTC"

> SELECT TIMESTAMPTZ '1989-06-01 9:10:10.410+07:00'
"1989-06-01 02:10:10.410 UTC"

# microseconds are returned with nanosecond precision in the binary format
> SELECT TIMESTAMPTZ '1989-06-01 06:10:10.12345678+00:00'
"1989-06-01 06:10:10.123457 UTC"

> SELECT TIMESTAMP WITHOUT TIME ZONE '1989-06-01 10:10:10.410+04:00'
"1989-06-01 10:10:10.410"

> SELECT TIMESTAMP '1989-06-01 10:10:10.410+04:00'
"1989-06-01 10:10:10.410"

> SELECT TIMESTAMPTZ '1989-06-01 9:10:10.410+7'
"1989-06-01 02:10:10.410 UTC"

> SELECT TIMESTAMPTZ '1989-06-01 9:10:10.410+700'
"1989-06-01 02:10:10.410 UTC"

> SELECT '1989-06-01 10:10:10.410+04:00'::timestamptz::text
"1989-06-01 06:10:10.41+00"

> SELECT '1989-06-01 10:10:10.413+04:00'::timestamptz::text
"1989-06-01 06:10:10.413+00"

# The text format should only ever return microseconds
> SELECT '1989-06-01 10:10:10.12345678+04:00'::timestamptz::text
"1989-06-01 06:10:10.123457+00"

> SELECT '1989-06-01 10:10:10.1234564+04:00'::timestamptz::text
"1989-06-01 06:10:10.123456+00"
