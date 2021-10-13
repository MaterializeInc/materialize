# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SOURCE billing_source FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-messages-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Batch' USING SCHEMA '${testdrive.protobuf-descriptors}'

> CREATE MATERIALIZED VIEW billing AS SELECT * FROM billing_source

$ kafka-create-topic topic=messages

$ kafka-ingest format=protobuf topic=messages message=batch timestamp=1
{"id": "1", "interval_start": "2020-01-01_00:00:00", "interval_end": "2020-01-01_00:00:09", "records": []}
{"id": "2", "interval_start": "2020-01-01_00:00:10", "interval_end": "2020-01-01_00:00:19", "records": [{"interval_start": "2020-01-01_00:00:10", "interval_end": "2020-01-01_00:00:15", "meter": "user", "value": 25, "measurements": [{"resource": "CPU", "measured_value": 5}, {"resource": "MEM", "measured_value": 128}]}, {"interval_start": "2020-01-01_00:00:16", "interval_end": "2020-01-01_00:00:19", "meter": "user", "value": 125, "measurements": [{"resource": "CPU", "measured_value": 13}, {"resource": "MEM", "measured_value": 256}]}]}

# TODO: default values for enums, strings, bytes do not work right now
$ kafka-ingest format=protobuf topic=messages message=batch timestamp=10
{"id": "0", "interval_start": "0", "interval_end": "0", "records": []}

> SHOW COLUMNS FROM billing
id              true text
interval_end    true text
interval_start  true text
records         true list
mz_offset       false bigint

> SELECT id, interval_start, interval_end, mz_offset FROM billing
0 0                   0                    3
1 2020-01-01_00:00:00 2020-01-01_00:00:09  1
2 2020-01-01_00:00:10 2020-01-01_00:00:19  2

> SELECT
    (r)."value",
    (r).meter,
    (ms).resource,
    (ms).measured_value
  FROM
    billing, unnest(records) as r, unnest((r).measurements) as ms
 value  meter  resource  measured_value
----------------------------------------
    25  user   CPU                    5
    25  user   MEM                  128
   125  user   CPU                   13
   125  user   MEM                  256

# Do some destructuring over the nested records

> SELECT (r).meter FROM billing, unnest(records) AS r
user
user

> CREATE MATERIALIZED VIEW billing_records AS
  SELECT
    billing.id,
    (r).interval_start AS interval_start,
    (r).interval_end AS interval_end,
    (r).meter AS meter,
    ((r)."value")::float::int AS value,
    (r).measurements AS measurements
  FROM billing, unnest(records) AS r

> SHOW COLUMNS FROM billing_records
id              true  text
interval_end    true  text
interval_start  true  text
measurements    true  list
meter           true  text
value           true  integer

> SELECT id, interval_start, interval_end, value, meter FROM billing_records br
2 2020-01-01_00:00:10 2020-01-01_00:00:15 25  user
2 2020-01-01_00:00:16 2020-01-01_00:00:19 125 user
