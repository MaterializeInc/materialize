# Copyright Copyright 2019-2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> CREATE SOURCE billing_source FROM 'kafka://${testdrive.kafka-addr}/testdrive-messages-${testdrive.seed}'
  USING SCHEMA '${testdrive.protobuf-descriptors}'
  WITH (format = 'protobuf-descriptor', message_name = '.Batch')
> CREATE MATERIALIZED VIEW billing AS SELECT * FROM billing_source

$ kafka-ingest format=protobuf topic=messages message=.Batch timestamp=1
{"id": "1", "interval_start": "2020-01-01_00:00:00", "interval_end": "2020-01-01_00:00:09", "records": []}
{"id": "2", "interval_start": "2020-01-01_00:00:10", "interval_end": "2020-01-01_00:00:19", "records": [{"interval_start": "2020-01-01_00:00:10", "interval_end": "2020-01-01_00:00:15", "meter": "user", "value": 25, "measurements": [{"resource": "Cpu", "measured_value": 5}, {"resource": "Mem", "measured_value": 128}]}, {"interval_start": "2020-01-01_00:00:16", "interval_end": "2020-01-01_00:00:19", "meter": "user", "value": 125, "measurements": [{"resource": "Cpu", "measured_value": 13}, {"resource": "Mem", "measured_value": 256}]}]}

# TODO: default values for enums, strings, bytes do not work right now
$ kafka-ingest format=protobuf topic=messages message=.Batch timestamp=10
{"id": "0", "interval_start": "0", "interval_end": "0", "records": []}

> SHOW COLUMNS FROM billing
id              YES text
interval_end    YES text
interval_start  YES text
records         YES jsonb

# TODO: these should be fully json
> SELECT * FROM billing WHERE id = 1
1 2020-01-01_00:00:00 2020-01-01_00:00:09 []

> SELECT id, interval_end, interval_start FROM billing WHERE id = 2
2 2020-01-01_00:00:19 2020-01-01_00:00:10

> SELECT records->0->'value', records->1->'value' FROM billing WHERE id = 2
25.0 125.0

> SELECT
    records->0->'measurements'->0->'measured_value',
    records->1->'measurements'->1->'measured_value'
  FROM billing WHERE id = 2
5.0 256.0

> SELECT records->0->>'meter', records->1->>'meter' FROM billing WHERE id = 2
user user

# Do some destructuring over the nested records

> SELECT r.value->>'meter' FROM billing, jsonb_array_elements(records) AS r
user
user

> CREATE MATERIALIZED VIEW billing_records AS
  SELECT
    billing.id,
    r.value->>'interval_start' AS interval_start,
    r.value->>'interval_end' AS interval_end,
    r.value->>'meter' AS meter,
    (r.value->'value')::float::int AS value,
    r.value->'measurements' AS measurements
  FROM billing, jsonb_array_elements(records) AS r

> SHOW COLUMNS FROM billing_records
id              YES text
interval_end    YES text
interval_start  YES text
measurements    YES jsonb
meter           YES text
value           YES int4

> SELECT id, interval_start, interval_end, value, meter FROM billing_records br
2 2020-01-01_00:00:10 2020-01-01_00:00:15 25  user
2 2020-01-01_00:00:16 2020-01-01_00:00:19 125 user
