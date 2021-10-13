# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "int"}
    ]
  }

$ avro-ocf-write path=data.ocf schema=${writer-schema} codec=null
{"a": 1, "b": 2}
{"a": 3, "b": 4}

> CREATE MATERIALIZED SOURCE basic
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'

> SELECT * FROM basic
a  b  mz_obj_no
---------------
1  2  1
3  4  2

> SHOW COLUMNS FROM basic
name       nullable  type
-------------------------
a          false     bigint
b          false     integer
mz_obj_no  false     bigint

$ avro-ocf-write path=data-no-codec.ocf schema=${writer-schema}
{"a": 1, "b": 2}
{"a": 3, "b": 4}

> CREATE MATERIALIZED SOURCE basic_no_codec
  FROM AVRO OCF '${testdrive.temp-dir}/data-no-codec.ocf'

> SELECT * FROM basic_no_codec
a  b  mz_obj_no
---------------
1  2  1
3  4  2

> SHOW COLUMNS FROM basic_no_codec
name       nullable  type
-------------------------
a          false     bigint
b          false     integer
mz_obj_no  false     bigint

$ avro-ocf-write path=data-snappy.ocf schema=${writer-schema} codec=snappy
{"a": 1, "b": 2}
{"a": 3, "b": 4}

> CREATE MATERIALIZED SOURCE basic_snappy
  FROM AVRO OCF '${testdrive.temp-dir}/data-snappy.ocf'

> SELECT * FROM basic_snappy
a  b  mz_obj_no
---------------
1  2  1
3  4  2

> SHOW COLUMNS FROM basic_snappy
name       nullable  type
-------------------------
a          false     bigint
b          false     integer
mz_obj_no  false     bigint

$ set reader-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

> CREATE MATERIALIZED SOURCE reader_schema
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'
  WITH (reader_schema = '${reader-schema}')

> SELECT * FROM reader_schema
a  b  mz_obj_no
---------------
1  2  1
3  4  2

> SHOW COLUMNS FROM reader_schema
name       nullable  type
-------------------------
a          false     bigint
b          false     bigint
mz_obj_no  false     bigint

! CREATE MATERIALIZED SOURCE reader_schema
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'
  WITH (reader_schema = '{"bad": "news", "bears"')
validating avro value schema:

> CREATE MATERIALIZED SOURCE tailed
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf' WITH (tail = true)

> SELECT * FROM tailed
a  b  mz_obj_no
---------------
1  2  1
3  4  2

$ avro-ocf-append path=data.ocf
{"a": 7, "b": 10}
{"a": 9, "b": 12}

> SELECT * FROM tailed
a  b  mz_obj_no
---------------
1  2   1
3  4   2
7  10  3
9  12  4

> SELECT * FROM basic
a  b  mz_obj_no
---------------
1  2  1
3  4  2

$ set timestamp-schema={
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
          "connect.name": "io.debezium.time.MicroTimestamp",
          "logicalType": "timestamp-micros"
        }
      },
      {
        "name": "ts_tz",
        "type": {
          "type": "long",
          "connect.name": "io.debezium.time.MicroTimestamp",
          "logicalType": "timestamp-micros"
        }
      }
    ]
  }

$ avro-ocf-write path=timestamp.ocf schema=${timestamp-schema}
{"d": 10988, "ts": 949399810111000, "ts_tz": 949392610111000}
{"d": 10957, "ts": 946721410111000, "ts_tz": 946714210111000}

> CREATE MATERIALIZED SOURCE timestamp_source
  FROM AVRO OCF '${testdrive.temp-dir}/timestamp.ocf'

> SELECT * FROM timestamp_source
d     ts ts_tz  mz_obj_no
-------------------------
"2000-02-01" "2000-02-01 10:10:10.111" "2000-02-01 08:10:10.111" 1
"2000-01-01" "2000-01-01 10:10:10.111" "2000-01-01 08:10:10.111" 2

> SHOW COLUMNS FROM timestamp_source
name        nullable  type
-------------------------
d           false     date
ts          false     timestamp
ts_tz       false     timestamp
mz_obj_no   false     bigint

> CREATE SINK basic_sink_${testdrive.seed} FROM basic
  INTO AVRO OCF '${testdrive.temp-dir}/basic-sink.ocf'

$ avro-ocf-verify sink=materialize.public.basic_sink_${testdrive.seed}
{"before": null, "after": {"row": {"a": 1, "b": 2, "mz_obj_no": 1}}}
{"before": null, "after": {"row": {"a": 3, "b": 4, "mz_obj_no": 2}}}

> CREATE VIEW dateish AS
  SELECT d FROM timestamp_source

> CREATE SINK date_sink_${testdrive.seed} FROM dateish
  INTO AVRO OCF '${testdrive.temp-dir}/date-sink.ocf'

$ avro-ocf-verify sink=materialize.public.date_sink_${testdrive.seed}
{"before": null, "after": {"row": {"d": 10957}}}
{"before": null, "after": {"row": {"d": 10988}}}
