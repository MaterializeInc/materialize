# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set writer_schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "int"}
    ]
  }

$ avro-ocf-write path=data.ocf schema=${writer_schema}
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
Field      Nullable  Type
-------------------------
a          NO        int8
b          NO        int4
mz_obj_no  NO        int8


$ set reader_schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

> CREATE MATERIALIZED SOURCE reader_schema
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'
  WITH (reader_schema = '${reader_schema}')

> SELECT * FROM reader_schema
a  b  mz_obj_no
---------------
1  2  1
3  4  2

> SHOW COLUMNS FROM reader_schema
Field      Nullable  Type
-------------------------
a          NO        int8
b          NO        int8
mz_obj_no  NO        int8

! CREATE MATERIALIZED SOURCE reader_schema
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'
  WITH (reader_schema = '{"bad": "news", "bears"')
validating avro ocf reader schema:
