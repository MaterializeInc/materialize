# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test how Avro unions of varying sizes and nullabilities are converted
# to Materialize types.

$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": ["long"]},
      {"name": "b", "type": ["long", "null"]},
      {"name": "c", "type": ["long", "null", "string"]},
      {"name": "d", "type": ["long", "string"]}
    ]
  }

$ avro-ocf-write path=data.ocf schema=${writer-schema} codec=null
{"a": 1, "b": null, "c": null, "d": "d"}
{"a": 2, "b": 2, "c": "foo", "d": 4}
{"a": 2, "b": 2, "c": 3, "d": "d"}

> CREATE MATERIALIZED SOURCE unions
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'

> SHOW COLUMNS FROM unions
Field      Nullable  Type
-------------------------
a          NO        int8
b          YES       int8
c1         YES       int8
c2         YES       text
d1         YES       int8
d2         YES       text
mz_obj_no  NO        int8

> SELECT * FROM unions
a   b       c1      c2     d1      d2      mz_obj_no
----------------------------------------------------
1   <null>  <null>  <null>  <null>  d      1
2   2       <null>  foo     4       <null> 2
2   2       3       <null>  <null>  d      3
