# Copyright Materialize, Inc. and contributors. All rights reserved.
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

$ kafka-create-topic topic=data
$ kafka-ingest topic=data format=avro schema=${writer-schema}
{"a": {"long": 1}, "b": null, "c": null, "d": {"string": "d"}}
{"a": {"long": 2}, "b": {"long": 2}, "c": {"string": "foo"}, "d": {"long": 4}}
{"a": {"long": 2}, "b": {"long": 2}, "c": {"long": 3}, "d": {"string": "d"}}

> CREATE SOURCE unions
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${writer-schema}'

> SHOW COLUMNS FROM unions
name       nullable  type
-------------------------
a          false     bigint
b          true      bigint
c1         true      bigint
c2         true      text
d1         true      bigint
d2         true      text

> SELECT * FROM unions
a   b       c1      c2     d1      d2
------------------------------------------
1   <null>  <null>  <null>  <null>  d
2   2       <null>  foo     4       <null>
2   2       3       <null>  <null>  d
