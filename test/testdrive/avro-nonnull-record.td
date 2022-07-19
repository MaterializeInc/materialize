# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for #7170, in which non-nullable fields of nullable records
# were not correctly handled.

$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {
        "name": "a",
        "type": {
          "name": "b",
          "fields": [
            {"name": "b", "type": "int"},
            {"name": "c", "type": "int"}
          ],
          "type": "record"
        }
      }
    ]
  }

$ kafka-create-topic topic=data
$ kafka-ingest topic=data format=avro schema=${writer-schema}
{"a": {"b": 1, "c": 1}}
{"a": {"b": 2, "c": 1}}

> CREATE SOURCE basic
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${writer-schema}'

> SELECT (b1.a).b, (b2.a).b FROM basic b1 LEFT JOIN basic b2 ON (b1.a).b = (b2.a).c
1 1
1 2
2 <null>
