# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# A test that timelines with objects that have released all write frontiers work
#

$ set schema=[
  {
    "type": "array",
    "items": {
      "type": "record",
      "name": "update",
      "namespace": "com.materialize.cdc",
      "fields": [
        {
          "name": "data",
          "type": {
            "type": "record",
            "name": "data",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          }
        },
        {
          "name": "time",
          "type": "long"
        },
        {
          "name": "diff",
          "type": "long"
        }
      ]
    }
  },
  {
    "type": "record",
    "name": "progress",
    "namespace": "com.materialize.cdc",
    "fields": [
      {
        "name": "lower",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "upper",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "counts",
        "type": {
          "type": "array",
          "items": {
            "type": "record",
            "name": "counts",
            "fields": [
              {
                "name": "time",
                "type": "long"
              },
              {
                "name": "count",
                "type": "long"
              }
            ]
          }
        }
      }
    ]
  }
  ]

$ kafka-create-topic topic=foo

$ kafka-ingest format=avro topic=foo schema=${schema}
{"array":[{"data":{"a":1,"b":1},"time":1,"diff":1}]}

> CREATE SOURCE data_foo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-foo-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE MATERIALIZE
  WITH (TIMELINE 'user')

> CREATE MATERIALIZED VIEW foo AS SELECT a, b FROM data_foo

$ kafka-ingest format=avro topic=foo schema=${schema}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[],"counts":[{"time":1,"count":1}]}}

> SELECT * FROM data_foo;
a  b
----
1  1

> SELECT * FROM foo;
a  b
----
1  1
