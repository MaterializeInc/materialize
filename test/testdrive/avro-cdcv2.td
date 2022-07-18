# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test support for Avro sources without using the Confluent Schema Registry.

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
              {
                "name": "id",
                "type": "long"
              },
              {
                "name": "price",
                "type": [
                  "null",
                  "int"
                ]
              }
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

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":5,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":4,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":5,"diff":-1}]}

# Create a source using an inline schema.

> CREATE SOURCE data_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE MATERIALIZE

$ kafka-ingest format=avro topic=data schema=${schema}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[3],"counts":[]}}
{"com.materialize.cdc.progress":{"lower":[3],"upper":[10],"counts":[{"time":4,"count":1},{"time":5,"count":2}, {"time": 6, "count": 1}]}}

> SELECT * FROM data_schema_inline
id price
--------
5 10

$ kafka-ingest format=avro topic=data schema=${schema}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":6,"diff":-1}]}

> SELECT * FROM data_schema_inline

# Inject "junk" with a previous timestamp, which could simulate a materialized
# that restarted and emits previously emitted data at a compacted timestamp

$ kafka-ingest format=avro topic=data schema=${schema}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":5,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":4,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":5,"diff":-1}]}

$ kafka-ingest format=avro topic=data schema=${schema}
{"com.materialize.cdc.progress":{"lower":[3],"upper":[6],"counts":[{"time":4,"count":1},{"time":5,"count":2}]}}

> SELECT * FROM data_schema_inline

# and now, new data again

$ kafka-ingest format=avro topic=data schema=${schema}
{"array":[{"data":{"id":6,"price":{"int":10}},"time":10,"diff":1}]}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=5
{"com.materialize.cdc.progress":{"lower":[10],"upper":[15],"counts":[{"time":10,"count":1}]}}

> SELECT * FROM data_schema_inline
id price
--------
6 10

# Test that tails report progress messages even without new data
> BEGIN

> DECLARE c CURSOR FOR TAIL data_schema_inline WITH (SNAPSHOT = FALSE, PROGRESS = TRUE);

> FETCH 1 FROM c WITH (timeout = '60s')
15 true <null> <null> <null>

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=6
{"com.materialize.cdc.progress":{"lower":[15],"upper":[20],"counts":[]}}

> FETCH 1 FROM c WITH (timeout = '60s')
20 true <null> <null> <null>
