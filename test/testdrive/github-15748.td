# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_envelope_materialize = true

#
# This test creates a single timestamp with multiple events in it
#

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

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


#
# Insert some rows and then delete, upsert and insert even more in the same timestamp
#

$ kafka-create-topic topic=topic1
> CREATE SOURCE source1
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE MATERIALIZE
> CREATE SINK sink1 FROM source1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}') KEY (a) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE UPSERT

# We start with 10 records

$ kafka-ingest format=avro topic=topic1 schema=${schema}
{"array":[{"data":{"a":10,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":9,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":8,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":7,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":6,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":5,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":4,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":2,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":3,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":1,"b":1},"time":1,"diff":1}]}

# All of the below happens in a single timestamp "time":2

# Delete 2 records
$ kafka-ingest format=avro topic=topic1 schema=${schema}
{"array":[{"data":{"a":7,"b":1},"time":2,"diff":-1}]}
{"array":[{"data":{"a":3,"b":1},"time":2,"diff":-1}]}

# Upsert 2 records
$ kafka-ingest format=avro topic=topic1 schema=${schema}
{"array":[{"data":{"a":8,"b":1},"time":2,"diff":-1}]}
{"array":[{"data":{"a":2,"b":1},"time":2,"diff":-1}]}
{"array":[{"data":{"a":8,"b":8},"time":2,"diff":1}]}
{"array":[{"data":{"a":2,"b":2},"time":2,"diff":1}]}

# Insert 2 records
$ kafka-ingest format=avro topic=topic1 schema=${schema}
{"array":[{"data":{"a":0,"b":0},"time":2,"diff":1}]}
{"array":[{"data":{"a":15,"b":15},"time":2,"diff":1}]}

# Emit the progress
$ kafka-ingest format=avro topic=topic1 schema=${schema}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[10],"counts":[{"time":1,"count":10}, {"time":2,"count":8}]}}

$ kafka-verify-data headers=materialize-timestamp format=avro topic=testdrive-sink1-${testdrive.seed} sort-messages=true
1	{"a": 1} {"a": 1, "b": 1}
1	{"a": 10} {"a": 10, "b": 1}
1	{"a": 2} {"a": 2, "b": 1}
1	{"a": 3} {"a": 3, "b": 1}
1	{"a": 4} {"a": 4, "b": 1}
1	{"a": 5} {"a": 5, "b": 1}
1	{"a": 6} {"a": 6, "b": 1}
1	{"a": 7} {"a": 7, "b": 1}
1	{"a": 8} {"a": 8, "b": 1}
1	{"a": 9} {"a": 9, "b": 1}

$ kafka-verify-data headers=materialize-timestamp format=avro topic=testdrive-sink1-${testdrive.seed} sort-messages=true
2	{"a": 0} {"a": 0, "b": 0}
2	{"a": 15} {"a": 15, "b": 15}
2	{"a": 2} {"a": 2, "b": 2}
2	{"a": 3}
2	{"a": 7}
2	{"a": 8} {"a": 8, "b": 8}
