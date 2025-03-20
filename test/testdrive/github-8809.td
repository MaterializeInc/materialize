# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/database-issues/issues/8809.

$ set-arg-default single-replica-cluster=quickstart

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

$ kafka-create-topic topic=topic1
$ kafka-ingest format=avro topic=topic1 schema=${schema}
{"array":[{"data":{"a":1,"b":2},"time":1,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[10],"counts":[{"time":1,"count":1}]}}

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_envelope_materialize = true

> CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}');

> CREATE SOURCE source1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}')

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="5s"

> CREATE TABLE source1_tbl FROM SOURCE source1 (REFERENCE "testdrive-topic1-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE MATERIALIZE

> SELECT write_frontier, read_frontier < write_frontier
  FROM mz_internal.mz_frontiers
  JOIN mz_tables ON (id = object_id)
  WHERE name = 'source1_tbl'
10 true

> SELECT * FROM source1_tbl
1 2
