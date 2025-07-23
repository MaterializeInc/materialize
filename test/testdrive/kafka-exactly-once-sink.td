# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SET transaction_isolation = 'strict serializable'
$ set-arg-default default-storage-size=1
$ set-arg-default single-replica-cluster=quickstart

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_envelope_materialize = true
ALTER SYSTEM SET max_clusters = 20

$ set cdcv2-schema=[
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

$ set dbz-key-schema={
    "type": "record",
    "name": "Key",
    "fields": [
      {
        "name": "a", "type": "long"
      }
    ]
  }

$ set dbz-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "op", "type": "string" },
      { "name": "after", "type": ["row", "null"] },
      {
        "name": "source",
        "type": {
          "type": "record",
          "name": "Source",
          "namespace": "whatever",
          "fields": [
            {
              "name": "snapshot",
              "type": [
                {
                  "type": "string",
                  "connect.version": 1,
                  "connect.parameters": {
                    "allowed": "true,last,false"
                  },
                  "connect.default": "false",
                  "connect.name": "io.debezium.data.Enum"
                },
                "null"
              ],
              "default": "false"
            },
            {
              "name": "lsn",
              "type": ["long", "null"]
            },
            {
              "name": "sequence",
              "type": ["string", "null"]
            }
          ]
        }
      }
    ]
  }

$ kafka-create-topic topic=input_dbz
$ kafka-create-topic topic=input_cdcv2

$ kafka-ingest format=avro topic=input_dbz key-format=avro key-schema=${dbz-key-schema} schema=${dbz-schema} timestamp=1
{"a": 1} {"before": null, "after": {"row": {"a": 1, "b": 1}}, "source": {"lsn": {"long": 3}, "sequence": {"string": "[\"1\", \"3\"]"}, "snapshot": {"string": "false"}}, "op": "c"}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CLUSTER input_kafka_cdcv2_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE input_kafka_cdcv2
  IN CLUSTER input_kafka_cdcv2_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-input_cdcv2-${testdrive.seed}')

> CREATE TABLE input_kafka_cdcv2_tbl FROM SOURCE input_kafka_cdcv2 (REFERENCE "testdrive-input_cdcv2-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${cdcv2-schema}' ENVELOPE MATERIALIZE

> CREATE CLUSTER input_kafka_dbz_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE input_kafka_dbz
  IN CLUSTER input_kafka_dbz_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-input_dbz-${testdrive.seed}')

> CREATE TABLE input_kafka_dbz_tbl FROM SOURCE input_kafka_dbz (REFERENCE "testdrive-input_dbz-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE DEBEZIUM

> CREATE TABLE input_table (a bigint, b bigint)

> CREATE MATERIALIZED VIEW input_kafka_cdcv2_mview AS SELECT a + 2 AS a , b + 10 AS b from input_kafka_cdcv2_tbl;

> CREATE MATERIALIZED VIEW input_kafka_cdcv2_mview_view AS SELECT * FROM input_kafka_cdcv2_mview;

> CREATE VIEW input_kafka_dbz_view AS SELECT a + 2 AS a , b + 10 AS b from input_kafka_dbz_tbl;

> CREATE MATERIALIZED VIEW input_kafka_dbz_view_mview AS SELECT * FROM input_kafka_dbz_view;

> CREATE MATERIALIZED VIEW input_table_mview AS SELECT a + 2 AS a , b + 10 AS b from input_table;

> CREATE VIEW input_values_view AS VALUES (1), (2), (3);

> CREATE MATERIALIZED VIEW input_values_mview AS VALUES (1), (2), (3);

> CREATE MATERIALIZED VIEW input_kafka_dbz_derived_table AS SELECT * FROM ( SELECT * FROM input_kafka_dbz_tbl ) AS a1;

$ kafka-create-topic topic=static
$ kafka-ingest topic=static format=bytes
city,state,zip
Rochester,NY,14618
New York,NY,10004
"bad,place""",CA,92679

> CREATE CLUSTER input_csv_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE input_csv
  IN CLUSTER input_csv_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-static-${testdrive.seed}')

> CREATE TABLE input_csv_tbl
  FROM SOURCE input_csv (REFERENCE "testdrive-static-${testdrive.seed}")
  FORMAT CSV WITH 3 COLUMNS

> CREATE CLUSTER output1_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output1
  IN CLUSTER output1_cluster
  FROM input_kafka_cdcv2_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output1-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE CLUSTER output2_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output2
  IN CLUSTER output2_cluster
  FROM input_kafka_dbz_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output2-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE CLUSTER output3_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output3
  IN CLUSTER output3_cluster
  FROM input_table
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output3-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE CLUSTER output4_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output4
  IN CLUSTER output4_cluster
  FROM input_kafka_cdcv2_mview
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output4-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE CLUSTER output4_view_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output4_view
  IN CLUSTER output4_view_cluster
  FROM input_kafka_cdcv2_mview_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output4b-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

! CREATE SINK output5
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input_kafka_dbz_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output5-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
contains:input_kafka_dbz_view is a view, which cannot be exported as a sink

> CREATE CLUSTER output5_view_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output5_view
  IN CLUSTER output5_view_cluster
  FROM input_kafka_dbz_view_mview
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output5b-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE CLUSTER output6_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output6
  IN CLUSTER output6_cluster
  FROM input_table_mview
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output6-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

! CREATE SINK output7
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input_values_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output7-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
contains:input_values_view is a view, which cannot be exported as a sink

> CREATE CLUSTER output8_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output8
  IN CLUSTER output8_cluster
  FROM input_values_mview
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output8-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE CLUSTER output12_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output12
  IN CLUSTER output12_cluster
  FROM input_kafka_dbz_derived_table
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output12-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE CLUSTER output13_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK output13
  IN CLUSTER output13_cluster
  FROM input_csv_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output13-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# We need some data -- any data -- to start creating timestamp bindings
$ kafka-ingest format=avro topic=input_cdcv2 schema=${cdcv2-schema}
{"array":[{"data":{"a":1,"b":1},"time":1,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[1],"counts":[{"time":1,"count":1}]}}

# verify output of progress topic with fixed timestamps
$ kafka-create-topic topic=progress-test-input

> CREATE CONNECTION kafka_fixed TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    PROGRESS TOPIC 'testdrive-progress-fixed-${testdrive.seed}',
    SECURITY PROTOCOL PLAINTEXT
  );

> CREATE CLUSTER compaction_test_input_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE compaction_test_input
  IN CLUSTER compaction_test_input_cluster
  FROM KAFKA CONNECTION kafka_fixed (TOPIC 'testdrive-progress-test-input-${testdrive.seed}')

> CREATE TABLE compaction_test_input_tbl FROM SOURCE compaction_test_input (REFERENCE "testdrive-progress-test-input-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${cdcv2-schema}' ENVELOPE MATERIALIZE

> CREATE CLUSTER compaction_test_sink_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK compaction_test_sink
  IN CLUSTER compaction_test_sink_cluster
  FROM compaction_test_input_tbl
  INTO KAFKA CONNECTION kafka_fixed (TOPIC 'compaction-test-output-${testdrive.seed}')
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE DEBEZIUM

# NB: the final timestamp is incomplete and should be present in neither output nor progress topic
$ kafka-ingest format=avro topic=progress-test-input schema=${cdcv2-schema}
{"array":[{"data":{"a":1,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":2,"b":2},"time":1,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[2],"counts":[{"time":1,"count":2}]}}
{"array":[{"data":{"a":2,"b":2},"time":3,"diff":1}]}

$ kafka-verify-topic sink=materialize.public.compaction_test_sink await-value-schema=true

$ kafka-verify-data headers=materialize-timestamp format=avro sink=materialize.public.compaction_test_sink sort-messages=true
1	{"before": null, "after": {"row": {"a": 1, "b": 1}}}
1	{"before": null, "after": {"row": {"a": 2, "b": 2}}}

> CREATE CLUSTER compaction_test_sink_check_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE compaction_test_sink_check
  IN CLUSTER compaction_test_sink_check_cluster
  FROM KAFKA CONNECTION kafka_fixed (TOPIC 'testdrive-progress-fixed-${testdrive.seed}')

> CREATE TABLE compaction_test_sink_check_tbl FROM SOURCE compaction_test_sink_check (REFERENCE "testdrive-progress-fixed-${testdrive.seed}")
  FORMAT JSON ENVELOPE NONE

# Retrieve all the progress messages that are beyond [2]. There should be
# exactly one of them because the upper of the CDCv2 stream stops at [2].
> SELECT data->'frontier'
  FROM compaction_test_sink_check_tbl
  WHERE data->'frontier'->0 IS NULL OR (data->'frontier'->0)::int >= 2
[2]

# verify output with real-time timestamps

$ kafka-create-topic topic=rt-binding-progress-test-input

$ kafka-ingest format=avro topic=rt-binding-progress-test-input key-format=avro key-schema=${dbz-key-schema} schema=${dbz-schema} timestamp=1
{"a": 1} {"before": null, "after": {"row": {"a": 1, "b": 1}}, "source": {"lsn": {"long": 3}, "sequence": {"string": "[\"1\", \"3\"]"}, "snapshot": {"string": "false"}}, "op": "c"}

> CREATE CLUSTER rt_binding_progress_test_source_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE rt_binding_progress_test_source
  IN CLUSTER rt_binding_progress_test_source_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rt-binding-progress-test-input-${testdrive.seed}')

> CREATE TABLE rt_binding_progress_test_source_tbl FROM SOURCE rt_binding_progress_test_source (REFERENCE "testdrive-rt-binding-progress-test-input-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE DEBEZIUM

> CREATE CLUSTER rt_binding_progress_test_sink_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK rt_binding_progress_test_sink
  IN CLUSTER rt_binding_progress_test_sink_cluster
  FROM rt_binding_progress_test_source_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'rt-binding-progress-test-output-${testdrive.seed}')
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE DEBEZIUM

$ kafka-verify-topic sink=materialize.public.rt_binding_progress_test_sink await-value-schema=true

$ kafka-verify-data format=avro sink=materialize.public.rt_binding_progress_test_sink
{"before": null, "after": {"row": {"a": 1, "b": 1}}}
