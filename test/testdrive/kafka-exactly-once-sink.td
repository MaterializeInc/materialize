
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=\d{13} replacement=<TIMESTAMP>

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

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE input_kafka_cdcv2
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-input_cdcv2-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${cdcv2-schema}' ENVELOPE MATERIALIZE

> CREATE SOURCE input_kafka_dbz
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-input_dbz-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${dbz-schema}' ENVELOPE DEBEZIUM

> CREATE TABLE input_table (a bigint, b bigint)

> CREATE MATERIALIZED VIEW input_kafka_cdcv2_mview AS SELECT a + 2 AS a , b + 10 AS b from input_kafka_cdcv2;

> CREATE MATERIALIZED VIEW input_kafka_cdcv2_mview_view AS SELECT * FROM input_kafka_cdcv2_mview;

> CREATE VIEW input_kafka_dbz_view AS SELECT a + 2 AS a , b + 10 AS b from input_kafka_dbz;

> CREATE MATERIALIZED VIEW input_kafka_dbz_view_mview AS SELECT * FROM input_kafka_dbz_view;

> CREATE MATERIALIZED VIEW input_table_mview AS SELECT a + 2 AS a , b + 10 AS b from input_table;

> CREATE VIEW input_values_view AS VALUES (1), (2), (3);

> CREATE MATERIALIZED VIEW input_values_mview AS VALUES (1), (2), (3);

> CREATE MATERIALIZED VIEW input_kafka_dbz_derived_table AS SELECT * FROM ( SELECT * FROM input_kafka_dbz ) AS a1;

$ kafka-create-topic topic=static
$ kafka-ingest topic=static format=bytes
city,state,zip
Rochester,NY,14618
New York,NY,10004
"bad,place""",CA,92679

> CREATE SOURCE input_csv
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-static-${testdrive.seed}')
  FORMAT CSV WITH 3 COLUMNS

> CREATE SINK output1 FROM input_kafka_cdcv2
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output1-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK output2 FROM input_kafka_dbz
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output2-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK output3 FROM input_table
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output3-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK output4 FROM input_kafka_cdcv2_mview
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output4-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK output4_view FROM input_kafka_cdcv2_mview_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output4b-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

! CREATE SINK output5 FROM input_kafka_dbz_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output5-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
contains:input_kafka_dbz_view is a view, which cannot be exported as a sink

> CREATE SINK output5_view FROM input_kafka_dbz_view_mview
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output5b-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK output6 FROM input_table_mview
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output6-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

! CREATE SINK output7 FROM input_values_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output7-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
contains:input_values_view is a view, which cannot be exported as a sink

> CREATE SINK output8 FROM input_values_mview
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output8-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK output12 FROM input_kafka_dbz_derived_table
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output12-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK output13 FROM input_csv
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output13-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK output14_custom_consistency_topic FROM input_kafka_cdcv2
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output14-view-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

# We need some data -- any data -- to start creating timestamp bindings
$ kafka-ingest format=avro topic=input_cdcv2 schema=${cdcv2-schema}
{"array":[{"data":{"a":1,"b":1},"time":1,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[1],"counts":[{"time":1,"count":1}]}}

$ kafka-ingest format=avro topic=input_dbz schema=${dbz-schema} timestamp=1
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "source": {"lsn": {"long": 3}, "sequence": {"string": "[\"1\", \"3\"]"}, "snapshot": {"string": "false"}}, "op": "c"}

# ensure that the sink works with log compaction enabled on the consistency topic

$ kafka-create-topic topic=compaction-test-input

# create topic with known compaction settings, instead of letting
# Materialize create it when creating the sink
$ kafka-create-topic topic=compaction-test-output-consistency compaction=true

> CREATE SOURCE compaction_test_input
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-compaction-test-input-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${cdcv2-schema}' ENVELOPE MATERIALIZE

> CREATE SINK compaction_test_sink FROM compaction_test_input
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'compaction-test-output-${testdrive.seed}')
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE DEBEZIUM

$ kafka-ingest format=avro topic=compaction-test-input schema=${cdcv2-schema}
{"array":[{"data":{"a":1,"b":1},"time":1,"diff":1}]}
{"array":[{"data":{"a":2,"b":2},"time":1,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[2],"counts":[{"time":1,"count":2}]}}

# We cannot observe the consistency topic, because compaction is not deterministic.
# We indirectly test this by verifying that the data output is correct. If this
# output were not here, something must have gone wrong with comitting to the
# consistency topic.

$ kafka-verify format=avro sink=materialize.public.compaction_test_sink sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 2}}, "transaction": {"id": "1"}}
