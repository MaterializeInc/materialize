
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

> CREATE MATERIALIZED SOURCE input_kafka_cdcv2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_cdcv2-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${cdcv2-schema}' ENVELOPE MATERIALIZE

> CREATE MATERIALIZED SOURCE input_kafka_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_dbz-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${dbz-schema}' ENVELOPE DEBEZIUM

> CREATE TABLE input_table (a bigint, b bigint)

> CREATE MATERIALIZED VIEW input_kafka_cdcv2_mview AS SELECT a + 2 AS a , b + 10 AS b from input_kafka_cdcv2;

> CREATE MATERIALIZED VIEW input_kafka_cdcv2_mview_view AS SELECT * FROM input_kafka_cdcv2_mview;

> CREATE VIEW input_kafka_dbz_mview AS SELECT a + 2 AS a , b + 10 AS b from input_kafka_dbz;

> CREATE MATERIALIZED VIEW input_kafka_dbz_mview_view AS SELECT * FROM input_kafka_dbz_mview;

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
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH 3 COLUMNS

> CREATE SINK output1 FROM input_kafka_cdcv2
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output1-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output2 FROM input_kafka_dbz
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output2-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

! CREATE SINK output3 FROM input_table
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output3-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
contains:reuse_topic requires that sink input dependencies are sources, materialize.public.input_table is not

> CREATE SINK output4 FROM input_kafka_cdcv2_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output4-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output4_view FROM input_kafka_cdcv2_mview_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output4b-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output5 FROM input_kafka_dbz_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output5-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output5_view FROM input_kafka_dbz_mview_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output5b-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

! CREATE SINK output6 FROM input_table_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output6-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
contains:reuse_topic requires that sink input dependencies are sources, materialize.public.input_table is not

! CREATE SINK output7 FROM input_values_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output7-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
contains:reuse_topic requires that sink input dependencies are sources, materialize.public.input_values_view is not

! CREATE SINK output8 FROM input_values_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output8-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
contains:reuse_topic requires that sink input dependencies are sources, materialize.public.input_values_mview is not

> CREATE SINK output12 FROM input_kafka_dbz_derived_table
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output12-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output13 FROM input_csv
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output13-view-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output14_custom_consistency_topic FROM input_kafka_cdcv2
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output14-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output14-custom-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

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

> CREATE MATERIALIZED SOURCE compaction_test_input
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-compaction-test-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${cdcv2-schema}' ENVELOPE MATERIALIZE

> CREATE SINK compaction_test_sink FROM compaction_test_input
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'compaction-test-output-${testdrive.seed}'
  WITH (reuse_topic=true) FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE DEBEZIUM

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

# verify output of consistency topic with real-time timestamp bindings

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ kafka-create-topic topic=rt-binding-consistency-test-input

> CREATE MATERIALIZED SOURCE rt_binding_consistency_test_source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-rt-binding-consistency-test-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${dbz-schema}' ENVELOPE DEBEZIUM

> CREATE SINK rt_binding_consistency_test_sink FROM rt_binding_consistency_test_source
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'rt-binding-consistency-test-output-${testdrive.seed}'
  WITH (reuse_topic=true) FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE DEBEZIUM

$ kafka-ingest format=avro topic=rt-binding-consistency-test-input schema=${dbz-schema} timestamp=1
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "source": {"lsn": {"long": 3}, "sequence": {"string": "[\"1\", \"3\"]"}, "snapshot": {"string": "false"}}, "op": "c"}

$ kafka-verify format=avro sink=materialize.public.rt_binding_consistency_test_sink
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "<TIMESTAMP>"}}

# Because the consistency topic produces a new end record every second, parital-search will wait
# for 10 seconds before continuing rather than the per-record timeout. Be wary of copy/pasting the
# `partial-search` arg.
$ kafka-verify format=avro sink=materialize.public.rt_binding_consistency_test_sink consistency=debezium partial-search=10
{"id": "<TIMESTAMP>", "status": "BEGIN", "event_count": null, "data_collections": null}
{"id": "<TIMESTAMP>", "status": "END", "event_count": {"long": 1}, "data_collections": {"array": [{"event_count": 1, "data_collection": "rt-binding-consistency-test-output-${testdrive.seed}"}]}}

# Test the CONSISTENCY TOPIC and CONSISTENCY FORMAT options

> CREATE VIEW simple_view AS SELECT 1 AS a, 2 AS b, 3 AS c;

# Can't provide CONSISTENCY FORMAT without a TOPIC
! CREATE SINK double_avro FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'double-avro'
  CONSISTENCY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (consistency_topic='willfail')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
contains:Expected TOPIC, found FORMAT

# Can't provide CONSISTENCY FORMAT and consistency_topic WITH option
! CREATE SINK double_avro FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'double-avro'
    CONSISTENCY TOPIC 'topicname'
    WITH (consistency_topic='willfail')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
contains:Cannot specify consistency_topic and CONSISTENCY options simultaneously

# Can't create sinks with JSON-encoded consistency topics
! CREATE SINK double_avro FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'double-avro'
    CONSISTENCY TOPIC 'consistency-double-avro' CONSISTENCY FORMAT JSON
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
contains:CONSISTENCY FORMAT JSON not yet supported

# Providing CONSISTENCY TOPIC without CONSISTENCY FORMAT will default to the sink's FORMAT
# of the sink, if valid
> CREATE SINK default_avro FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'default-avro'
    CONSISTENCY TOPIC 'consistency-default-avro'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# Fail to default to JSON
! CREATE SINK default_avro FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'default-avro'
    CONSISTENCY TOPIC 'consistency-default-avro'
  FORMAT JSON
contains:CONSISTENCY FORMAT JSON not yet supported

> CREATE SINK double_avro FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'double-avro'
    CONSISTENCY TOPIC 'consistency-double-avro' CONSISTENCY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.double_avro sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 2, "c": 3}}, "transaction": {"id": "0"}}

> CREATE SINK double_avro_2 FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'double-avro'
    CONSISTENCY (TOPIC 'consistency-double-avro' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.double_avro_2 sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 2, "c": 3}}, "transaction": {"id": "0"}}

> CREATE SINK json_avro FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-avro'
    CONSISTENCY TOPIC 'consistency-json-avro' CONSISTENCY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.json_avro sort-messages=true key=false
{"before": null, "after": {"a": 1, "b": 2, "c": 3}, "transaction": {"id": "0"}}

> CREATE SINK json_avro_2 FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-avro'
    CONSISTENCY (TOPIC 'consistency-json-avro' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}')
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.json_avro_2 sort-messages=true key=false
{"before": null, "after": {"a": 1, "b": 2, "c": 3}, "transaction": {"id": "0"}}

! CREATE SINK json_reuse_topic_no_parens FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-reuse-topic'
    WITH (reuse_topic=true)
  FORMAT JSON
contains:For FORMAT JSON, you need to manually specify an Avro consistency topic using 'CONSISTENCY TOPIC consistency_topic CONSISTENCY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY url'. The default of using a JSON consistency topic is not supported.

# This should succeed, but will incorrectly create a nonced topic.
# See https://github.com/MaterializeInc/materialize/issues/8231.
> CREATE SINK json_reuse_topic_no_parens FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-reuse-topic'
    CONSISTENCY TOPIC 'consistency-json-avro' CONSISTENCY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
    WITH (reuse_topic=true)
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.json_reuse_topic_no_parens sort-messages=true key=false
{"before": null, "after": {"a": 1, "b": 2, "c": 3}, "transaction": {"id": "0"}}

# This updated syntax will also succeed, creating a reusable topic.
> CREATE SINK json_reuse_topic_with_parens FROM rt_binding_consistency_test_source
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-reuse-topic-2'
    CONSISTENCY (TOPIC 'consistency-json-avro-2' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}')
    WITH (reuse_topic=true)
  FORMAT JSON

# Same as above!
> CREATE SINK json_reuse_topic_with_parens_and_with FROM rt_binding_consistency_test_source
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-reuse-topic-3'
    CONSISTENCY (TOPIC 'consistency-json-avro-3' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' WITH (dummy='unused'))
    WITH (reuse_topic=true)
  FORMAT JSON

# Default consistency format is Avro if consistency_topic is used
> CREATE SINK default_avro_2 FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'default-avro-2'
    WITH (consistency_topic='default-consistency-avro')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.default_avro sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 2, "c": 3}}, "transaction": {"id": "0"}}

> CREATE SINK json_avro_upsert_key FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-avro-upsert-key'
  KEY (b)
    CONSISTENCY TOPIC 'consistency-json-avro-upsert-key' CONSISTENCY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify format=json sink=materialize.public.json_avro_upsert_key key=true
{"b": 2} {"a": 1, "b": 2, "c": 3, "transaction": {"id": "0"}}

> CREATE SINK json_avro_upsert_key_2 FROM simple_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'json-avro-upsert-key'
  KEY (b)
    CONSISTENCY (TOPIC 'consistency-json-avro-upsert-key' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}')
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify format=json sink=materialize.public.json_avro_upsert_key_2 key=true
{"b": 2} {"a": 1, "b": 2, "c": 3, "transaction": {"id": "0"}}

# Verify compaction of exactly once sinks.

# TODO: we've disabled this after observing it be flaky in CI. Re-enable once we've
# fixed the issue.
# $ verify-timestamp-compaction source=input_csv max-size=3 permit-progress=true
# $ verify-timestamp-compaction source=rt_binding_consistency_test_source max-size=3 permit-progress=true
# $ verify-timestamp-compaction source=input_kafka_dbz max-size=3 permit-progress=true
# $ verify-timestamp-compaction source=input_kafka_cdcv2 max-size=3 permit-progress=true
