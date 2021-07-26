
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
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
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ set trxschema={
    "type":"record", "name":"TransactionMetadataValue", "namespace":"io.debezium.connector.common",
    "fields":[
    {"name":"status","type":"string"},
    {"name":"id","type": "string"},
    {"name": "event_count",
    "type": ["null", "long"],
    "default": null
    },
    {"name":"data_collections","type":["null",{"type":"array",
    "items": {"type":"record",
    "name":"ConnectDefault",
    "namespace":"io.confluent.connect.Avro",
    "fields": [ {
    "name": "data_collection",
    "type": "string"
    },
    {
    "name": "event_count",
    "type": "long" }]}}],
    "default": null}],
    "connect.name": "io.debezium.connector.common.TransactionMetadataValue"
    }

$ kafka-create-topic topic=input-consistency
$ kafka-create-topic topic=input

> CREATE MATERIALIZED SOURCE input_kafka_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
    WITH (consistency_topic = 'testdrive-input-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE MATERIALIZED SOURCE input_kafka_no_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE TABLE input_table (a bigint, b bigint)

> CREATE MATERIALIZED VIEW input_kafka_byo_mview AS SELECT a + 2 AS a , b + 10 AS b from input_kafka_byo;

> CREATE MATERIALIZED VIEW input_kafka_byo_mview_view AS SELECT * FROM input_kafka_byo_mview;

> CREATE VIEW input_kafka_no_byo_mview AS SELECT a + 2 AS a , b + 10 AS b from input_kafka_no_byo;

> CREATE MATERIALIZED VIEW input_kafka_no_byo_mview_view AS SELECT * FROM input_kafka_no_byo_mview;

> CREATE MATERIALIZED VIEW input_table_mview AS SELECT a + 2 AS a , b + 10 AS b from input_table;

> CREATE VIEW input_values_view AS VALUES (1), (2), (3);

> CREATE MATERIALIZED VIEW input_values_mview AS VALUES (1), (2), (3);

> CREATE MATERIALIZED VIEW input_kafka_no_byo_derived_table AS SELECT * FROM ( SELECT * FROM input_kafka_no_byo ) AS a1;

$ file-append path=static.csv
city,state,zip
Rochester,NY,14618
New York,NY,10004
"bad,place""",CA,92679

> CREATE SOURCE input_csv
  FROM FILE '${testdrive.temp-dir}/static.csv'
  FORMAT CSV WITH 3 COLUMNS

> CREATE SINK output1 FROM input_kafka_byo
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output1-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output1-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output2 FROM input_kafka_no_byo
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output2-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output2-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

! CREATE SINK output3 FROM input_table
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output3-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output3-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
reuse_topic requires that sink input dependencies are sources, materialize.public.input_table is not

> CREATE SINK output4 FROM input_kafka_byo_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output4-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output4-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output4_view FROM input_kafka_byo_mview_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output4b-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output4b-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output5 FROM input_kafka_no_byo_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output5-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output5-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output5_view FROM input_kafka_no_byo_mview_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output5b-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output5b-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

! CREATE SINK output6 FROM input_table_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output6-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output6-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
reuse_topic requires that sink input dependencies are sources, materialize.public.input_table is not

! CREATE SINK output7 FROM input_values_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output7-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output7-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
reuse_topic requires that sink input dependencies are sources, materialize.public.input_values_view is not

! CREATE SINK output8 FROM input_values_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output8-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output8-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
reuse_topic requires that sink input dependencies are sources, materialize.public.input_values_mview is not

> CREATE SINK output12 FROM input_kafka_no_byo_derived_table
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output12-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output12-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output13 FROM input_csv
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output13-view-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic='output13-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# ensure that the sink works with log compaction enabled on the consistency topic

$ kafka-create-topic topic=compaction-test-input-consistency
$ kafka-create-topic topic=compaction-test-input

# create topic with known compaction settings, instead of letting
# Materialize create it when creating the sink
$ kafka-create-topic topic=compaction-test-output-consistency compaction=true

> CREATE MATERIALIZED SOURCE compaction_test_input
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-compaction-test-input-${testdrive.seed}'
    WITH (consistency_topic = 'testdrive-compaction-test-input-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE SINK compaction_test_sink FROM compaction_test_input
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'compaction-test-output-${testdrive.seed}'
  WITH (reuse_topic=true, consistency_topic = 'testdrive-compaction-test-output-consistency-${testdrive.seed}') FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE DEBEZIUM

$ kafka-ingest format=avro topic=compaction-test-input schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 1, "b": 1}}}
{"before": null, "after": {"row": {"a": 2, "b": 2}}}

$ kafka-ingest format=avro topic=compaction-test-input-consistency timestamp=1 schema=${trxschema}
{"status":"BEGIN","id":"1","event_count":null,"data_collections":null}
{"status":"END","id":"1","event_count":{"long": 2},"data_collections":{"array": [{"event_count": 2, "data_collection": "testdrive-compaction-test-input-${testdrive.seed}"}]}}

# We cannot observe the consistency topic, because compaction is not deterministic.
# We indirectly test this by verifying that the data output is correct. If this
# output were not here, something must have gone wrong with comitting to the
# consistency topic.

$ kafka-verify format=avro sink=materialize.public.compaction_test_sink sort-messages=true
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 2}}, "transaction": {"id": "1"}}
