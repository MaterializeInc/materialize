
# Copyright Materialize, Inc. All rights reserved.
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

$ kafka-create-topic topic=input-consistency
$ kafka-create-topic topic=input

> CREATE MATERIALIZED SOURCE input_kafka_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
    WITH (consistency = 'testdrive-input-consistency-${testdrive.seed}')
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

> CREATE VIEW input_kafka_no_byo_join_view AS SELECT * FROM input_kafka_byo, input_kafka_no_byo;

> CREATE MATERIALIZED VIEW input_kafka_no_byo_join_mview AS SELECT * FROM input_kafka_no_byo, input_kafka_byo;

> CREATE MATERIALIZED VIEW input_kafka_no_byo_scalar_subquery AS SELECT (SELECT a FROM input_kafka_no_byo LIMIT 1) FROM input_kafka_byo;

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
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

! CREATE SINK output2 FROM input_kafka_no_byo
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all input sources of an exactly-once Kafka sink must be replayable, materialize.public.input_kafka_no_byo is not

! CREATE SINK output3 FROM input_table
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all inputs of an exactly-once Kafka sink must be sources, materialize.public.input_table is not

> CREATE SINK output4 FROM input_kafka_byo_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK output4_view FROM input_kafka_byo_mview_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

! CREATE SINK output5 FROM input_kafka_no_byo_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all input sources of an exactly-once Kafka sink must be replayable, materialize.public.input_kafka_no_byo is not

! CREATE SINK output5_view FROM input_kafka_no_byo_mview_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all input sources of an exactly-once Kafka sink must be replayable, materialize.public.input_kafka_no_byo is not

! CREATE SINK output6 FROM input_table_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all inputs of an exactly-once Kafka sink must be sources, materialize.public.input_table is not

! CREATE SINK output7 FROM input_values_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all inputs of an exactly-once Kafka sink must be sources, materialize.public.input_values_view is not

! CREATE SINK output8 FROM input_values_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all inputs of an exactly-once Kafka sink must be sources, materialize.public.input_values_mview is not

! CREATE SINK output9 FROM input_kafka_no_byo_join_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all input sources of an exactly-once Kafka sink must be replayable, materialize.public.input_kafka_no_byo is not

! CREATE SINK output9 FROM input_kafka_no_byo_join_mview
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all input sources of an exactly-once Kafka sink must be replayable, materialize.public.input_kafka_no_byo is not

! CREATE SINK output9 FROM input_kafka_no_byo_scalar_subquery
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all input sources of an exactly-once Kafka sink must be replayable, materialize.public.input_kafka_no_byo is not

! CREATE SINK output9 FROM input_kafka_no_byo_derived_table
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all input sources of an exactly-once Kafka sink must be replayable, materialize.public.input_kafka_no_byo is not

! CREATE SINK output9 FROM input_csv
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-view-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
all input sources of an exactly-once Kafka sink must be replayable, materialize.public.input_csv is not
