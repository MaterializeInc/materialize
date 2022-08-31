# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test Avro Sinks in general. This tests things that are not specific to an
# envelope. Mostly that we can correctly encode various data types and how we
# determine field names. This uses ENVELOPE DEBEZIUM implicitly but the tested
# behavior is not specific to DEBEZIUM sinks.

# Test that we invent field names for unnamed columns.

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

# See #9723
#> CREATE MATERIALIZED VIEW unnamed_cols AS SELECT 1, 2 AS b, 3;
#
#> CREATE SINK unnamed_cols_sink FROM unnamed_cols
#  INTO KAFKA CONNECTION kafka_conn TOPIC 'unnamed-cols-sink'
#  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
#
#$ kafka-verify format=avro sink=materialize.public.unnamed_cols_sink
#{"before": null, "after": {"row": {"column1": 1, "b": 2, "column3": 3}}}

# Test that invented field names do not clash with named columns.

# See #9723
#> CREATE MATERIALIZED VIEW clashing_cols AS SELECT 1, 2 AS column1, 3 as b, 4 as b2, 5 as b3;
#
#> CREATE SINK clashing_cols_sink FROM clashing_cols
#  INTO KAFKA CONNECTION kafka_conn TOPIC 'clashing-cols-sink'
#  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
#
#$ kafka-verify format=avro sink=materialize.public.clashing_cols_sink
#{"before": null, "after": {"row": {"column1": 1, "column1_1": 2, "b": 3, "b2": 4, "b3": 5}}}

# Test date/time types.

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> CREATE MATERIALIZED VIEW datetime_data (date, ts, ts_tz) AS VALUES
  (DATE '2000-01-01', TIMESTAMP '2000-01-01 10:10:10.111', TIMESTAMPTZ '2000-01-01 10:10:10.111+02'),
  (DATE '2000-02-01', TIMESTAMP '2000-02-01 10:10:10.111', TIMESTAMPTZ '2000-02-01 10:10:10.111+02')

> CREATE SINK datetime_data_sink FROM datetime_data
  INTO KAFKA CONNECTION kafka_conn TOPIC 'datetime-data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

$ kafka-verify format=avro sink=materialize.public.datetime_data_sink sort-messages=true
{"before": null, "after": {"row": {"date": 10957, "ts": 946721410111000, "ts_tz": 946714210111000}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"date": 10988, "ts": 949399810111000, "ts_tz": 949392610111000}}, "transaction": {"id": "<TIMESTAMP>"}}

> CREATE MATERIALIZED VIEW time_data (time) AS VALUES (TIME '01:02:03'), (TIME '01:02:04')

> CREATE SINK time_data_sink FROM time_data
  INTO KAFKA CONNECTION kafka_conn TOPIC 'time-data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

$ kafka-verify format=avro sink=materialize.public.time_data_sink sort-messages=true
{"before": null, "after": {"row": {"time": 3723000000}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"time": 3724000000}}, "transaction": {"id": "<TIMESTAMP>"}}

# Test jsonb

> CREATE MATERIALIZED VIEW json_data (a, b) AS VALUES ('{"a":1, "b":2}'::jsonb, 2)

# Sinks with JSON columns should not crash - see https://github.com/MaterializeInc/materialize/issues/4722
> CREATE SINK json_data_sink FROM json_data
  INTO KAFKA CONNECTION kafka_conn TOPIC 'json-data-sink'
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

# Test map

> CREATE MATERIALIZED VIEW map_data (map) AS SELECT '{a => 1, b => 2}'::map[text=>int];

> CREATE SINK map_sink FROM map_data
  INTO KAFKA CONNECTION kafka_conn TOPIC 'map-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

$ kafka-verify format=avro sink=materialize.public.map_sink sort-messages=true
{"before": null, "after": {"row": {"map": {"a": {"int": 1}, "b": {"int": 2}}}}, "transaction": {"id": "<TIMESTAMP>"}}

> CREATE MATERIALIZED VIEW list_data (list) AS SELECT LIST[1, 2];

> CREATE SINK list_sink FROM list_data
  INTO KAFKA CONNECTION kafka_conn TOPIC 'list-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

$ kafka-verify format=avro sink=materialize.public.list_sink sort-messages=true
{"before": null, "after": {"row": {"list": [{"int": 1}, {"int": 2}]}}, "transaction": {"id": "<TIMESTAMP>"}}

# Test optional namespace for auto-generated value schema
> CREATE MATERIALIZED VIEW namespace_value_data (namespace) AS SELECT 1;

> CREATE SINK namespace_value_sink FROM namespace_value_data
  INTO KAFKA CONNECTION kafka_conn TOPIC 'namespace-value-sink'
  WITH (avro_value_fullname = 'abc.def.ghi')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

$ kafka-verify-schema format=avro sink=materialize.public.namespace_value_sink
{"type":"record","name":"ghi","namespace":"abc.def","fields":[{"name":"before","type":["null",{"type":"record","name":"row","fields":[{"name":"namespace","type":"int"}]}]},{"name":"after","type":["null","row"]},{"name":"transaction","type":{"type":"record","name":"transaction","fields":[{"name":"id","type":"string"}]}}]}

$ kafka-verify format=avro sink=materialize.public.namespace_value_sink sort-messages=true
{"before": null, "after": {"row": {"namespace": 1}}, "transaction": {"id": "<TIMESTAMP>"}}

# Test optional namespaces for autogenerated key and value schemas
> CREATE MATERIALIZED VIEW namespace_key_value_data (a, b) AS SELECT * FROM (VALUES (1, 2));

> CREATE SINK namespace_key_value_sink FROM namespace_key_value_data
  INTO KAFKA CONNECTION kafka_conn TOPIC 'namespace-key-value-sink'
  KEY (b)
  WITH (avro_key_fullname = 'some.neat.class.foo', avro_value_fullname = 'some.neat.class.bar')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

$ kafka-verify-schema format=avro sink=materialize.public.namespace_key_value_sink
{"type":"record","name":"foo","namespace":"some.neat.class","fields":[{"name":"b","type":"int"}]}
{"type":"record","name":"bar","namespace":"some.neat.class","fields":[{"name":"before","type":["null",{"type":"record","name":"row","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"}]}]},{"name":"after","type":["null","row"]},{"name":"transaction","type":{"type":"record","name":"transaction","fields":[{"name":"id","type":"string"}]}}]}

$ kafka-verify format=avro sink=materialize.public.namespace_key_value_sink sort-messages=true
{"b": 2} {"before": null, "after": {"row": {"a": 1, "b": 2}}, "transaction": {"id": "<TIMESTAMP>"}}

# Bad Sinks

> CREATE MATERIALIZED VIEW input (a, b) AS SELECT * FROM (VALUES (1, 2))

! CREATE SINK bad_sink FROM input
  INTO KAFKA CONNECTION kafka_conn TOPIC 'input-sink' KEY (a, a)
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
contains:Repeated column name in sink key: a

! CREATE SINK bad_sink FROM input
  INTO KAFKA CONNECTION kafka_conn TOPIC 'input-sink'
  WITH (avro_value_fullname = 'some.neat.class.foo', avro_key_fullname = 'some.neat.class.bar')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
contains:Cannot specify avro_key_fullname without a corresponding KEY field

! CREATE SINK bad_sink FROM input
  INTO KAFKA CONNECTION kafka_conn TOPIC 'input-sink'
  WITH (avro_key_fullname = 'some.neat.class.bar')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
contains:Cannot specify avro_key_fullname without a corresponding KEY field

! CREATE SINK bad_sink FROM input
  INTO KAFKA CONNECTION kafka_conn TOPIC 'input-sink' KEY (a)
  WITH (avro_key_fullname = 'some.neat.class.bar')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
contains:Must specify both avro_key_fullname and avro_value_fullname when specifying generated schema names

! CREATE SINK bad_sink FROM input
  INTO KAFKA CONNECTION kafka_conn TOPIC 'input-sink' KEY (a)
  WITH (avro_value_fullname = 'some.neat.class.bar')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
contains:Must specify both avro_key_fullname and avro_value_fullname when specifying generated schema names
