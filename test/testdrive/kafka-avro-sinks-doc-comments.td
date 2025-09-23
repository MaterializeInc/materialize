# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

$ set-arg-default default-storage-size=scale=1,workers=1
$ set-arg-default single-replica-cluster=quickstart

# Test Avro UPSERT sinks doc comments

> CREATE TYPE point AS (x integer, y integer);
> CREATE TYPE custom_map AS MAP (KEY TYPE = text, VALUE TYPE = bool)
> CREATE TABLE t (c1 point, c2 text NOT NULL, "c3_map[text=>text]" custom_map, c4 point list);
> INSERT INTO t SELECT ROW(1, 1)::point AS c1, 'text' AS c2, '{a=>true}'::custom_map as "c3_map[text=>text]", LIST[ROW(1, 1)::point] as c4;

> COMMENT ON TABLE t IS 'comment on table t with a \\ \';
> COMMENT ON COLUMN t."c3_map[text=>text]" IS 'comment on column t.c3_map with a ''';
> COMMENT ON COLUMN t.c4 IS 'comment on column t.c4 with an äöü';
> COMMENT ON TYPE point IS 'comment on type point';
> COMMENT ON COLUMN point.x IS 'comment on column point.x';

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CLUSTER sink1_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK sink1
  IN CLUSTER sink1_cluster
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
   DOC ON COLUMN t.c1 = 'doc on t.c1',
   VALUE DOC ON COLUMN t.c2 = 'value doc on t.c2',
   KEY DOC ON COLUMN t.c2 = 'key doc on t.c2',
   DOC ON COLUMN t.c4 = 'doc on t.c4',
   KEY DOC ON TYPE point = 'key doc on point',
   VALUE DOC ON TYPE point = 'value doc on point',
   KEY DOC ON TYPE t = 'key doc on t',
   VALUE DOC ON COLUMN point.y = 'value doc on point.y'
  )
  ENVELOPE UPSERT;

> SHOW CREATE SINK sink1;
name                            create_sql
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.sink1 "CREATE SINK materialize.public.sink1 IN CLUSTER sink1_cluster FROM materialize.public.t INTO KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'testdrive-sink1-${testdrive.seed}') KEY (c2) NOT ENFORCED FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_conn (DOC ON COLUMN materialize.public.t.c1 = 'doc on t.c1', VALUE DOC ON COLUMN materialize.public.t.c2 = 'value doc on t.c2', KEY DOC ON COLUMN materialize.public.t.c2 = 'key doc on t.c2', DOC ON COLUMN materialize.public.t.c4 = 'doc on t.c4', KEY DOC ON TYPE materialize.public.point = 'key doc on point', VALUE DOC ON TYPE materialize.public.point = 'value doc on point', KEY DOC ON TYPE materialize.public.t = 'key doc on t', VALUE DOC ON COLUMN materialize.public.point.y = 'value doc on point.y', DOC ON TYPE materialize.public.point = 'comment on type point', DOC ON COLUMN materialize.public.point.x = 'comment on column point.x', DOC ON TYPE materialize.public.t = 'comment on table t with a \\\\ \\', DOC ON COLUMN materialize.public.t.\"c3_map[text=>text]\" = 'comment on column t.c3_map with a ''') ENVELOPE UPSERT;"

$ unset-regex

$ schema-registry-verify schema-type=avro subject=testdrive-sink1-${testdrive.seed}-value
{"type":"record","name":"envelope","doc":"comment on table t with a \\\\ \\","fields":[{"name":"c1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","doc":"value doc on point","fields":[{"name":"x","type":["null","int"],"doc":"comment on column point.x"},{"name":"y","type":["null","int"],"doc":"value doc on point.y"}]}],"doc":"doc on t.c1"},{"name":"c2","type":"string","doc":"value doc on t.c2"},{"name":"c3_map_text__text_","type":["null",{"type":"map","values":["null","boolean"]}]},{"name":"c4","type":["null",{"type":"array","items":["null",{"type":"record","name":"record1","namespace":"com.materialize.sink","doc":"value doc on point","fields":[{"name":"x","type":["null","int"],"doc":"comment on column point.x"},{"name":"y","type":["null","int"],"doc":"value doc on point.y"}]}]}],"doc":"doc on t.c4"}]}

$ schema-registry-verify schema-type=avro subject=testdrive-sink1-${testdrive.seed}-key
{"type":"record","name":"row","doc":"key doc on t","fields":[{"name":"c2","type":"string","doc":"key doc on t.c2"}]}

> CREATE CLUSTER sink2_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK sink2
  IN CLUSTER sink2_cluster
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink2-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

$ schema-registry-verify schema-type=avro subject=testdrive-sink2-${testdrive.seed}-value
{"type":"record","name":"envelope","doc":"comment on table t with a \\\\ \\","fields":[{"name":"c1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","doc":"comment on type point","fields":[{"name":"x","type":["null","int"],"doc":"comment on column point.x"},{"name":"y","type":["null","int"]}]}]},{"name":"c2","type":"string"},{"name":"c3_map_text__text_","type":["null",{"type":"map","values":["null","boolean"]}]},{"name":"c4","type":["null",{"type":"array","items":["null",{"type":"record","name":"record1","namespace":"com.materialize.sink","doc":"comment on type point","fields":[{"name":"x","type":["null","int"],"doc":"comment on column point.x"},{"name":"y","type":["null","int"]}]}]}],"doc":"comment on column t.c4 with an äöü"}]}

> CREATE CLUSTER sink3_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK sink3
  IN CLUSTER sink3_cluster
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink3-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON COLUMN t.c2 = 'doc on t.c2'
  )
  ENVELOPE DEBEZIUM;

$ schema-registry-verify schema-type=avro subject=testdrive-sink3-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","doc":"comment on table t with a \\\\ \\","fields":[{"name":"c1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","doc":"comment on type point","fields":[{"name":"x","type":["null","int"],"doc":"comment on column point.x"},{"name":"y","type":["null","int"]}]}]},{"name":"c2","type":"string","doc":"doc on t.c2"},{"name":"c3_map_text__text_","type":["null",{"type":"map","values":["null","boolean"]}]},{"name":"c4","type":["null",{"type":"array","items":["null",{"type":"record","name":"record1","namespace":"com.materialize.sink","doc":"comment on type point","fields":[{"name":"x","type":["null","int"],"doc":"comment on column point.x"},{"name":"y","type":["null","int"]}]}]}],"doc":"comment on column t.c4 with an äöü"}]}]},{"name":"after","type":["null","row"]}]}

$ schema-registry-verify schema-type=avro subject=testdrive-sink3-${testdrive.seed}-key
{"type":"record","name":"row","doc":"comment on table t with a \\\\ \\","fields":[{"name":"c2","type":"string","doc":"doc on t.c2"}]}

# Explain schema. Note that we intentionally use a sink name that already exists
# to ensure that `EXPLAIN SCHEMA` doesn't complain if the specified sink name is
# already in use.
> EXPLAIN VALUE SCHEMA FOR CREATE SINK sink1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink3-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS = TRUE
  )
  ENVELOPE DEBEZIUM;
"{\n  \"type\": \"record\",\n  \"name\": \"envelope\",\n  \"fields\": [\n    {\n      \"name\": \"before\",\n      \"type\": [\n        \"null\",\n        {\n          \"type\": \"record\",\n          \"name\": \"row\",\n          \"doc\": \"comment on table t with a \\\\\\\\ \\\\\",\n          \"fields\": [\n            {\n              \"name\": \"c1\",\n              \"type\": [\n                \"null\",\n                {\n                  \"type\": \"record\",\n                  \"name\": \"record0\",\n                  \"namespace\": \"com.materialize.sink\",\n                  \"doc\": \"comment on type point\",\n                  \"fields\": [\n                    {\n                      \"name\": \"x\",\n                      \"type\": [\n                        \"null\",\n                        \"int\"\n                      ],\n                      \"default\": null,\n                      \"doc\": \"comment on column point.x\"\n                    },\n                    {\n                      \"name\": \"y\",\n                      \"type\": [\n                        \"null\",\n                        \"int\"\n                      ],\n                      \"default\": null\n                    }\n                  ]\n                }\n              ],\n              \"default\": null\n            },\n            {\n              \"name\": \"c2\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"c3_map_text__text_\",\n              \"type\": [\n                \"null\",\n                {\n                  \"type\": \"map\",\n                  \"values\": [\n                    \"null\",\n                    \"boolean\"\n                  ]\n                }\n              ],\n              \"default\": null\n            },\n            {\n              \"name\": \"c4\",\n              \"type\": [\n                \"null\",\n                {\n                  \"type\": \"array\",\n                  \"items\": [\n                    \"null\",\n                    {\n                      \"type\": \"record\",\n                      \"name\": \"record1\",\n                      \"namespace\": \"com.materialize.sink\",\n                      \"doc\": \"comment on type point\",\n                      \"fields\": [\n                        {\n                          \"name\": \"x\",\n                          \"type\": [\n                            \"null\",\n                            \"int\"\n                          ],\n                          \"default\": null,\n                          \"doc\": \"comment on column point.x\"\n                        },\n                        {\n                          \"name\": \"y\",\n                          \"type\": [\n                            \"null\",\n                            \"int\"\n                          ],\n                          \"default\": null\n                        }\n                      ]\n                    }\n                  ]\n                }\n              ],\n              \"default\": null,\n              \"doc\": \"comment on column t.c4 with an äöü\"\n            }\n          ]\n        }\n      ],\n      \"default\": null\n    },\n    {\n      \"name\": \"after\",\n      \"type\": [\n        \"null\",\n        \"row\"\n      ],\n      \"default\": null\n    }\n  ]\n}"

> EXPLAIN KEY SCHEMA AS JSON FOR CREATE SINK sink1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink3-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
"{\n  \"type\": \"record\",\n  \"name\": \"row\",\n  \"doc\": \"comment on table t with a \\\\\\\\ \\\\\",\n  \"fields\": [\n    {\n      \"name\": \"c2\",\n      \"type\": \"string\"\n    }\n  ]\n}"

# Updated comment is reflected right away in explain schema
> COMMENT ON COLUMN t.c2 IS 'comment on t.c2';

> EXPLAIN VALUE SCHEMA FOR CREATE SINK sink1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink3-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS = TRUE,
    VALUE DOC ON TYPE t = 'body of the upsert',
    KEY DOC ON TYPE t = 'key of the upsert'
  )
  ENVELOPE UPSERT;
"{\n  \"type\": \"record\",\n  \"name\": \"envelope\",\n  \"doc\": \"body of the upsert\",\n  \"fields\": [\n    {\n      \"name\": \"c1\",\n      \"type\": [\n        \"null\",\n        {\n          \"type\": \"record\",\n          \"name\": \"record0\",\n          \"namespace\": \"com.materialize.sink\",\n          \"doc\": \"comment on type point\",\n          \"fields\": [\n            {\n              \"name\": \"x\",\n              \"type\": [\n                \"null\",\n                \"int\"\n              ],\n              \"default\": null,\n              \"doc\": \"comment on column point.x\"\n            },\n            {\n              \"name\": \"y\",\n              \"type\": [\n                \"null\",\n                \"int\"\n              ],\n              \"default\": null\n            }\n          ]\n        }\n      ],\n      \"default\": null\n    },\n    {\n      \"name\": \"c2\",\n      \"type\": \"string\",\n      \"doc\": \"comment on t.c2\"\n    },\n    {\n      \"name\": \"c3_map_text__text_\",\n      \"type\": [\n        \"null\",\n        {\n          \"type\": \"map\",\n          \"values\": [\n            \"null\",\n            \"boolean\"\n          ]\n        }\n      ],\n      \"default\": null\n    },\n    {\n      \"name\": \"c4\",\n      \"type\": [\n        \"null\",\n        {\n          \"type\": \"array\",\n          \"items\": [\n            \"null\",\n            {\n              \"type\": \"record\",\n              \"name\": \"record1\",\n              \"namespace\": \"com.materialize.sink\",\n              \"doc\": \"comment on type point\",\n              \"fields\": [\n                {\n                  \"name\": \"x\",\n                  \"type\": [\n                    \"null\",\n                    \"int\"\n                  ],\n                  \"default\": null,\n                  \"doc\": \"comment on column point.x\"\n                },\n                {\n                  \"name\": \"y\",\n                  \"type\": [\n                    \"null\",\n                    \"int\"\n                  ],\n                  \"default\": null\n                }\n              ]\n            }\n          ]\n        }\n      ],\n      \"default\": null,\n      \"doc\": \"comment on column t.c4 with an äöü\"\n    }\n  ]\n}"

> EXPLAIN KEY SCHEMA FOR CREATE SINK sink1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink3-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS = TRUE,
    VALUE DOC ON TYPE t = 'body of the upsert',
    KEY DOC ON TYPE t = 'key of the upsert'
  )
  ENVELOPE UPSERT;
"{\n  \"type\": \"record\",\n  \"name\": \"row\",\n  \"doc\": \"key of the upsert\",\n  \"fields\": [\n    {\n      \"name\": \"c2\",\n      \"type\": \"string\",\n      \"doc\": \"comment on t.c2\"\n    }\n  ]\n}"

# Works without sink name
> EXPLAIN KEY SCHEMA FOR CREATE SINK
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink3-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS = TRUE,
    VALUE DOC ON TYPE t = 'body of the upsert',
    KEY DOC ON TYPE t = 'key of the upsert'
  )
  ENVELOPE UPSERT;
"{\n  \"type\": \"record\",\n  \"name\": \"row\",\n  \"doc\": \"key of the upsert\",\n  \"fields\": [\n    {\n      \"name\": \"c2\",\n      \"type\": \"string\",\n      \"doc\": \"comment on t.c2\"\n    }\n  ]\n}"

# errors
! CREATE SINK bad_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON COLUMN = 'comments'
  )
  ENVELOPE UPSERT
contains: Expected identifier, found equals sign

! CREATE SINK bad_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON COLUMN t.bad_column = 'comments'
  )
  ENVELOPE UPSERT
contains: column "t.bad_column" does not exist

# Fails if name is not provided
! CREATE SINK FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT
contains: unspecified name for sink

! CREATE SINK bad_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON TYPE bad_table_name = 'comments'
  )
  ENVELOPE UPSERT
contains: unknown catalog item 'bad_table_name'

! CREATE SINK bad_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON TYPE t
  )
  ENVELOPE UPSERT
contains: option value: cannot be empty

! CREATE SINK bad_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON COLUMN t = 'comments'
  )
  ENVELOPE UPSERT
contains: need to specify an object and a column

! CREATE SINK bad_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
   DOC ON COLUMN t.c1 = NULL
  )
  ENVELOPE UPSERT;
contains: cannot use value as string
