# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test Avro UPSERT sinks doc comments

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_comment = true;

> CREATE TYPE point AS (x integer, y integer);
> CREATE TYPE custom_map AS MAP (KEY TYPE = text, VALUE TYPE = bool)
> CREATE TABLE t (c1 point, c2 text NOT NULL, c3 custom_map, c4 point list);
> INSERT INTO t SELECT ROW(1, 1)::point AS c1, 'text' AS c2, '{a=>true}'::custom_map as c3, LIST[ROW(1, 1)::point] as c4;

> COMMENT ON TABLE t IS 'comment on table t';
> COMMENT ON COLUMN t.c3 IS 'comment on column t.c3';
> COMMENT ON COLUMN t.c4 IS 'comment on column t.c4';
> COMMENT ON TYPE point IS 'comment on type point';

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SINK sink1 FROM t
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
   KEY DOC ON TYPE t = 'key doc on t'
  )
  ENVELOPE UPSERT;

> SHOW CREATE SINK sink1;
name                            create_sql
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 "materialize.public.sink1"  "CREATE SINK \"materialize\".\"public\".\"sink1\" FROM \"materialize\".\"public\".\"t\" INTO KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" (TOPIC = 'testdrive-sink1-${testdrive.seed}') KEY (\"c2\") NOT ENFORCED FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION \"materialize\".\"public\".\"csr_conn\" (DOC ON COLUMN \"materialize\".\"public\".\"t\".c1 = 'doc on t.c1', VALUE DOC ON COLUMN \"materialize\".\"public\".\"t\".c2 = 'value doc on t.c2', KEY DOC ON COLUMN \"materialize\".\"public\".\"t\".c2 = 'key doc on t.c2', DOC ON COLUMN \"materialize\".\"public\".\"t\".c4 = 'doc on t.c4', KEY DOC ON TYPE \"materialize\".\"public\".\"point\" = 'key doc on point', VALUE DOC ON TYPE \"materialize\".\"public\".\"point\" = 'value doc on point', KEY DOC ON TYPE \"materialize\".\"public\".\"t\" = 'key doc on t', DOC ON TYPE \"materialize\".\"public\".\"point\" = 'comment on type point', DOC ON TYPE \"materialize\".\"public\".\"t\" = 'comment on table t', DOC ON COLUMN \"materialize\".\"public\".\"t\".c3 = 'comment on column t.c3') ENVELOPE UPSERT"

$ schema-registry-verify schema-type=avro subject=testdrive-sink1-${testdrive.seed}-value
{"type":"record","name":"envelope","doc":"comment on table t","fields":[{"name":"c1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","doc":"value doc on point","fields":[{"name":"x","type":["null","int"]},{"name":"y","type":["null","int"]}]}],"doc":"doc on t.c1"},{"name":"c2","type":"string","doc":"value doc on t.c2"},{"name":"c3","type":["null",{"type":"map","values":["null","boolean"]}],"doc":"comment on column t.c3"},{"name":"c4","type":["null",{"type":"array","items":["null",{"type":"record","name":"record1","namespace":"com.materialize.sink","doc":"value doc on point","fields":[{"name":"x","type":["null","int"]},{"name":"y","type":["null","int"]}]}]}],"doc":"doc on t.c4"}]}

$ schema-registry-verify schema-type=avro subject=testdrive-sink1-${testdrive.seed}-key
{"type":"record","name":"row","doc":"key doc on t","fields":[{"name":"c2","type":"string","doc":"key doc on t.c2"}]}

> CREATE SINK sink2 FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink2-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

$ schema-registry-verify schema-type=avro subject=testdrive-sink2-${testdrive.seed}-value
{"type":"record","name":"envelope","doc":"comment on table t","fields":[{"name":"c1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","doc":"comment on type point","fields":[{"name":"x","type":["null","int"]},{"name":"y","type":["null","int"]}]}]},{"name":"c2","type":"string"},{"name":"c3","type":["null",{"type":"map","values":["null","boolean"]}],"doc":"comment on column t.c3"},{"name":"c4","type":["null",{"type":"array","items":["null",{"type":"record","name":"record1","namespace":"com.materialize.sink","doc":"comment on type point","fields":[{"name":"x","type":["null","int"]},{"name":"y","type":["null","int"]}]}]}],"doc":"comment on column t.c4"}]}

# errors
! CREATE SINK bad_sink FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON COLUMN = 'comments'
  )
  ENVELOPE UPSERT
contains: Expected identifier, found equals sign

! CREATE SINK bad_sink FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON COLUMN t.bad_column = 'comments'
  )
  ENVELOPE UPSERT
contains: column "t.bad_column" does not exist

! CREATE SINK bad_sink FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON TYPE bad_table_name = 'comments'
  )
  ENVELOPE UPSERT
contains: unknown catalog item 'bad_table_name'

! CREATE SINK bad_sink FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON TYPE t
  )
  ENVELOPE UPSERT
contains: option value: cannot be empty

! CREATE SINK bad_sink FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    DOC ON COLUMN t = 'comments'
  )
  ENVELOPE UPSERT
contains: need to specify an object and a column
