# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# If mz_persist::gen::ProtoMeta::ENCODING_VERSION is changed, the version of this file needs to change to "current_source"

> CREATE TABLE persistent_user_table (f1 INTEGER, f2 STRING);

> INSERT INTO persistent_user_table VALUES (1234, REPEAT('x', 1024 * 1024 * 10));

#
# Views over said table
#

> CREATE VIEW persistent_user_table_view1 AS SELECT f1 * 10 FROM persistent_user_table;

> CREATE MATERIALIZED VIEW persistent_user_table_view2 AS SELECT f1 * 100 FROM persistent_user_table;

#
# A table in some other schema
#

> DROP SCHEMA IF EXISTS persistent_schema;

> CREATE SCHEMA persistent_schema;

> CREATE TABLE persistent_schema.persistent_user_table (f1 TEXT);

> INSERT INTO persistent_schema.persistent_user_table VALUES ('abc');

#
# Multi-row insert, multi-insert transactions
#


> CREATE TABLE persistent_user_table_txns (f1 INTEGER);

> INSERT INTO persistent_user_table_txns VALUES (1),(2),(3),(4),(5);

> BEGIN;

> INSERT INTO persistent_user_table_txns VALUES (11),(12),(13),(14),(15);

> INSERT INTO persistent_user_table_txns VALUES (21),(22),(23),(24),(25);

> COMMIT;

#
# Rename a table after persisting
#

> CREATE TABLE persistent_user_table_rename (f1 INTEGER);

> INSERT INTO persistent_user_table_rename VALUES (1234);

> ALTER TABLE persistent_user_table_rename RENAME TO persistent_user_table_renamed;

#
# A sink based on a persistent table
#

> CREATE TABLE persistent_user_table1_for_sink (f1 INTEGER);

> CREATE TABLE persistent_user_table2_for_sink (f1 INTEGER);

> BEGIN

> INSERT INTO persistent_user_table1_for_sink VALUES (1);

> INSERT INTO persistent_user_table2_for_sink VALUES (1);

> COMMIT

> CREATE SINK persistent_user_table1_sink FROM persistent_user_table1_for_sink
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'persistent-user-table1-sink-${testdrive.seed}'
  WITH (consistency_topic = 'persistent-user-table1-sink-consistency')
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}';

> CREATE SINK persistent_user_table2_sink FROM persistent_user_table2_for_sink
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'persistent-user-table2-sink-${testdrive.seed}'
  WITH (consistency_topic = 'persistent-user-table2-sink-consistency')
  FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}';

> BEGIN

> INSERT INTO persistent_user_table1_for_sink VALUES (2);

> INSERT INTO persistent_user_table2_for_sink VALUES (2);

> COMMIT

> SELECT mz_internal.mz_sleep(5);
<null>
