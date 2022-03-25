# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# The simplest smoke test
#

> CREATE TABLE t1 (f1 TIMESTAMP, f2 BYTEA);

> INSERT INTO t1 VALUES (NULL, NULL);

> INSERT INTO t1 VALUES ('2011-11-11 11:11:11', decode('1234', 'hex'));

> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;

#
# Exotic types
#

> CREATE TYPE int4_list AS LIST (element_type = int4);

> CREATE TYPE int4_list_list AS LIST (element_type = int4_list);

> CREATE TYPE int4_map AS MAP (key_type=text, value_type=int4);

> CREATE TYPE int4_map_map AS MAP (key_type=text, value_type=int4_map);

> CREATE TABLE exotic_types (int4_list int4_list, int4_list_list int4_list_list, int4_map int4_map, int4_map_map int4_map_map);

> INSERT INTO exotic_types VALUES ('{1,2}'::int4_list, '{{1,2}}'::int4_list_list, '{a=>1}'::int4_map, '{a=>{a=>1}}'::int4_map_map);

> CREATE TABLE char_type_quoted (f1 "char");

> INSERT INTO char_type_quoted VALUES ('a');

#
# Make sure dropping a table does not break persistence of other tables
#

> CREATE TABLE to_be_dropped (f1 INTEGER);

> INSERT INTO to_be_dropped VALUES (1),(2),(3);

> DROP TABLE to_be_dropped;

#
# Make sure persisting the same table in different schema does not cause interference
#

> CREATE SCHEMA schema1;

> CREATE SCHEMA schema2;

> CREATE TABLE schema1.t1 (f1 TEXT);

> INSERT INTO schema1.t1 VALUES ('schema1');

> CREATE TABLE schema2.t1 (f1 TEXT);

> INSERT INTO schema2.t1 VALUES ('schema2');

#
# A table that has been dropped and recreated should not have its old data
#

> CREATE TABLE to_be_recreated (f1 INTEGER);

> INSERT INTO to_be_recreated VALUES (1);

> DROP TABLE to_be_recreated;

> CREATE TABLE to_be_recreated (f1 INTEGER);

> INSERT INTO to_be_recreated VALUES (2);

#
# A table that has been renamed should remain readable
#

> CREATE TABLE to_be_renamed (f1 INTEGER);

> INSERT INTO to_be_renamed VALUES (1);

> ALTER TABLE to_be_renamed RENAME TO already_renamed;

> INSERT INTO already_renamed VALUES (2);
