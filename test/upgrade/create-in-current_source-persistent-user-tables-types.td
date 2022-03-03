# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# If mz_persist::gen::ProtoMeta::ENCODING_VERSION is changed, the version of this file needs to change to "current_source"

# Persist data of various types

> CREATE TABLE persistence_timestamp_with_time_zone (f1 TIMESTAMP WITH TIME ZONE);

> INSERT INTO persistence_timestamp_with_time_zone VALUES ('2007-02-01 15:04:05.123456+06');

> CREATE TABLE persistence_nested_array (f1 text[][]);

> INSERT INTO persistence_nested_array VALUES (ARRAY[ARRAY['a', 'white space'], ARRAY[NULL, ''], ARRAY['escape"m\e', 'nUlL']]);

> CREATE TABLE persistence_decimal (f1 DECIMAL(32, 16));

> INSERT INTO persistence_decimal VALUES (1234567890123456.1234567890123456);

> CREATE TABLE persistence_double (f1 DOUBLE);

> INSERT INTO persistence_double VALUES (1234.5678);

#
# custom types
#

> DROP TYPE IF EXISTS persistence_bigint_list_list;

> DROP TYPE IF EXISTS persistence_bigint_list;

> CREATE TYPE persistence_bigint_list AS LIST (element_type = bigint);

> CREATE TABLE persistence_bigint_list_table (f1 persistence_bigint_list);

> INSERT INTO persistence_bigint_list_table VALUES ('{0,1}'::persistence_bigint_list);

> INSERT INTO persistence_bigint_list_table VALUES ('{9223372036854775807,-9223372036854775808}'::persistence_bigint_list);


> CREATE TYPE persistence_bigint_list_list AS LIST (element_type = persistence_bigint_list);

> CREATE TABLE persistence_bigint_list_list_table (f1 persistence_bigint_list_list);

> INSERT INTO persistence_bigint_list_list_table VALUES ('{{9223372036854775807,-9223372036854775808}}'::persistence_bigint_list_list);


> DROP TYPE IF EXISTS persistence_bigint_map_map;

> DROP TYPE IF EXISTS persistence_bigint_map;

> CREATE TYPE persistence_bigint_map AS MAP (key_type=text, value_type=bigint);

> CREATE TABLE persistence_bigint_map_table (f1 persistence_bigint_map);

> INSERT INTO persistence_bigint_map_table VALUES ('{abcdef=>9223372036854775807}'::persistence_bigint_map);


> CREATE TYPE persistence_bigint_map_map AS MAP (key_type=text, value_type=persistence_bigint_map);

> CREATE TABLE persistence_bigint_map_map_table (f1 persistence_bigint_map_map);

> INSERT INTO persistence_bigint_map_map_table VALUES ('{abcdef=>{"abc def"=>-9223372036854775808}}'::persistence_bigint_map_map);

# This panics, see https://github.com/MaterializeInc/materialize/issues/8672
# > CREATE TABLE persistence_bigint_map_map_array_table (f1 persistence_bigint_map_map[][]);
# > INSERT INTO persistence_bigint_map_map_array_table VALUES (ARRAY[ARRAY['{ab=>{"cd"=>123}}'::persistence_bigint_map_map, '{ab=>{"cd"=>123}}'::persistence_bigint_map_map], ARRAY['{ab=>{"cd"=>123}}'::persistence_bigint_map_map, '{ab=>{"cd"=>123}}'::persistence_bigint_map_map]]);
