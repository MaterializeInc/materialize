# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test exercises solely to test arrays at the boundary (e.g., by sending
# them through pgwire).

> SELECT ARRAY[ARRAY['a', 'b'], ARRAY[NULL, 'd']]
array
----
{{a,b},{NULL,d}}

> SELECT ARRAY[ARRAY['a', 'b'], ARRAY[NULL, 'd']]::text
array
----
{{a,b},{NULL,d}}

! CREATE TABLE foobar (f1 int list[]);
integer list[] not yet supported

! SELECT ARRAY[LIST[1]];
integer list[] not yet supported

> CREATE TYPE bigint_list AS LIST (element_type=bigint);
! CREATE TABLE bigint_list_arr (f1 bigint_list[]);
bigint_list[] not yet supported

! SELECT ARRAY['{a => 1}'::map[text=>int]];
map[text=>integer][] not yet supported

> CREATE TYPE bigint_map AS MAP (key_type=text, value_type=bigint);
! CREATE TABLE bigint_map_map_array_table (f1 bigint_map[]);
bigint_map[] not yet supported
