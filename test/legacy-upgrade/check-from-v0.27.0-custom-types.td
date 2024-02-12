# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# List

> SELECT custom_list::text FROM int4_list_view;
{1,2}

> CREATE MATERIALIZED VIEW int4_list_view_after AS SELECT '{1,2}'::int4_list AS custom_list;

> SELECT custom_list::text FROM int4_list_view_after;
{1,2}

> DROP MATERIALIZED VIEW int4_list_view_after;

# Nested list

> SELECT custom_nested_list::text FROM int4_list_list_view;
{{1,2}}

> CREATE MATERIALIZED VIEW int4_list_list_view_after AS SELECT '{{1,2}}'::int4_list_list AS custom_nested_list;

> SELECT custom_nested_list::text FROM int4_list_list_view_after;
{{1,2}}

> DROP MATERIALIZED VIEW int4_list_list_view_after;

# Map

> SELECT custom_map::text FROM int4_map_view;
{a=>1}

> CREATE MATERIALIZED VIEW int4_map_view_after AS SELECT '{a=>1}'::int4_map AS custom_map;

> SELECT custom_map::text FROM int4_map_view_after;
{a=>1}

> DROP MATERIALIZED VIEW int4_map_view_after;

# Nested map

> SELECT custom_nested_map::text FROM int4_map_map_view;
"{a=>{\"a a\"=>1}}"

> CREATE MATERIALIZED VIEW int4_map_map_view_after AS SELECT '{a=>{"a a"=>1}}'::int4_map_map AS custom_nested_map;

> SELECT custom_nested_map::text FROM int4_map_map_view_after;
"{a=>{\"a a\"=>1}}"

> DROP MATERIALIZED VIEW int4_map_map_view_after;
