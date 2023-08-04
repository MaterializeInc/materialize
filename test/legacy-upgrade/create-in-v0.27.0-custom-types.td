# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# CREATE various custom types and use them in views
#

> DROP TYPE IF EXISTS int4_list_list;

> DROP TYPE IF EXISTS int4_list;

> CREATE TYPE int4_list AS LIST (ELEMENT TYPE = int4);

> CREATE MATERIALIZED VIEW int4_list_view AS SELECT '{1,2}'::int4_list AS custom_list;

> CREATE TYPE int4_list_list AS LIST (ELEMENT TYPE = int4_list);

> CREATE MATERIALIZED VIEW int4_list_list_view AS SELECT '{{1,2}}'::int4_list_list AS custom_nested_list;

> DROP TYPE IF EXISTS int4_map_map;

> DROP TYPE IF EXISTS int4_map;

> CREATE TYPE int4_map AS MAP (KEY TYPE = text, VALUE TYPE = int4);

> CREATE MATERIALIZED VIEW int4_map_view AS SELECT '{a=>1}'::int4_map AS custom_map;

> CREATE TYPE int4_map_map AS MAP (KEY TYPE = text, VALUE TYPE = int4_map);

> CREATE MATERIALIZED VIEW int4_map_map_view AS SELECT '{a=>{"a a"=>1}}'::int4_map_map AS custom_nested_map;
