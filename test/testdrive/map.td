# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM mz_map_types;

> SHOW TYPES

> CREATE TYPE bool AS MAP (key_type=text, value_type=int4)

> CREATE TYPE custom AS MAP (key_type=text, value_type=bool)

# Without qualifiers, should default to builtin bool.
> SELECT mz_internal.mz_classify_object_id(value_id)
  FROM mz_map_types JOIN mz_types ON mz_map_types.type_id = mz_types.id
  WHERE name = 'custom'
system

> CREATE TYPE another_custom AS MAP (key_type=text, value_type=public.bool)

# Qualified name should point to user-defined bool.
> SELECT mz_internal.mz_classify_object_id(value_id)
  FROM mz_map_types JOIN mz_types ON mz_map_types.type_id = mz_types.id
  WHERE name = 'another_custom'
user

> CREATE SCHEMA test_schema

> CREATE TYPE test_schema.bool AS MAP (key_type=text, value_type=float4)

> SHOW TYPES
name
--------------
bool
custom
another_custom

> SHOW TYPES FROM test_schema
name
----
bool

! DROP TYPE bool
contains:cannot drop item pg_catalog.bool because it is required by the database system

! DROP TYPE public.bool
contains:cannot drop materialize.public.bool: still depended upon by catalog item 'materialize.public.another_custom'

> DROP TYPE another_custom

> DROP TYPE public.bool
