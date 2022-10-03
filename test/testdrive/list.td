# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM mz_list_types;

> SHOW TYPES

> CREATE TYPE bool AS LIST (ELEMENT TYPE = int4)

> CREATE TYPE custom AS LIST (ELEMENT TYPE = bool)

$ set-regex match=^s\d*$ replacement=<GID>
# Without qualifiers, should default to builtin bool.
> SELECT element_id
  FROM mz_list_types JOIN mz_types ON mz_list_types.type_id = mz_types.id
  WHERE name = 'custom'
<GID>

> CREATE TYPE another_custom AS LIST (ELEMENT TYPE = public.bool)

$ set-regex match=^u\d*$ replacement=<GID>
# Qualified name should point to user-defined bool.
> SELECT element_id
  FROM mz_list_types JOIN mz_types ON mz_list_types.type_id = mz_types.id
  WHERE name = 'another_custom'
<GID>

> CREATE SCHEMA test_schema

> CREATE TYPE test_schema.bool AS LIST (ELEMENT TYPE = float4)

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

! CREATE TABLE f1 (a char list);
contains:char list not yet supported

! CREATE TYPE test_schema.bool AS LIST (ELEMENT TYPE = char)
contains:embedding char type in a list or map not yet supported
