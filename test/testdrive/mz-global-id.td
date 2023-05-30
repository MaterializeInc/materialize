# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# We want stability of GlobalIDs
reset-server

> CREATE DATABASE d;
> CREATE SCHEMA s;
> CREATE SCHEMA d.s;

> CREATE SOURCE counter
  FROM LOAD GENERATOR COUNTER (TICK INTERVAL '1s');

> CREATE SOURCE s.counter
  FROM LOAD GENERATOR COUNTER (TICK INTERVAL '1s');

> CREATE SOURCE d.s.counter
  FROM LOAD GENERATOR COUNTER (TICK INTERVAL '1s');

> SELECT mz_internal.mz_name_to_global_id('counter');
u2

> SELECT mz_internal.mz_name_to_global_id('s', 'counter');
u4

> SELECT mz_internal.mz_name_to_global_id('d', ARRAY['s'], 'counter');
u6

> SELECT mz_internal.mz_name_to_global_id(ARRAY['public', 's'], 'counter');
u2
u4

# Values that do not exist
> SELECT mz_internal.mz_name_to_global_id('dne')

> SELECT mz_internal.mz_name_to_global_id('dne', 'counter')

> SELECT mz_internal.mz_name_to_global_id(ARRAY[]::text[], 'counter')

> SELECT mz_internal.mz_name_to_global_id('public', 'dne')

> SELECT mz_internal.mz_name_to_global_id('dne', ARRAY['public'], 'counter')

> SELECT mz_internal.mz_name_to_global_id('d', ARRAY[]::text[], 'counter');

# Null input
> SELECT mz_internal.mz_name_to_global_id(null)

> SELECT mz_internal.mz_name_to_global_id(null, 'counter')

> SELECT mz_internal.mz_name_to_global_id('public', null)

> SELECT mz_internal.mz_name_to_global_id(null, ARRAY['public'], 'counter')

# Null input is valid for database when looking up items in ambient mz_schemas
> SELECT mz_internal.mz_name_to_global_id(null, ARRAY['pg_catalog'], 'int4')
s20
