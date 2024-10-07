# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s

# Test custom search_paths
> CREATE SCHEMA test1
> CREATE SCHEMA test2

> SET search_path = test1

> CREATE TABLE test1.test_table (a int)
> INSERT INTO test_table VALUES (1)

> SET search_path = test2

# test_table should not be resolvable in test2
! SELECT * FROM test_table
contains:unknown catalog item 'test_table'

# but still accessible with fully qualified names
> SELECT * FROM test1.test_table
1

# pg_catalog objects are always resolvable
> SELECT * FROM pg_am

# The temporary schema has priority over regular schemas
> SET search_path = test1

> CREATE TEMP TABLE test_table (a int)

> SELECT * FROM test_table
# No results returned

> SELECT * FROM test1.test_table
1

> INSERT INTO test_table VALUES (-1)

> SELECT * FROM test_table
-1

> SELECT * FROM test1.test_table
1
