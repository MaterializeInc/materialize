# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=((\d+)\sbytes|u(\d+)) replacement=<XXX>

> CREATE SCHEMA blue;
> CREATE SCHEMA green;

> SET SCHEMA = blue;
> CREATE TABLE t (x INT, y INT);
> CREATE INDEX t_idx ON t(x);

> SET SCHEMA = green;
> CREATE TABLE t (x INT, y INT);
> CREATE INDEX t_idx ON t(x);
> INSERT INTO t VALUES (0, 1), (0, 2), (1, 3), (2, 4);

# both work
> EXPLAIN ANALYZE MEMORY FOR INDEX materialize.green.t_idx;
operator                      total_memory   total_records
----------------------------------------------------------
"Arrange (#0{x})"             <XXX>          4
"  Stream <XXX>"              <null>         <null>

> EXPLAIN ANALYZE MEMORY FOR INDEX materialize.blue.t_idx;
operator                      total_memory   total_records
----------------------------------------------------------
"Arrange (#0{x})"             <XXX>          <null>
"  Stream <XXX>"              <null>         <null>

> ALTER SCHEMA blue SWAP WITH green;

> DROP SCHEMA green CASCADE;

# should still get output for blue!
> EXPLAIN ANALYZE MEMORY FOR INDEX materialize.blue.t_idx;
operator                      total_memory   total_records
----------------------------------------------------------
"Arrange (#0{x})"             <XXX>          4
"  Stream <XXX>"              <null>         <null>
