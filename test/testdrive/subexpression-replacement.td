# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Tests for the optimization described in https://github.com/MaterializeInc/materialize/pull/6196/
# Additional tests in test/sqllogictest/transform/predicate_reduction.slt
#

$ set-sql-timeout duration=125ms

# Remove references to internal table identifiers and "materialize.public" strings
$ set-regex match=(\s\(u\d+\)|materialize\.public\.) replacement=

> CREATE TABLE t1 (col_null INTEGER, col_not_null INTEGER NOT NULL);
> CREATE DEFAULT INDEX on t1

> INSERT INTO t1 VALUES (1, 1);

# The simplest expression there could be

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT * FROM t1 WHERE col_null IS NULL AND (col_null IS NULL AND col_not_null = 5);
Optimized Plan
----
"Explained Query (fast path):
  Filter (#0{t1.col_null}) IS NULL AND (#1{t1.col_not_null} = 5)
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=t1 t1_primary_idx=[lookup value=(5, 1)]

Used Indexes:
  - t1_primary_idx (lookup)

Target cluster: quickstart

# NULL-able expressions are dedupped
? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT * FROM t1 WHERE col_null = 1 AND (col_null = 1 AND col_not_null = 5);
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=t1 t1_primary_idx=[lookup value=(1, 5)]

Used Indexes:
  - t1_primary_idx (lookup)

Target cluster: quickstart

# OR/disjunction at the top level

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT * FROM t1 WHERE col_null IS NULL OR (col_null IS NULL AND col_not_null = 5);
Optimized Plan
----
"Explained Query (fast path):
  Filter (#0{t1.col_null}) IS NULL
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter (#0{t1.col_null}) IS NULL
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter (#0{t1.col_null}) IS NULL
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter (#0{t1.col_null}) IS NULL
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter ((#0{t1.col_null}) IS NULL OR (#1{t1.col_not_null} = 5))
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter (#0{t1.col_null} = 6) AND (5 = (#1{t1.col_not_null} + (1 / #1{t1.col_not_null})))
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter (5 = ((#1{t1.col_not_null} + #1{t1.col_not_null}) + #1{t1.col_not_null}))
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter (#1{t1.col_not_null} < 5) AND (#1{t1.col_not_null} > 3)
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter ((#1{t1.col_not_null} < 5) OR (#1{t1.col_not_null} > 3))
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Explained Query (fast path):
  Constant <empty>

Target cluster: quickstart

# This is not optimized
? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT * FROM t1 WHERE (col_not_null % 2) = 1 AND (((col_not_null % 2) = 1) = TRUE);
Optimized Plan
----
"Explained Query (fast path):
  Filter (1 = (#1{t1.col_not_null} % 2))
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter (#1{t1.col_not_null} = (#1{t1.col_not_null} + 1))
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Project (#0, #1)
    Filter (#2 > #2)
      Map (mz_sleep(integer_to_double(#1{t1.col_not_null})))
        ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter ((#1{t1.col_not_null} = 2) OR (#1{t1.col_not_null} = 3))
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Filter (#1{t1.col_not_null} = 3)
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Explained Query (fast path):
  Constant <empty>

Target cluster: quickstart

# Expression inside an IN list

# Optimized in AND/conjunctions

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT * FROM t1 WHERE col_not_null = 1 AND TRUE IN (col_not_null = 1, col_not_null = 2);
Optimized Plan
----
"Explained Query (fast path):
  Filter (#1{t1.col_not_null} = 1)
    ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
Optimized Plan
----
"Explained Query (fast path):
  Project (#0, #1)
    Filter (#2 OR (#2 = true) OR (true = (#1{t1.col_not_null} = 2)))
      Map ((#1{t1.col_not_null} = 1))
        ReadIndex on=t1 t1_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_primary_idx (*** full scan ***)

Target cluster: quickstart
"
