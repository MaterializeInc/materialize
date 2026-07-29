# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

define
DefSource name=x
  - c0: bigint?
  - c1: bigint?
----
Source defined as t0

# check that equivalences involving runtime constants can be pushed down

apply pipeline=predicate_pushdown
Join on=(#1 = mz_now())
  Get x
  Get x
----
CrossJoin
  Filter (#1 = mz_now())
    Get x
  Get x

apply pipeline=predicate_pushdown
Join on=(#1 = #3 = mz_now())
  Get x
  Get x
----
CrossJoin
  Filter (#1 = mz_now())
    Get x
  Filter (#1 = mz_now())
    Get x

# Join equivalence with several runtime constants

apply pipeline=predicate_pushdown
Join on=(#1 = 1 = mz_now())
  Get x
  Get x
----
CrossJoin
  Filter (#1 = 1) AND (#1 = mz_now())
    Get x
  Get x

# Check that equality filters with runtime constants don't get stuck in the join

apply pipeline=predicate_pushdown
Filter (#1 = mz_now())
  CrossJoin
    Get x
    Get x
----
CrossJoin
  Filter (#1) IS NOT NULL AND (#1 = mz_now())
    Get x
  Get x

apply pipeline=predicate_pushdown
Filter (mz_now() = #1)
  CrossJoin
    Get x
    Get x
----
CrossJoin
  Filter (#1) IS NOT NULL AND (#1 = mz_now())
    Get x
  Get x

# extract_equal_or_both_null

apply pipeline=predicate_pushdown
Filter ((((#0) IS NULL) AND ((#2) IS NULL)) OR (#0 = add_int64(#2, 1)))
  CrossJoin
    Get x
    Get x
----
Join on=(#0 = (#2 + 1))
  Get x
  Get x

apply pipeline=predicate_pushdown
Filter ((#0 = add_int64(#2, 1)) OR (((#0) IS NULL) AND ((add_int64(#2, 1)) IS NULL)))
  CrossJoin
    Get x
    Get x
----
Join on=(#0 = (#2 + 1))
  Get x
  Get x

apply pipeline=predicate_pushdown
Filter (and(((#0) IS NULL), (((#2) IS NULL) AND ((#0) IS NULL))) OR (#0 = #2))
  CrossJoin
    Get x
    Get x
----
Join on=(#0 = #2)
  Get x
  Get x

apply pipeline=predicate_pushdown
Filter (and(((#0) IS NULL), (((#2) IS NULL) AND ((#0) IS NULL))) OR (#0 = add_int64(#2, 1)))
  CrossJoin
    Get x
    Get x
----
Join on=(#0 = (#2 + 1))
  Get x
  Get x

# Push down filter predicates through FlatMap operators

apply pipeline=predicate_pushdown
Filter (#0 = #1)
  FlatMap generate_series_i32(#0)
    Get x
----
FlatMap generate_series(#0)
  Filter (#0 = #1)
    Get x

apply pipeline=predicate_pushdown
Filter (#0 = #2)
  FlatMap generate_series_i32(#0)
    Get x
----
Filter (#0 = #2)
  FlatMap generate_series(#0)
    Get x

apply pipeline=predicate_pushdown
Filter (#0 > #1) AND (#1 < #2)
  FlatMap generate_series_i32(#0)
    Get x
----
Filter (#1 < #2)
  FlatMap generate_series(#0)
    Filter (#0 > #1)
      Get x

apply pipeline=predicate_pushdown
Filter (#0 > #1)
  Threshold
    Union
      Get x
      Negate
        Filter (#0 < 7)
          Get x
----
Threshold
  Union
    Filter (#0 > #1)
      Get x
    Negate
      Filter (#0 < 7) AND (#0 > #1)
        Get x

apply pipeline=predicate_pushdown
Filter (#0 > 5)
  Reduce group_by=[#0] aggregates=[count(*)]
    Constant // { types: "(integer?, integer?)" }
      - (0, 1)
      - (0, 2)
----
Reduce group_by=[#0] aggregates=[count(*)]
  Filter (#0 > 5)
    Constant
      - (0, 1)
      - (0, 2)

apply pipeline=predicate_pushdown
Filter (#1 > 5)
  TopK group_by=[#1] order_by=[#1 desc nulls_first] limit=1
    Constant // { types: "(text?, integer?)" }
      - ("a", 1)
      - ("b", 2)
----
TopK group_by=[#1] order_by=[#1 desc nulls_first] limit=1
  Filter (#1 > 5)
    Constant
      - ("a", 1)
      - ("b", 2)
