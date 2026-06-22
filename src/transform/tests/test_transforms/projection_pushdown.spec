# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Source definitions
# ------------------

# Define x source
define
DefSource name=x
  - c0: bigint
  - c1: bigint
  - c3: bigint
----
Source defined as t0

# Define y source
define
DefSource name=y
  - c0: bigint
  - c1: bigint
  - c3: bigint
----
Source defined as t1

# Define xi source (32-bit columns, for the int32 `generate_series` variant)
define
DefSource name=xi
  - c0: integer
  - c1: integer
  - c3: integer
----
Source defined as t2


# Absorb project in a constant
apply pipeline=projection_pushdown
Project (#2, #1, #0)
  Constant // { types: "(bigint, bigint, bigint)" }
    - (1, 3, 4)
    - (2, 5, 6)
----
Project (#2, #1, #0)
  Constant
    - (1, 3, 4)
    - (2, 5, 6)

# Project around a project
apply pipeline=projection_pushdown
Project (#2, #1, #1, #0)
  Project (#0, #2, #1)
    Map (7)
      Constant // { types: "(bigint, bigint, bigint)" }
        - (1, 3, 4)
        - (2, 5, 6)
----
Project (#2, #1, #1, #0)
  Project (#0, #2, #1)
    Map ()
      Constant
        - (1, 3, 4)
        - (2, 5, 6)

# Project around a filter (1)
apply pipeline=projection_pushdown
Project (#2, #2)
  Filter (#0 = #2)
    Constant // { types: "(bigint, bigint, bigint)" }
      - (1, 3, 4)
      - (2, 5, 6)
----
Project (#0, #0)
  Project (#1)
    Filter (#0 = #1)
      Project (#0, #2)
        Constant
          - (1, 3, 4)
          - (2, 5, 6)

# Project around a filter (2)
apply pipeline=projection_pushdown
Project (#1)
  Filter #2 IS NULL
    Constant // { types: "(bigint, bigint, bigint)" }
      - (1, 3, 4)
      - (2, 5, 6)
----
Project (#0)
  Filter (#1) IS NULL
    Project (#1, #2)
      Constant
        - (1, 3, 4)
        - (2, 5, 6)

# Project around a map
apply pipeline=projection_pushdown
Project (#3)
  Map ((#1 + #0), 7)
    Constant // { types: "(bigint, bigint, bigint)" }
      - (1, 3, 4)
      - (2, 5, 6)
----
Project (#2)
  Map ((#1 + #0))
    Project (#0, #1)
      Constant
        - (1, 3, 4)
        - (2, 5, 6)

# Project around a column where a scalar refers to another fellow member of `scalars`
apply pipeline=projection_pushdown
Project (#3, #5, #5, #5, #3)
  Map ((#1 + #0), 7, (#4 + 7))
    Constant // { types: "(bigint, bigint, bigint)" }
      - (1, 3, 4)
      - (2, 5, 6)
----
Project (#0, #1, #1, #1, #0)
  Project (#2, #4)
    Map ((#1 + #0), 7, (#3 + 7))
      Project (#0, #1)
        Constant
          - (1, 3, 4)
          - (2, 5, 6)

# Projection pushdown causes elimination of unnecessary map scalars (1)
apply pipeline=projection_pushdown
Project (#3)
  Filter (#2 >= #1)
    Map ((#1 + #2), 7)
      Constant // { types: "(bigint, bigint, bigint)" }
        - (1, 3, 4)
        - (2, 5, 6)
----
Project (#2)
  Filter (#1 >= #0)
    Map ((#0 + #1))
      Project (#1, #2)
        Constant
          - (1, 3, 4)
          - (2, 5, 6)

# Projection pushdown causes elimination of unnecessary map scalars (2)
apply pipeline=projection_pushdown
Project (#2, #1, #0)
  Map ("dummy")
    Reduce group_by=[#0] aggregates=[sum(#1)]
      Constant // { types: "(bigint, bigint, bigint)" }
        - (1, 3, 4)
        - (2, 5, 6)
----
Project (#2, #1, #0)
  Map ("dummy")
    Reduce group_by=[#0] aggregates=[sum(#1)]
      Project (#0, #1)
        Constant
          - (1, 3, 4)
          - (2, 5, 6)

# Project around a join
apply pipeline=projection_pushdown
Project (#3)
  Join on=(#0 = #5)
    Filter (#2 >= #1)
      Map ((#1 + #2), 7)
        Constant // { types: "(bigint, bigint, bigint)" }
          - (1, 3, 4)
          - (2, 5, 6)
    Project (#0)
      Get x
----
Project (#1)
  Join on=(#0 = #2)
    Project (#0, #3)
      Filter (#2 >= #1)
        Map ((#1 + #2))
          Constant
            - (1, 3, 4)
            - (2, 5, 6)
    Project (#0)
      Get x

# An unused int64 `generate_series` with a unit step collapses to a
# `repeat_row_non_negative` of the same cardinality, in cheap `i64` arithmetic.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(#0, #2, 1)
    Get x
----
Project (#1)
  FlatMap repeat_row_non_negative(case when (#2 >= #0) then (((#2 - #0) / 1) + 1) else 0 end)
    Get x

# Query using the column newly created by FlatMap: no collapse.
apply pipeline=projection_pushdown
Project (#3)
  FlatMap generate_series(#0, #2, 1)
    Get x
----
Project (#2)
  FlatMap generate_series(#0, #1, 1)
    Project (#0, #2)
      Get x

# A non-unit positive step collapses too, but in `numeric`: a feasible series
# can have a span too wide for `i64`, so the synthesized arithmetic must not.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(#0, #2, 3)
    Get x
----
Project (#1)
  FlatMap repeat_row_non_negative(case when (#2 >= #0) then numeric_to_bigint((floornumeric(((bigint_to_numeric(#2) - bigint_to_numeric(#0)) / bigint_to_numeric(3))) + bigint_to_numeric(1))) else 0 end)
    Get x

# A negative step flips the emptiness guard to `<=`; also `numeric`.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(#0, #2, -2)
    Get x
----
Project (#1)
  FlatMap repeat_row_non_negative(case when (#2 <= #0) then numeric_to_bigint((floornumeric(((bigint_to_numeric(#2) - bigint_to_numeric(#0)) / bigint_to_numeric(-2))) + bigint_to_numeric(1))) else 0 end)
    Get x

# A non-literal step is left alone (we can't specialize on its sign).
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(#0, #2, #1)
    Get x
----
Project (#1)
  FlatMap generate_series(#0, #2, #1)
    Get x

# The int32 variant widens its `i64`-step bounds with `integer_to_bigint`.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series_i32(#0, #2, 1)
    Get xi
----
Project (#1)
  FlatMap repeat_row_non_negative(case when (#2 >= #0) then (((integer_to_bigint(#2) - integer_to_bigint(#0)) / 1) + 1) else 0 end)
    Get xi

# The int32 variant widens its `numeric`-step bounds with `integer_to_numeric`.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series_i32(#0, #2, 2)
    Get xi
----
Project (#1)
  FlatMap repeat_row_non_negative(case when (#2 >= #0) then numeric_to_bigint((floornumeric(((integer_to_numeric(#2) - integer_to_numeric(#0)) / bigint_to_numeric(2))) + bigint_to_numeric(1))) else 0 end)
    Get xi

# All-literal arguments fold to an exact count at rewrite time:
# (11 - 2) / 3 + 1 = 4.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(2, 11, 3)
    Get x
----
FlatMap repeat_row_non_negative(4)
  Project (#1)
    Get x

# An all-literal empty series folds to a count of zero.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(11, 2, 3)
    Get x
----
FlatMap repeat_row_non_negative(0)
  Project (#1)
    Get x

# An all-literal descending series folds exactly: (2 - 11) / -3 + 1 = 4.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(11, 2, -3)
    Get x
----
FlatMap repeat_row_non_negative(4)
  Project (#1)
    Get x

# A literal span too wide for `i64` still folds when the count fits:
# 10^19 / 10^18 + 1 = 11.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(-5000000000000000000, 5000000000000000000, 1000000000000000000)
    Get x
----
FlatMap repeat_row_non_negative(11)
  Project (#1)
    Get x

# A literal count beyond `i64` declines to collapse: the original enumerates it
# without error, so the rewrite must not introduce one.
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(-5000000000000000000, 5000000000000000000, 1)
    Get x
----
Project (#0)
  FlatMap generate_series(-5000000000000000000, 5000000000000000000, 1)
    Project (#1)
      Get x


# Project around a union (1)
apply pipeline=projection_pushdown
Project (#1, #0)
  Union
    Get x
    Get y
----
Union
  Project (#1, #0)
    Get x
  Project (#1, #0)
    Get y

# Project around a union (2)
apply pipeline=projection_pushdown
Project (#1, #1)
  Union
    Get x
    Get y
----
Project (#0, #0)
  Union
    Project (#1)
      Get x
    Project (#1)
      Get y

# Project around a negate
apply pipeline=projection_pushdown
Project (#0, #2)
  Union
    Get x
    Negate
      Filter (#1 = 1)
        Get x
----
Union
  Project (#0, #2)
    Get x
  Negate
    Project (#0, #2)
      Filter (#1 = 1)
        Get x

# Project around an ArrangeBy (1) - barrier!
apply pipeline=projection_pushdown
Project (#2)
  ArrangeBy keys=[[#0], [#1]]
    Get x
----
Project (#2)
  ArrangeBy keys=[[#0], [#1]]
    Get x

# Project around an ArrangeBy (2) - barrier!
apply pipeline=projection_pushdown
Project (#1)
  ArrangeBy keys=[[#0], [#1]]
    Get x
----
Project (#1)
  ArrangeBy keys=[[#0], [#1]]
    Get x

# Project around an ArrangeBy (3) - barrier!
apply pipeline=projection_pushdown
Project (#1, #0)
  ArrangeBy keys=[[#0], [#1]]
    Get x
----
Project (#1, #0)
  ArrangeBy keys=[[#0], [#1]]
    Get x

# Project around a Reduce (1)
apply pipeline=projection_pushdown
Project ()
  Distinct project=[(#0 + #2)]
    Get x
----
Project ()
  Distinct project=[(#0 + #1)]
    Project (#0, #2)
      Get x

# Project around a Reduce (2)
apply pipeline=projection_pushdown
Project (#1)
  Reduce group_by=[#0] aggregates=[sum((#0 * #2))]
    Get x
----
Project (#1)
  Reduce group_by=[#0] aggregates=[sum((#0 * #1))]
    Project (#0, #2)
      Get x

# Project around a Reduce (3)
apply pipeline=projection_pushdown
Project (#1, #0)
  Reduce group_by=[#0] aggregates=[sum((#0 * #2))]
    Get x
----
Project (#1, #0)
  Reduce group_by=[#0] aggregates=[sum((#0 * #1))]
    Project (#0, #2)
      Get x

# Project around a TopK (1)
apply pipeline=projection_pushdown
Project (#2, #2, #2)
  TopK group_by=[#0] order_by=[#1 asc nulls_first, #2 asc nulls_first] limit=(#0 + 4)
    Get x
----
Project (#0, #0, #0)
  Project (#2)
    TopK group_by=[#0] order_by=[#1 asc nulls_first, #2 asc nulls_first] limit=(#0 + 4)
      Get x

# Project around a TopK (2)
apply pipeline=projection_pushdown
Project (#2, #2)
  TopK order_by=[#1 asc nulls_first]
    Get x
----
Project (#0, #0)
  Project (#1)
    TopK order_by=[#0 asc nulls_first]
      Project (#1, #2)
        Get x

# Project around a TopK (3)
apply pipeline=projection_pushdown
Project (#2, #1)
  TopK group_by=[#2] order_by=[#1 asc nulls_first] limit=(#2 + 4)
    Get x
----
TopK group_by=[#0] order_by=[#1 asc nulls_first] limit=(#0 + 4)
  Project (#2, #1)
    Get x

# Project around a Let (1)
apply pipeline=projection_pushdown
Return
  Project (#5)
    Join on=(#0 = #8)
      Get l0
      Get l0
With
  cte l0 =
    Join on=(#0 = #3)
      Get x
      Get y
----
With
  cte l0 =
    Project (#0, #1, #3)
      Join on=(#0 = #2)
        Project (#0, #2)
          Get x
        Project (#0, #2)
          Get y
Return
  Project (#1)
    Join on=(#0 = #2)
      Project (#0, #2)
        Get l0
      Project (#1)
        Get l0

# Project around a Let (2)
apply pipeline=projection_pushdown
Return
  Project (#2)
    Join on=(#0 = #8)
      Get l0
      Get l0
With
  cte l0 =
    Join on=(#0 = #3)
      Get x
      Get y
----
With
  cte l0 =
    Project (#0, #1)
      Join on=(#0 = #2)
        Project (#0, #2)
          Get x
        Project (#0)
          Get y
Return
  Project (#1)
    Join on=(#0 = #2)
      Get l0
      Project (#1)
        Get l0

# Project around a Let (3)
apply pipeline=projection_pushdown
Return
  Union
    Project (#0, #4, #5, #6)
      Join on=(#0 = #4)
        Get l0
        Get y
    Project (#0, #1, #0, #3)
      Get l0
With
  cte l0 =
    Map (1)
      Get x
----
With
  cte l0 =
    Map (1)
      Project (#0, #1)
        Get x
Return
  Union
    Join on=(#0 = #1)
      Project (#0)
        Get l0
      Get y
    Project (#0, #1, #0, #2)
      Get l0


## LetRec cases
## ------------


# Project around a LetRec (1)
apply pipeline=projection_pushdown
Return
  Project (#5)
    Join on=(#0 = #8)
      Get l0
      Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint, bigint, bigint, bigint, bigint)" }
    Join on=(#0 = #3)
      Get x
      Get y
----
With Mutually Recursive
  cte l0 =
    Project (#0, #1, #3)
      Join on=(#0 = #2)
        Project (#0, #2)
          Get x
        Project (#0, #2)
          Get y
Return
  Project (#1)
    Join on=(#0 = #2)
      Project (#0, #2)
        Get l0
      Project (#1)
        Get l0

# Project around a LetRec (2)
apply pipeline=projection_pushdown
Return
  Project (#2)
    Join on=(#0 = #8)
      Get l0
      Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint, bigint, bigint, bigint, bigint)" }
    Join on=(#0 = #3)
      Get x
      Get y
----
With Mutually Recursive
  cte l0 =
    Project (#0, #1)
      Join on=(#0 = #2)
        Project (#0, #2)
          Get x
        Project (#0)
          Get y
Return
  Project (#1)
    Join on=(#0 = #2)
      Get l0
      Project (#1)
        Get l0

# Project around a LetRec (3)
apply pipeline=projection_pushdown
Return
  Union
    Project (#0, #4, #5, #6)
      Join on=(#0 = #4)
        Get l0
        Get y
    Project (#0, #1, #0, #3)
      Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint, bigint, bigint)" }
    Map (1)
      Get x
----
With Mutually Recursive
  cte l0 =
    Map (1)
      Project (#0, #1)
        Get x
Return
  Union
    Join on=(#0 = #1)
      Project (#0)
        Get l0
      Get y
    Project (#0, #1, #0, #2)
      Get l0


# Three bindings, l0 and l2 are not recursive
apply pipeline=projection_pushdown
Return
  Project (#0, #3)
    Join on=(#0 = #3)
      Get l2
      Get l1
With Mutually Recursive
  cte l2 = // { types: "(bigint, bigint, bigint)" }
    Get l0
  cte l1 = // { types: "(bigint, bigint, bigint)" }
    Project (#0, #0, #0)
      Distinct project=[#0]
        Union
          Project (#0)
            Get l1
          Project (#0)
            Get x
  cte l0 = // { types: "(bigint, bigint, bigint)" }
    Get x
----
With Mutually Recursive
  cte l0 =
    Project (#0)
      Get x
  cte l1 =
    Project (#0, #0, #0)
      Distinct project=[#0]
        Union
          Project (#0)
            Get l1
          Project (#0)
            Get x
  cte l2 =
    Get l0
Return
  Join on=(#0 = #1)
    Get l2
    Project (#0)
      Get l1
