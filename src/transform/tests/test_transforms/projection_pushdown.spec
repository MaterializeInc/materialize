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
  - bigint
  - bigint
  - bigint
----
Source defined as t0

# Define y source
define
DefSource name=y
  - bigint
  - bigint
  - bigint
----
Source defined as t1


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

# Query not using the columns newly created by FlatMap
apply pipeline=projection_pushdown
Project (#1)
  FlatMap generate_series(#0, #2, 1)
    Get x
----
Project (#1)
  FlatMap generate_series(#0, #2, 1)
    Get x

# Query using the columns newly created by FlatMap
apply pipeline=projection_pushdown
Project (#3)
  FlatMap generate_series(#0, #2, 1)
    Get x
----
Project (#2)
  FlatMap generate_series(#0, #1, 1)
    Project (#0, #2)
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
  Distinct group_by=[(#0 + #2)]
    Get x
----
Project ()
  Distinct group_by=[(#0 + #1)]
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
  TopK group_by=[#0] order_by=[#1 asc nulls_first, #2 asc nulls_first]
    Get x
----
Project (#0, #0, #0)
  Project (#2)
    TopK group_by=[#0] order_by=[#1 asc nulls_first, #2 asc nulls_first]
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
  TopK group_by=[#2] order_by=[#1 asc nulls_first]
    Get x
----
TopK group_by=[#0] order_by=[#1 asc nulls_first]
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
Return
  Project (#1)
    Join on=(#0 = #2)
      Project (#0, #2)
        Get l0
      Project (#1)
        Get l0
With
  cte l0 =
    Project (#0, #1, #3)
      Join on=(#0 = #2)
        Project (#0, #2)
          Get x
        Project (#0, #2)
          Get y

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
Return
  Project (#1)
    Join on=(#0 = #2)
      Get l0
      Project (#1)
        Get l0
With
  cte l0 =
    Project (#0, #1)
      Join on=(#0 = #2)
        Project (#0, #2)
          Get x
        Project (#0)
          Get y

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
Return
  Union
    Join on=(#0 = #1)
      Project (#0)
        Get l0
      Get y
    Project (#0, #1, #0, #2)
      Get l0
With
  cte l0 =
    Map (1)
      Project (#0, #1)
        Get x


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
Return
  Project (#1)
    Join on=(#0 = #2)
      Project (#0, #2)
        Get l0
      Project (#1)
        Get l0
With Mutually Recursive
  cte l0 =
    Project (#0, #1, #3)
      Join on=(#0 = #2)
        Project (#0, #2)
          Get x
        Project (#0, #2)
          Get y

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
Return
  Project (#1)
    Join on=(#0 = #2)
      Get l0
      Project (#1)
        Get l0
With Mutually Recursive
  cte l0 =
    Project (#0, #1)
      Join on=(#0 = #2)
        Project (#0, #2)
          Get x
        Project (#0)
          Get y

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
Return
  Union
    Join on=(#0 = #1)
      Project (#0)
        Get l0
      Get y
    Project (#0, #1, #0, #2)
      Get l0
With Mutually Recursive
  cte l0 =
    Map (1)
      Project (#0, #1)
        Get x


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
      Distinct group_by=[#0]
        Union
          Project (#0)
            Get l1
          Project (#0)
            Get x
  cte l0 = // { types: "(bigint, bigint, bigint)" }
    Get x
----
Return
  Join on=(#0 = #1)
    Get l2
    Project (#0)
      Get l1
With Mutually Recursive
  cte l2 =
    Get l0
  cte l1 =
    Project (#0, #0, #0)
      Distinct group_by=[#0]
        Union
          Project (#0)
            Get l1
          Project (#0)
            Get x
  cte l0 =
    Project (#0)
      Get x
