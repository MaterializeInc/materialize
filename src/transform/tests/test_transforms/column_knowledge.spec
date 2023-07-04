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

# Define t0 source
define
DefSource name=t0 keys=[[#0]]
  - bigint
  - bigint?
----
Source defined as t0

# Define t1 source
define
DefSource name=t1 keys=[[#0]]
  - text
  - bigint
  - boolean
----
Source defined as t1

# Define t2 source
define
DefSource name=t2
  - bigint?
  - bigint?
----
Source defined as t2

# Define t3 source
define
DefSource name=t3
  - bigint?
  - bigint?
----
Source defined as t3


# Infer and apply constant value knowledge.
# Cases: Map, FlatMap, Filter, Project, Reduce, Let/Get.
apply pipeline=column_knowledge
Return
  FlatMap generate_series(#1, #0 + 3, 1)
    Project (#2, #0)
      Get l0
With
  cte l0 =
    Filter (#1) IS NULL
      Reduce group_by=[#0 * #1] aggregates=[sum(#2), max(#0 + #2)]
        Filter #0 + #1 > 2 AND #2 > #1 + #1
          Map (#0 + #1)
            Filter #0 = 1 AND #1 = 2
              Get t0
----
Return
  FlatMap generate_series(2, 7, 1)
    Project (#2, #0)
      Get l0
With
  cte l0 =
    Filter false
      Reduce group_by=[2] aggregates=[sum(3), max(4)]
        Filter true AND false
          Map (3)
            Filter (#0 = 1) AND (#1 = 2)
              Get t0


# Infer and apply nullability knowledge.
# Cases: Map, Filter, Project, Reduce, Let/Get.
# TODO: The type of `(#1) IS NULL` in the map is `BOOLEAN NOT NULL`,
#       so the first group_by key component should be reduced to `false`.
apply pipeline=column_knowledge
Return
  Filter (#0) IS NULL
    Project (#2)
      Get l0
With
  cte l0 =
    Reduce group_by=[(#2) IS NULL, (#3) IS NULL] aggregates=[count((#3) IS NULL)]
      Map ((#1) IS NULL, (#0) IS NULL)
        Filter (#0) IS NULL AND (#1) IS NULL
          Get t0
----
Return
  Filter false
    Project (#2)
      Get l0
With
  cte l0 =
    Reduce group_by=[(#2) IS NULL, false] aggregates=[count(false)]
      Map ((#1) IS NULL, false)
        Filter false AND (#1) IS NULL
          Get t0


# Infer and apply constant value knowledge.
# Cases: Union.
apply pipeline=column_knowledge
Return
  Filter #1 > 1 AND #2 IS NULL
    Union
      Get l0
      Get l1
With
  cte l1 =
    Project (#0, #0, #1)
      Filter (#0 = 2) AND (#1 = 3)
        Get t0
  cte l0 =
    Project (#0, #1, #1)
      Filter (#0 = 1) AND (#1 = 2)
        Get t0
----
Return
  Filter true AND false
    Union
      Get l0
      Get l1
With
  cte l1 =
    Project (#0, #0, #1)
      Filter (#0 = 2) AND (#1 = 3)
        Get t0
  cte l0 =
    Project (#0, #1, #1)
      Filter (#0 = 1) AND (#1 = 2)
        Get t0


# Infer and apply constant value knowledge.
# Cases: Join.
apply pipeline=column_knowledge
Return
  Map (#0 + #1 + #2, #2 * #3)
    Join on=((#0 + #1) = #2)
      Get l0
      Get t0
With
  cte l0 =
    Filter ((#0 = 1) AND (#1 = 2))
      Get t0
----
Return
  Map (6, (3 * #3))
    Join on=(3 = #2)
      Get l0
      Get t0
With
  cte l0 =
    Filter (#0 = 1) AND (#1 = 2)
      Get t0


## Outer join patterns
## -------------------


# Single binding, value knowledge
apply pipeline=column_knowledge
Return
  Project (#0, #1, #3, #4, #5, #6)
    Map ((#0) IS NULL, (#0) IS NULL, (#2) IS NULL)
      Union
        Map (null::bigint, null::bigint)
          Union
            Project (#0, #1)
              Negate
                Join on=(#0 = #2)
                  Get t2
                  Distinct group_by=[#0]
                    Get l0
            Get t2
        Get l0
With
  cte l0 =
    Join on=(#0 = #2)
      Filter (#0) IS NOT NULL
        Get t2
      Filter (#0) IS NOT NULL
        Get t3
----
Return
  Project (#0, #1, #3..=#6)
    Map ((#0) IS NULL, (#0) IS NULL, (#2) IS NULL)
      Union
        Map (null, null)
          Union
            Project (#0, #1)
              Negate
                Join on=(#0 = #2)
                  Get t2
                  Distinct group_by=[#0]
                    Get l0
            Get t2
        Get l0
With
  cte l0 =
    Join on=(#0 = #2)
      Filter (#0) IS NOT NULL
        Get t2
      Filter (#0) IS NOT NULL
        Get t3


## LetRec cases
## ------------


# Single binding, value knowledge
apply pipeline=column_knowledge
Return
  Map (#0 + #1)
    Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint?)" }
    Filter #0 = 3 AND #1 = 5
      Distinct group_by=[#0, #1]
        Union
          Get t0
          Get l0
----
Return
  Map (8)
    Get l0
With Mutually Recursive
  cte l0 =
    Filter (#0 = 3) AND (#1 = 5)
      Distinct group_by=[#0, #1]
        Union
          Get t0
          Get l0


# Single binding, NOT NULL knowledge
apply pipeline=column_knowledge
Return
  Map (#1 IS NOT NULL)
    Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint?)" }
    Filter #1 IS NOT NULL
      Distinct group_by=[#0, #1]
        Union
          Get t0
          Get l0
----
Return
  Map (true)
    Get l0
With Mutually Recursive
  cte l0 =
    Filter (#1) IS NOT NULL
      Distinct group_by=[#0, #1]
        Union
          Get t0
          Get l0


# Multiple bindings, value knowledge
apply pipeline=column_knowledge
Return
  Get l1
With Mutually Recursive
  cte l1 = // { types: "(bigint, bigint, bigint)" }
    Distinct group_by=[#0, #1, #2]
      Union
        Project (#3, #1, #2)
          Map (#0 * 2)
            CrossJoin
              Get l0
              Get t0
        Get l1
  cte l0 = // { types: "(bigint)" }
    Distinct group_by=[#0]
      Union
        Constant // { types: "(bigint)" }
          - (1)
        Filter (#0 = 1)
          Get l0
----
Return
  Get l1
With Mutually Recursive
  cte l1 =
    Distinct group_by=[2, #1, #2]
      Union
        Project (#3, #1, #2)
          Map (2)
            CrossJoin
              Get l0
              Get t0
        Get l1
  cte l0 =
    Distinct group_by=[1]
      Union
        Constant
          - (1)
        Filter true
          Get l0



# Multiple bindings, NOT NULL knowledge
#
# This also illustrates a missed opportunity here, because if we are a bit
# smarter we will know that l1 can only have 'false' in its first component.
apply pipeline=column_knowledge
Return
  Get l1
With Mutually Recursive
  cte l1 = // { types: "(boolean, bigint, bigint)" }
    Distinct group_by=[#0, #1, #2]
      Union
        Project (#3, #1, #2)
          Map (#0 IS NULL)
            CrossJoin
              Get l0
              Get t0
        Get l1
  cte l0 = // { types: "(bigint)" }
    Distinct group_by=[#0]
      Union
        Constant // { types: "(bigint)" }
          - (1)
        Filter (#0 IS NOT NULL)
          Get l0
----
Return
  Get l1
With Mutually Recursive
  cte l1 =
    Distinct group_by=[false, #1, #2]
      Union
        Project (#3, #1, #2)
          Map (false)
            CrossJoin
              Get l0
              Get t0
        Get l1
  cte l0 =
    Distinct group_by=[1]
      Union
        Constant
          - (1)
        Filter true
          Get l0



# # TODO
# apply pipeline=column_knowledge
# Return
#   Map (#0 + #1)
#     Get l1
# With Mutually Recursive
#   cte l1 = // { types: "(bigint, bigint?)" }
#     Filter #0 = 1
#       Get t0
#   cte l0 = // { types: "(bigint, bigint?)" }
#     Get t0
# ----
# Return
#   Map ((#0 + #1))
#     Get l1
# With Mutually Recursive
#   cte l1 =
#     Filter (#0 = 1)
#       Get t0
#   cte l0 =
#     Get t0
