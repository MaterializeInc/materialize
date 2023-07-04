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


# Case: `Project`
apply pipeline=projection_lifting
Project (#2, #3, #4)
  Project (#0, #1, #2, #0, #1, #2)
    Get t1
----
Project (#2, #0, #1)
  Get t1


# Case: `Map`
apply pipeline=projection_lifting
Map (#3 + 1, #1 + 3)
  Project (#0, #0, #2, #1)
    Get t1
----
Project (#0, #0, #2, #1, #3, #4)
  Map ((#1 + 1), (#0 + 3))
    Get t1


# Case: `FlatMap`
apply pipeline=projection_lifting
FlatMap generate_series(#3 + 1, #2 + 3, #1)
  Project (#0, #2, #1, #2)
    Get t1
----
Project (#0, #2, #1..=#3)
  FlatMap generate_series((#2 + 1), (#1 + 3), #2)
    Get t1


# Case: `Filter`
apply pipeline=projection_lifting
Filter 5 * (#1 + #3) > #2
  Project (#0, #2, #1, #2)
    Get t1
----
Project (#0, #2, #1, #2)
  Filter ((5 * (#2 + #2)) > #1)
    Get t1


# Case: `Join`
apply pipeline=projection_lifting
Join on=(#1 = #5 AND #2 = #6)
  Get t1
  Project (#0, #2, #1, #2)
    Get t1
----
Project (#0..=#3, #5, #4, #5)
  Join on=(#1 = #4 AND #2 = #5)
    Get t1
    Get t1


# Case: `Reduce`
apply pipeline=projection_lifting
Reduce group_by=[#1, #2] aggregates=[sum(#3), max(#2 + #3)]
  Project (#0, #2, #1, #2)
    Get t1
----
Reduce group_by=[#2, #1] aggregates=[sum(#2), max((#1 + #2))]
  Get t1


# Case: `TopK`
apply pipeline=projection_lifting
TopK group_by=[#2, #1] order_by=[#1 asc nulls_last] monotonic
  Project (#0, #2, #1, #2)
    Get t1
----
Project (#0, #2, #1, #2)
  TopK group_by=[#1, #2] order_by=[#2 asc nulls_last]
    Get t1


# Case: `Negate`
apply pipeline=projection_lifting
Union
  Map (true, "")
    Get t0
  Negate
    Project (#1, #1, #2, #0)
      Get t1
----
Union
  Map (true, "")
    Get t0
  Project (#1, #1, #2, #0)
    Negate
      Get t1


# Case: `Union`
apply pipeline=projection_lifting
Union
  Project (#1, #1, #2, #0)
    Get t1
  Project (#1, #1, #2, #0)
    Get t1
----
Project (#1, #1, #2, #0)
  Union
    Get t1
    Get t1


## LetRec cases
## ------------


# Three bindings, l0 and l2 are not recursive
apply pipeline=projection_lifting
Return
  Join on=(#0 = #2)
    Get l2
    Get l1
    Get l0
With Mutually Recursive
  cte l2 = // { types: "(bigint, bigint?)" }
    Filter #0 IS NOT NULL
      Project (#1, #1)
        Get t1
  cte l1 = // { types: "(bigint, bigint?)" }
    Distinct group_by=[#0, #1]
      Union
        Get t0
        Get l0
  cte l0 = // { types: "(bigint, bigint?)" }
    Filter #0 IS NOT NULL
      Project (#1, #1)
        Get t1
----
Return
  Project (#1, #1, #3, #4, #6, #6)
    Join on=(#1 = #3)
      Get l2
      Get l1
      Get l0
With Mutually Recursive
  cte l2 =
    Filter (#1) IS NOT NULL
      Get t1
  cte l1 =
    Distinct group_by=[#0, #1]
      Union
        Get t0
        Project (#1, #1)
          Get l0
  cte l0 =
    Filter (#1) IS NOT NULL
      Get t1
