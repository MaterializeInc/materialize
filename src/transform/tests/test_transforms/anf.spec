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
DefSource name=t0
  - c0: bigint
  - c1: bigint
----
Source defined as t0


# Linear chains.
apply pipeline=anf
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3
  Reduce group_by=[#0] aggregates=[min(#1), max(#2)]
    ArrangeBy keys=[[#1], [#2, #3]]
      Threshold
        Negate
          Project (#2, #0, #1)
            Filter (#0 > 0)
              Map (null::bigint)
                Get t0
----
With
  cte l0 =
    Get t0
  cte l1 =
    Map (null)
      Get l0
  cte l2 =
    Filter (#0 > 0)
      Get l1
  cte l3 =
    Project (#2, #0, #1)
      Get l2
  cte l4 =
    Negate
      Get l3
  cte l5 =
    Threshold
      Get l4
  cte l6 =
    ArrangeBy keys=[[#1], [#2, #3]]
      Get l5
  cte l7 =
    Reduce group_by=[#0] aggregates=[min(#1), max(#2)]
      Get l6
  cte l8 =
    TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3
      Get l7
Return
  Get l8

# Joins.
apply pipeline=anf
Join on=(#0 = #2 = #4)
  Get t0
  Constant <empty> // { types: "(bigint, bigint)" }
  Get t0
----
With
  cte l0 =
    Get t0
  cte l1 =
    Constant <empty>
  cte l2 =
    Join on=(#0 = #2 = #4)
      Get l0
      Get l1
      Get l0
Return
  Get l2

# Union.
apply pipeline=anf
Union
  Get t0
  Constant <empty> // { types: "(bigint, bigint)" }
  Get t0
----
With
  cte l0 =
    Get t0
  cte l1 =
    Constant <empty>
  cte l2 =
    Union
      Get l0
      Get l1
      Get l0
Return
  Get l2


## LetRec cases
## ------------


# Recursive queries.
apply pipeline=anf
With
  cte l0 =
    Union
      Project (#0, #1)
        Get t0
      Project (#0, #1)
        Get t0
  cte l3 =
    With Mutually Recursive
      cte l1 = // { types: "(bigint, bigint?)" }
        Distinct project=[#0, #1]
          Union
            Filter #1 > 7
              Get l0
            Filter #1 > 7
              Get l1
            Filter #1 > 7
              Get l1
            Filter #1 > 7
              Get l2
            Filter #1 > 7
              Get l2
      cte l2 = // { types: "(bigint, bigint?)" }
        Distinct project=[#0, #1]
          Union
            Filter #1 > 7
              Get l0
            Filter #1 > 7
              Get l1
            Filter #1 > 7
              Get l1
            Filter #1 > 7
              Get l2
            Filter #1 > 7
              Get l2
    Return
      Union
        Filter #1 > 7
          Get t0
        Filter #1 > 7
          Get l2
        Filter #1 > 7
          Get l1
Return
  Get l3
----
With
  cte l0 =
    Get t0
  cte l1 =
    Project (#0, #1)
      Get l0
  cte l2 =
    Union
      Get l1
      Get l1
  cte l21 =
    With Mutually Recursive
      cte l6 =
        Filter (#1 > 7)
          Get l2
      cte l7 =
        Filter (#1 > 7)
          Get l11
      cte l8 =
        Filter (#1 > 7)
          Get l15
      cte l9 =
        Union
          Get l6
          Get l7
          Get l7
          Get l8
          Get l8
      cte l10 =
        Distinct project=[#0, #1]
          Get l9
      cte l11 =
        Get l10
      cte l12 =
        Filter (#1 > 7)
          Get l11
      cte l13 =
        Union
          Get l6
          Get l12
          Get l12
          Get l8
          Get l8
      cte l14 =
        Distinct project=[#0, #1]
          Get l13
      cte l15 =
        Get l14
    Return
      With
        cte l16 =
          Get t0
        cte l17 =
          Filter (#1 > 7)
            Get l16
        cte l18 =
          Filter (#1 > 7)
            Get l15
        cte l19 =
          Filter (#1 > 7)
            Get l11
        cte l20 =
          Union
            Get l17
            Get l18
            Get l19
      Return
        Get l20
Return
  Get l21

# From a failing test
# See https://github.com/MaterializeInc/materialize/pull/19287#issuecomment-1555923422.
apply pipeline=anf
Return
  Project (#1)
    Filter (#1 > 7)
      Map ((#0 + #0))
        Get l0
With Mutually Recursive
  cte l1 = // { types: "(bigint)" }
    Union
      Get l0
      Project (#1)
        Filter (#1 > 7)
          Map ((#0 + #0))
            Get l0
      Project (#1)
        Filter (#1 > 7)
          Map ((#0 + #0))
            Get l0
  cte l0 = // { types: "(bigint)" }
    Union
      Project (#0)
        Get t0
      Get l1
----
With
  cte l15 =
    With Mutually Recursive
      cte l3 =
        Get t0
      cte l4 =
        Project (#0)
          Get l3
      cte l5 =
        Union
          Get l4
          Get l11
      cte l6 =
        Get l5
      cte l7 =
        Map ((#0 + #0))
          Get l6
      cte l8 =
        Filter (#1 > 7)
          Get l7
      cte l9 =
        Project (#1)
          Get l8
      cte l10 =
        Union
          Get l6
          Get l9
          Get l9
      cte l11 =
        Get l10
    Return
      With
        cte l12 =
          Map ((#0 + #0))
            Get l6
        cte l13 =
          Filter (#1 > 7)
            Get l12
        cte l14 =
          Project (#1)
            Get l13
      Return
        Get l14
Return
  Get l15
