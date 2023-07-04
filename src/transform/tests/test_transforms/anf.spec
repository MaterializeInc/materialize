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
  - bigint
  - bigint
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
Return
  Get l8
With
  cte l8 =
    TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3
      Get l7
  cte l7 =
    Reduce group_by=[#0] aggregates=[min(#1), max(#2)]
      Get l6
  cte l6 =
    ArrangeBy keys=[[#1], [#2, #3]]
      Get l5
  cte l5 =
    Threshold
      Get l4
  cte l4 =
    Negate
      Get l3
  cte l3 =
    Project (#2, #0, #1)
      Get l2
  cte l2 =
    Filter (#0 > 0)
      Get l1
  cte l1 =
    Map (null)
      Get l0
  cte l0 =
    Get t0

# Joins.
apply pipeline=anf
Join on=(eq(#0, #2, #4))
  Get t0
  Constant <empty> // { types: "(bigint, bigint)" }
  Get t0
----
Return
  Get l2
With
  cte l2 =
    Join on=(eq(#0, #2, #4))
      Get l0
      Get l1
      Get l0
  cte l1 =
    Constant <empty>
  cte l0 =
    Get t0

# Union.
apply pipeline=anf
Union
  Get t0
  Constant <empty> // { types: "(bigint, bigint)" }
  Get t0
----
Return
  Get l2
With
  cte l2 =
    Union
      Get l0
      Get l1
      Get l0
  cte l1 =
    Constant <empty>
  cte l0 =
    Get t0


## LetRec cases
## ------------


# Recursive queries.
# Here:
# (1) l5 and l10 are equivalent.
# (2) l7 is equivalent to l12.
# (3) l6 is not equivalent (although structurally equal) to l11.
apply pipeline=anf
Return
  Get l3
With
  cte l3 =
    Return
      Union
        Filter #1 > 7
          Get t0
        Filter #1 > 7
          Get l2
        Filter #1 > 7
          Get l1
    With Mutually Recursive
      cte l2 = // { types: "(bigint, bigint?)" }
        Distinct group_by=[#0, #1]
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
      cte l1 = // { types: "(bigint, bigint?)" }
        Distinct group_by=[#0, #1]
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
  cte l0 =
    Union
      Project (#0, #1)
        Get t0
      Project (#0, #1)
        Get t0
----
Return
  Get l20
With
  cte l20 =
    Return
      Return
        Get l19
      With
        cte l19 =
          Union
            Get l16
            Get l17
            Get l18
        cte l18 =
          Filter (#1 > 7)
            Get l3
        cte l17 =
          Filter (#1 > 7)
            Get l4
        cte l16 =
          Filter (#1 > 7)
            Get l15
        cte l15 =
          Get t0
    With Mutually Recursive
      cte l4 =
        Return
          Get l14
        With
          cte l14 =
            Distinct group_by=[#0, #1]
              Get l13
          cte l13 =
            Union
              Get l10
              Get l11
              Get l11
              Get l12
              Get l12
          cte l12 =
            Filter (#1 > 7)
              Get l4
          cte l11 =
            Filter (#1 > 7)
              Get l3
          cte l10 =
            Filter (#1 > 7)
              Get l2
      cte l3 =
        Return
          Get l9
        With
          cte l9 =
            Distinct group_by=[#0, #1]
              Get l8
          cte l8 =
            Union
              Get l5
              Get l6
              Get l6
              Get l7
              Get l7
          cte l7 =
            Filter (#1 > 7)
              Get l4
          cte l6 =
            Filter (#1 > 7)
              Get l3
          cte l5 =
            Filter (#1 > 7)
              Get l2
  cte l2 =
    Union
      Get l1
      Get l1
  cte l1 =
    Project (#0, #1)
      Get l0
  cte l0 =
    Get t0

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
Return
  Get l12
With
  cte l12 =
    Return
      Return
        Get l11
      With
        cte l11 =
          Project (#1)
            Get l10
        cte l10 =
          Filter (#1 > 7)
            Get l9
        cte l9 =
          Map ((#0 + #0))
            Get l0
    With Mutually Recursive
      cte l1 =
        Return
          Get l8
        With
          cte l8 =
            Union
              Get l0
              Get l7
              Get l7
          cte l7 =
            Project (#1)
              Get l6
          cte l6 =
            Filter (#1 > 7)
              Get l5
          cte l5 =
            Map ((#0 + #0))
              Get l0
      cte l0 =
        Return
          Get l4
        With
          cte l4 =
            Union
              Get l3
              Get l1
          cte l3 =
            Project (#0)
              Get l2
          cte l2 =
            Get t0
