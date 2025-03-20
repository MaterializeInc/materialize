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


# Factor out common subexpressions.
apply pipeline=relation_cse
Union
  Map (null::bigint)
    Get t0
  Map (null::bigint)
    Get t0
----
With
  cte l0 =
    Map (null)
      Get t0
Return
  Union
    Get l0
    Get l0


## LetRec cases
## ------------


# Recursive queries.
apply pipeline=relation_cse
Return
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
With
  cte l0 =
    Union
      Project (#0, #1)
        Get t0
      Project (#0, #1)
        Get t0
----
With Mutually Recursive
  cte l0 =
    Project (#0, #1)
      Get t0
  cte l1 =
    Filter (#1 > 7)
      Union
        Get l0
        Get l0
  cte l2 =
    Filter (#1 > 7)
      Get l4
  cte l3 =
    Filter (#1 > 7)
      Get l6
  cte l4 =
    Distinct project=[#0, #1]
      Union
        Get l1
        Get l2
        Get l2
        Get l3
        Get l3
  cte l5 =
    Filter (#1 > 7)
      Get l4
  cte l6 =
    Distinct project=[#0, #1]
      Union
        Get l1
        Get l5
        Get l5
        Get l3
        Get l3
Return
  Union
    Filter (#1 > 7)
      Get t0
    Filter (#1 > 7)
      Get l6
    Filter (#1 > 7)
      Get l4
