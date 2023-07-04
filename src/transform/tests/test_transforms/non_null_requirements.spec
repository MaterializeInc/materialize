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

# Prune subterms with literal nulls based on
# requirements derived from Filter predicates.
apply pipeline=non_null_requirements
Filter #2 > #0
  Union
    Map (null::bigint)
      Get t0
    Constant // { types: "(bigint, bigint, bigint)" }
      - (1, 2, 3)
      - (2, 3, 4)
----
Filter (#2 > #0)
  Union
    Constant <empty>
    Constant
      - (1, 2, 3)
      - (2, 3, 4)

# Prune subterms with literal nulls based on
# requirements derived from Map expressions.
apply pipeline=non_null_requirements
Filter (#3 > 0)
  Map ((#2 + #0))
    Union
      Constant // { types: "(bigint, bigint, bigint)" }
        - (1, 2, 3)
        - (2, 3, 4)
      Constant // { types: "(bigint, bigint, bigint?)" }
        - (1, 2, null)
        - (1, 3, null)
----
Filter (#3 > 0)
  Map ((#2 + #0))
    Union
      Constant
        - (1, 2, 3)
        - (2, 3, 4)
      Constant <empty>

# Prune subterms with literal nulls based on
# requirements derived from FlatMap expressions.
apply pipeline=non_null_requirements
FlatMap generate_series(#0, #1, #2)
  Union
    Negate
      Map (null::bigint)
        Get t0
    Constant // { types: "(bigint, bigint, bigint?)" }
      - (1, 2, null)
      - (1, 3, null)
----
FlatMap generate_series(#0..=#2)
  Union
    Negate
      Constant <empty>
    Constant <empty>


# Prune subterms based on
# requirements derived through Let bindings.
apply pipeline=non_null_requirements
Return
  Filter #2 > 0
    Get l1
With
  cte l1 =
    ArrangeBy keys=[[#0]]
      Threshold
        Get l0
  cte l0 =
    Map (null::bigint)
      Get t0
----
Return
  Filter (#2 > 0)
    Get l1
With
  cte l1 =
    ArrangeBy keys=[[#0]]
      Threshold
        Get l0
  cte l0 =
    Constant <empty>


# Regression test for #5520
apply pipeline=non_null_requirements
Filter #0 = #3
  FlatMap generate_series(#1)
    Map (null::bigint)
      Get t0
----
Filter (#0 = #3)
  FlatMap generate_series(#1)
    Map (null)
      Get t0


## LetRec cases
## ------------


# Prune subterms based on
# requirements derived through Let bindings.
#
# Note: if we implement fixpoint-based handling of LetRec bindings, the
# `Filter #2 > 0` under `l1` will not be needed any more.
apply pipeline=non_null_requirements
Return
  Filter #0 < 7 AND #2 > 0
    Union
      Get l2
      Get l1
With Mutually Recursive
  cte l2 = // { types: "(bigint, bigint, bigint?)" }
    ArrangeBy keys=[[#0]]
      Threshold
        Get l0
  cte l1 = // { types: "(bigint, bigint, bigint?)" }
    Distinct group_by=[#0, #1, #2]
      Union
        Get l1
        Filter #2 > 0
          Get l0
  cte l0 = // { types: "(bigint, bigint, bigint?)" }
    Map (null::bigint)
      Get t0
----
Return
  Filter (#0 < 7) AND (#2 > 0)
    Union
      Get l2
      Get l1
With Mutually Recursive
  cte l2 =
    ArrangeBy keys=[[#0]]
      Threshold
        Get l0
  cte l1 =
    Distinct group_by=[#0..=#2]
      Union
        Get l1
        Filter (#2 > 0)
          Get l0
  cte l0 =
    Constant <empty>
