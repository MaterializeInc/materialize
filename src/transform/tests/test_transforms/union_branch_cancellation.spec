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
----
Source defined as t0


# Equivalent branches can't be canceled
# -------------------------------------

apply pipeline=union_branch_cancellation
Union
  Negate
    Get x
  Negate
    Get x
----
Union
  Negate
    Get x
  Negate
    Get x


apply pipeline=union_branch_cancellation
Union
  Get x
  Get x
----
Union
  Get x
  Get x


apply pipeline=union_branch_cancellation
Union
  Project (#0)
    Get x
  Project (#0)
    Get x
----
Union
  Project (#0)
    Get x
  Project (#0)
    Get x


# Negated branches are canceled
# -----------------------------

apply pipeline=union_branch_cancellation
(union [(get x) (negate (get x))])
Union
  Get x
  Negate
    Get x
----
parse error at 1:0:
expected one of: `Constant`, `Get`, `Return`, `With`, `Project`, `Map`, `FlatMap`, `Filter`, `CrossJoin`, `Join`, `Distinct`, `Reduce`, `TopK`, `Negate`, `Threshold`, `Union`, `ArrangeBy`


apply pipeline=union_branch_cancellation
Union
  Project (#0)
    Get x
  Project (#0)
    Negate
      Get x
----
Union
  Constant <empty>
  Constant <empty>


apply pipeline=union_branch_cancellation
Union
  Project (#0)
    Negate
      Get x
  Project (#0)
    Get x
----
Union
  Constant <empty>
  Constant <empty>


apply pipeline=union_branch_cancellation
Union
  Map (#0)
    Get x
  Map (#0)
    Negate
      Get x
----
Union
  Constant <empty>
  Constant <empty>


apply pipeline=union_branch_cancellation
Union
  Map (#0)
    Negate
      Get x
  Map (#0)
    Get x
----
Union
  Constant <empty>
  Constant <empty>


apply pipeline=union_branch_cancellation
Union
  Filter (#0 < 42)
    Get x
  Filter (#0 < 42)
    Negate
      Get x
----
Union
  Constant <empty>
  Constant <empty>


apply pipeline=union_branch_cancellation
Union
  Filter (#0 < 42)
    Negate
      Get x
  Filter (#0 < 42)
    Get x
----
Union
  Constant <empty>
  Constant <empty>


apply pipeline=union_branch_cancellation
Union
  Map #1
    Filter (#0 < 42)
      Get x
  Map #1
    Filter (#0 < 42)
      Negate
        Get x
----
parse error at 2:6:
expected parentheses


apply pipeline=union_branch_cancellation
Union
  Map #1
    Filter (#0 < 42)
      Negate
        Get x
  Map #1
    Filter (#0 < 42)
      Get x
----
parse error at 2:6:
expected parentheses

# map -> filter in the same order, but with a Negate in between

apply pipeline=union_branch_cancellation
Union
  Map (#0)
    Filter (#1 > 17)
      Get x
  Map (#0)
    Negate
      Filter (#1 > 17)
        Get x
----
Union
  Constant <empty>
  Constant <empty>


apply pipeline=union_branch_cancellation
Union
  Map (#0)
    Negate
      Filter (#1 > 17)
        Get x
  Map (#0)
    Filter (#1 > 17)
      Get x
----
Union
  Constant <empty>
  Constant <empty>

# map -> filter in different order, branches can't be canceled

apply pipeline=union_branch_cancellation
Union
  Filter (#0 = 17)
    Map (#1)
      Get x
  Map (#1)
    Filter (#0 = 17)
      Negate
        Get x
----
Union
  Filter (#0 = 17)
    Map (#1)
      Get x
  Map (#1)
    Filter (#0 = 17)
      Negate
        Get x

# first two branches cancel each other, but not the third one

apply pipeline=union_branch_cancellation
Union
  Map (#0)
    Negate
      Get x
  Map (#0)
    Get x
  Map (#0)
    Negate
      Get x
----
Union
  Constant <empty>
  Constant <empty>
  Map (#0)
    Negate
      Get x


apply pipeline=union_branch_cancellation
Union
  Map (#0)
    Negate
      Get x
  Map (#0)
    Get x
  Map (#0)
    Get x
----
Union
  Constant <empty>
  Constant <empty>
  Map (#0)
    Get x

# first and third cancel each other

apply pipeline=union_branch_cancellation
Union
  Map (#0)
    Negate
      Get x
  Map (#0)
    Negate
      Get x
  Map (#0)
    Get x
----
Union
  Constant <empty>
  Map (#0)
    Negate
      Get x
  Constant <empty>
