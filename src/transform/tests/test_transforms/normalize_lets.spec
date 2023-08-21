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


# Inline Let bindings with a single reference.
apply pipeline=normalize_lets
Return
  Project (#0)
    Get l0
With
  cte l0 =
    Filter #0 > 0
      Get t0
----
Project (#0)
  Filter (#0 > 0)
    Get t0

# Inline non-recursive LetRec bindings with a single reference (1).
apply pipeline=normalize_lets
Return
  Project (#0)
    Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint)" }
    Filter #0 > 0
      Get t0
----
Project (#0)
  Filter (#0 > 0)
    Get t0

# Inline non-recursive LetRec bindings with a single reference (2).
apply pipeline=normalize_lets
Return
  Union
    Get l1
    Get l1
With Mutually Recursive
  cte l1 = // { types: "(bigint, bigint)" }
    Get l0
  cte l0 = // { types: "(bigint, bigint)" }
    Filter #0 > 0
      Get t0
----
Return
  Union
    Get l0
    Get l0
With
  cte l0 =
    Filter (#0 > 0)
      Get t0

# Promote non-recursive Let bindings up the tree.
apply pipeline=normalize_lets
Project (#0, #2)
  Return
    Return
      CrossJoin
        Get l2
        Get l2
    With
      cte l2 =
        Union
          Get l1
          Get l1
  With
    cte l1 =
      Project (#0, #3)
        Return
          CrossJoin
            Get l0
            Get l0
      With
        cte l0 =
          Project (#1, #0)
            Get t0
----
Return
  Project (#0, #2)
    CrossJoin
      Get l2
      Get l2
With
  cte l2 =
    Union
      Get l1
      Get l1
  cte l1 =
    Project (#0, #3)
      CrossJoin
        Get l0
        Get l0
  cte l0 =
    Project (#1, #0)
      Get t0

# Promote LetRec nodes up the tree.
apply pipeline=normalize_lets
Map ("return_inner")
  Return
    Get l1
  With Mutually Recursive
    cte l1 = // { types: "(bigint, bigint)" }
      Filter (#0 < 100)
        Return
          Union
            Filter (#0 < 20)
              Get l1
            Get l0
        With Mutually Recursive
          cte l0 = // { types: "(bigint, bigint)" }
            Union
              Filter (#0 < 10)
                Get l0
              Get t0
----
Return
  Map ("return_inner")
    Get l1
With Mutually Recursive
  cte l1 =
    Return
      Filter (#0 < 100)
        Union
          Filter (#0 < 20)
            Get l1
          Get l0
    With Mutually Recursive
      cte l0 =
        Union
          Filter (#0 < 10)
            Get l0
          Get t0

# Don't inline Let bindings with multiple references.
apply pipeline=normalize_lets
Return
  Union
    Project (#1)
      Get l0
    Project (#0)
      Get l0
With
  cte l0 =
    Filter #0 > 0
      Get t0
----
Return
  Union
    Project (#1)
      Get l0
    Project (#0)
      Get l0
With
  cte l0 =
    Filter (#0 > 0)
      Get t0

# Fuse an outer Let into an inner LetRec.
apply pipeline=normalize_lets
Return
  Union
    Get l2
    Get l2
With
  cte l2 =
    Return
      Filter #0 > 0
        Get l1
    With Mutually Recursive
      cte l1 = // { types: "(bigint, bigint, bigint?)" }
        Union
          Get l0
          Get l0
          Get l1
  cte l0 =
    Map (null::bigint)
      Get t0
----
Return
  Return
    Union
      Get l2
      Get l2
  With
    cte l2 =
      Filter (#0 > 0)
        Get l1
With Mutually Recursive
  cte l1 =
    Union
      Get l0
      Get l0
      Get l1
  cte l0 =
    Map (null)
      Get t0
