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


# Lift literals from constant collections.
# A suffix of common literals is lifted as a map.
# Everything else is rewritten as a Map + Project.
apply pipeline=literal_lifting
Constant // { types: "(text, bigint, bigint, bigint, text)" }
  - (1, "my", 2, 3, "foo")
  - (1, "oh", 2, 5, "foo")
  - (1, "my", 2, 7, "foo")
----
Map ("foo")
  Project (#2, #0, #3, #1)
    Map (1, 2)
      Constant
        - ("my", 3)
        - ("oh", 5)
        - ("my", 7)


# Lift prefix and suffix from constant collections.
# Pull shared suffix through union.
apply pipeline=literal_lifting
Union
  Constant // { types: "(bigint, bigint, bigint)" }
    - (1, 2, 3)
    - (2, 2, 3)
  Constant // { types: "(bigint, bigint, bigint)" }
    - (4, 3, 3)
    - (4, 5, 3)
----
Map (3)
  Union
    Map (2)
      Constant
        - (1)
        - (2)
    Project (#1, #0)
      Map (4)
        Constant
          - (3)
          - (5)


# Illustrates a limitation of the current implementation.
# Scalar expressions are not reduced after substituting the lifted literals.
apply pipeline=literal_lifting
Union
  Map (#2 + 5)
    Map (1)
      Get t0
  Map (1, 6)
    Get t0
----
Union
  Project (#0, #1, #3, #2)
    Map ((1 + 5), 1)
      Get t0
  Map (1, 6)
    Get t0


## LetRec cases
## ------------


# Single binding, value knowledge
apply pipeline=literal_lifting
Return
  Map ((#0 + #1))
    Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint)" }
    Distinct group_by=[#0, #1]
      Union
        Map (42)
          Project (#0)
            Get t0
        Constant // { types: "(bigint, bigint)" }
          - (1, 42)
          - (2, 42)
----
Return
  Project (#0, #2, #1)
    Map ((#0 + 42), 42)
      Get l0
With Mutually Recursive
  cte l0 =
    Distinct group_by=[#0]
      Union
        Project (#0)
          Get t0
        Constant
          - (1)
          - (2)


# Multiple bindings, value knowledge
# Lifts the 17 from the end of l0 up through the top
# of the l1 value, but not further.
apply pipeline=literal_lifting
Return
  Get l1
With Mutually Recursive
  cte l1 = // { types: "(bigint, bigint, bigint)" }
    Distinct group_by=[#0, #1, #2]
      Union
        CrossJoin
          Get t0
          Get l0
        Map (17)
          Project (#0, #1)
            Get l1
  cte l0 = // { types: "(bigint)" }
    Distinct group_by=[#0]
      Union
        Constant // { types: "(bigint)" }
          - (17)
        Map (17)
          Constant <empty> // { types: "()" }
----
Return
  Get l1
With Mutually Recursive
  cte l1 =
    Map (17)
      Distinct group_by=[#0, #1]
        Union
          CrossJoin
            Get t0
            Get l0
          Project (#0, #1)
            Get l1
  cte l0 =
    Distinct
      Union
        Constant
          - ()
        Constant <empty>
