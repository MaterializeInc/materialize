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


# Pushing through Get, Distinct, Union, Project, Join
apply pipeline=predicate_pushdown
Return
  Filter #0 = "a"
    Get l1
With
  cte l1 =
    Distinct group_by=[#0, #1, #2]
      Union
        Project (#0, #1, #5)
          Join on=(#0 = #3 AND #2 = #4)
            Get l0
            Get t1
        Get t1
  cte l0 =
    Constant // { types: "(text, bigint, bigint)" }
      - ("a", 1, 2)
      - ("b", 3, 4)
----
Return
  Filter
    Get l1
With
  cte l1 =
    Distinct group_by=[#0..=#2]
      Union
        Project (#0, #1, #5)
          Join on=(#0 = #3 AND #2 = #4)
            Filter
              Get l0
            Filter (#0 = "a")
              Get t1
        Filter (#0 = "a")
          Get t1
  cte l0 =
    Filter (#0 = "a")
      Constant
        - ("a", 1, 2)
        - ("b", 3, 4)


## LetRec cases
## ------------

# TODO: Push a literal constraint through a loop
apply pipeline=predicate_pushdown
Return
  Filter (#0 = "foo")
    Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint)" }
    Distinct group_by=[#0, #1]
      Union
        Get t0
        Get l0
        Project (#0, #3)
          Join on=(#1 = #2)
            Get l0
            Get l0
----
Return
  Filter (#0 = "foo")
    Get l0
With Mutually Recursive
  cte l0 =
    Distinct group_by=[#0, #1]
      Union
        Get t0
        Get l0
        Project (#0, #3)
          Join on=(#1 = #2)
            Get l0
            Get l0
