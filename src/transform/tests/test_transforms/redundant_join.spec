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
  - bigint
  - bigint
----
Source defined as t0

# Define y source
define
DefSource name=y keys=[[#0]]
  - bigint
  - bigint
----
Source defined as t1


# ProvInfo inference (basic cases)
# --------------------------------

# Simple get.
# [ProvInfo {
#   id: t0,
#   dereferenced_projection: [#0, #1],
#   exact: true
# }]
apply pipeline=redundant_join
Get x
----
Get x

# Chain of projections.
# [ProvInfo {
#   id: t0,
#   dereferenced_projection: [#1, #1, #0],
#   exact: true
# }]
apply pipeline=redundant_join
Project (#0, #0, #1)
  Project (#1, #0)
    Get x
----
Project (#0, #0, #1)
  Project (#1, #0)
    Get x

# Map.
# [ProvInfo {
#   id: t0,
#   dereferenced_projection: [#0, #1, _, #1 > #0]
#   exact: true
# }]
apply pipeline=redundant_join
Map (5, #0 > #1)
  Project (#1, #0)
    Get x
----
Map (5, (#0 > #1))
  Project (#1, #0)
    Get x

# Filter.
# [ProvInfo {
#   id: t0,
#   dereferenced_projection: [#0, #1]
#   exact: false
# }]
apply pipeline=redundant_join
Filter #0 > 5
  Get x
----
Filter (#0 > 5)
  Get x

# Map + Project With Let/Get bindings.
# [ProvInfo {
#   id: t0,
#   dereferenced_projection: [_, #1, #0, #1, #0 > #1],
#   exact: true },
#  ProvInfo {
#   id: l0,
#   dereferenced_projection: [#2, #1, #0, #1, #0 > #1],
#   exact: true
# }]
apply pipeline=redundant_join
Return
  Map (#1, (#2 > #1))
    Project (#2, #1, #0)
      Get l0
With
  cte l0 =
    Map (3)
      Get x
----
Return
  Map (#1, (#2 > #1))
    Project (#2, #1, #0)
      Get l0
With
  cte l0 =
    Map (3)
      Get x



# Redundant join elimination
# --------------------------

# 2-way join, rhs
apply pipeline=redundant_join
Join on=(#0 = #2)
  Get x
  Distinct group_by=[#0]
    Get x
----
Project (#0..=#2)
  Map (#0)
    CrossJoin
      Get x

# 2-way join, rhs references a local binding with the same properties as above.
apply pipeline=redundant_join
Return
  Join on=(#0 = #2)
    Get x
    Get l0
With
  cte l0 =
    Distinct group_by=[#0]
      Get x
----
Return
  Project (#0..=#2)
    Map (#0)
      CrossJoin
        Get x
With
  cte l0 =
    Distinct group_by=[#0]
      Get x


## LetRec cases
## ------------


# Project around a LetRec (1)
apply pipeline=redundant_join
Return
  Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint, bigint)" }
    Distinct group_by=[#0, #1, #2]
      Union
        Get l0
        Join on=(#2 = (#0 % 2))
          Get x
          Distinct group_by=[(#0 % 2)]
            Project (#0)
              Get x
----
Return
  Get l0
With Mutually Recursive
  cte l0 =
    Distinct group_by=[#0..=#2]
      Union
        Get l0
        Project (#0..=#2)
          Map ((#0 % 2))
            CrossJoin
              Get x
