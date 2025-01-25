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

# Constant
explain with=humanized_exprs
Constant // { types: "(bigint, bigint)" }
  - (1, 2)
  - (1, 3)
----
Constant
  - (1, 2)
  - (1, 3)

# Global Get
explain with=humanized_exprs
Get t0
----
Get t0

# Local Get in a Let block
explain with=humanized_exprs
Return
  Get l0
With
  cte l0 =
    Get t0
----
With
  cte l0 =
    Get t0
Return
  Get l0

# Local Get in a LetRec block
explain with=humanized_exprs
Return
  Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint)" }
    Get t0
----
With Mutually Recursive
  cte l0 =
    Get t0
Return
  Get l0

# Project
explain with=humanized_exprs
Project (#1, #0, #1)
  Get t0
----
Project (#1{c1}, #0{c0}, #1{c1})
  Get t0

# Map
explain with=humanized_exprs
Map (#1, #0 + #1, #0)
  Get t0
----
Map (#1{c1}, (#0{c0} + #1{c1}), #0{c0})
  Get t0

# FlatMap
explain with=humanized_exprs
FlatMap generate_series(#0, #1, 1)
  Get t0
----
FlatMap generate_series(#0{c0}, #1{c1}, 1)
  Get t0

# Filter
explain with=humanized_exprs
Filter (#0 > #1 + 42)
  Get t0
----
Filter (#0{c0} > (#1{c1} + 42))
  Get t0

# Reduce
explain with=humanized_exprs
Reduce group_by=[#0, 42] aggregates=[min(#1), max(#1)]
  Get t0
----
Reduce group_by=[#0{c0}, 42] aggregates=[min(#1{c1}), max(#1{c1})]
  Get t0

# TopK
explain with=humanized_exprs
TopK group_by=[#1] order_by=[#0 desc nulls_first] limit=3
  Get t0
----
TopK group_by=[#1{c1}] order_by=[#0{c0} desc nulls_first] limit=3
  Get t0

# Negate
explain with=humanized_exprs
Negate
  Get t0
----
Negate
  Get t0

# Threshold
explain with=humanized_exprs
Threshold
  Get t0
----
Threshold
  Get t0

# ArrangeBy
explain with=humanized_exprs
ArrangeBy keys=[[#1], [#1, #0]]
  Get t0
----
ArrangeBy keys=[[#1{c1}], [#1{c1}, #0{c0}]]
  Get t0

# Union (anonymous last)
explain with=humanized_exprs
Union
  Get t0
  Constant <empty> // { types: "(bigint, bigint)" }
----
Union
  Get t0
  Constant <empty>

# Union (anonymous first)
explain with=humanized_exprs
Union
  Constant <empty> // { types: "(bigint, bigint)" }
  Get t0
----
Union
  Constant <empty>
  Get t0

# Join
explain with=humanized_exprs
Join on=(#1 = #2)
  Get t0
  Get t0
----
Join on=(#1{c1} = #2{c0})
  Get t0
  Get t0
