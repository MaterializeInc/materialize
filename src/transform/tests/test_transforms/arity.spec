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
explain with=arity
Constant // { types: "(bigint, bigint)" }
  - (1, 2)
  - (1, 3)
----
Constant // { arity: 2 }
  - (1, 2)
  - (1, 3)

# Global Get
explain with=arity
Get t0
----
Get t0 // { arity: 2 }

# Local Get in a Let block
explain with=arity
Return
  Get l0
With
  cte l0 =
    Get t0
----
With
  cte l0 =
    Get t0 // { arity: 2 }
Return // { arity: 2 }
  Get l0 // { arity: 2 }

# Local Get in a LetRec block
explain with=arity
Return
  Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint)" }
    Get t0
----
With Mutually Recursive
  cte l0 =
    Get t0 // { arity: 2 }
Return // { arity: 2 }
  Get l0 // { arity: 2 }

# Project
explain with=arity
Project (#1, #0, #1)
  Get t0
----
Project (#1, #0, #1) // { arity: 3 }
  Get t0 // { arity: 2 }

# Map
explain with=arity
Map (#1, #0 + #1, #0)
  Get t0
----
Map (#1, (#0 + #1), #0) // { arity: 5 }
  Get t0 // { arity: 2 }

# FlatMap
explain with=arity
FlatMap generate_series(#0, #1, 1)
  Get t0
----
FlatMap generate_series(#0, #1, 1) // { arity: 3 }
  Get t0 // { arity: 2 }

# Filter
explain with=arity
Filter (#0 > #1 + 42)
  Get t0
----
Filter (#0 > (#1 + 42)) // { arity: 2 }
  Get t0 // { arity: 2 }

# Reduce
explain with=arity
Reduce group_by=[#0] aggregates=[min(#1), max(#1)]
  Get t0
----
Reduce group_by=[#0] aggregates=[min(#1), max(#1)] // { arity: 3 }
  Get t0 // { arity: 2 }

# TopK
explain with=arity
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3
  Get t0
----
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3 // { arity: 2 }
  Get t0 // { arity: 2 }

# Negate
explain with=arity
Negate
  Get t0
----
Negate // { arity: 2 }
  Get t0 // { arity: 2 }

# Threshold
explain with=arity
Threshold
  Get t0
----
Threshold // { arity: 2 }
  Get t0 // { arity: 2 }

# ArrangeBy
explain with=arity
ArrangeBy keys=[[#1], [#0, #1]]
  Get t0
----
ArrangeBy keys=[[#1], [#0, #1]] // { arity: 2 }
  Get t0 // { arity: 2 }

# Union (anonymous last)
explain with=arity
Union
  Get t0
  Constant <empty> // { types: "(bigint, bigint)" }
----
Union // { arity: 2 }
  Get t0 // { arity: 2 }
  Constant <empty> // { arity: 2 }

# Union (anonymous first)
explain with=arity
Union
  Constant <empty> // { types: "(bigint, bigint)" }
  Get t0
----
Union // { arity: 2 }
  Constant <empty> // { arity: 2 }
  Get t0 // { arity: 2 }

# Join
explain with=arity
Join on=(#1 = #2)
  Get t0
  Get t0
----
Join on=(#1 = #2) // { arity: 4 }
  Get t0 // { arity: 2 }
  Get t0 // { arity: 2 }
