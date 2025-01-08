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
  - c1: smallint
----
Source defined as t0

# Constant
explain with=types
Constant // { types: "(bigint, smallint)" }
  - (1, 2)
  - (1, 3)
----
Constant // { types: "(Int64, Int16)" }
  - (1, 2)
  - (1, 3)

# Global Get
explain with=types
Get t0
----
Get t0 // { types: "(Int64, Int16)" }

# Local Get in a Let block
explain with=types
Return
  Get l0
With
  cte l0 =
    Get t0
----
With
  cte l0 =
    Get t0 // { types: "(Int64, Int16)" }
Return // { types: "(Int64, Int16)" }
  Get l0 // { types: "(Int64, Int16)" }

# Local Get in a LetRec block
explain with=types
Return
  Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, smallint)" }
    Get t0
----
With Mutually Recursive
  cte l0 =
    Get t0 // { types: "(Int64, Int16)" }
Return // { types: "(Int64, Int16)" }
  Get l0 // { types: "(Int64, Int16)" }

# Project
explain with=types
Project (#1, #0, #1)
  Get t0
----
Project (#1, #0, #1) // { types: "(Int16, Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }

# Map
explain with=types
Map (#1, #0 + #1, #0)
  Get t0
----
Map (#1, (#0 + #1), #0) // { types: "(Int64, Int16, Int16, Int64, Int64)" }
  Get t0 // { types: "(Int64, Int16)" }

# FlatMap
explain with=types
FlatMap generate_series(#0, #1, 1)
  Get t0
----
FlatMap generate_series(#0, #1, 1) // { types: "(Int64, Int16, Int64)" }
  Get t0 // { types: "(Int64, Int16)" }

# Filter
explain with=types
Filter (#0 > #1 + 42)
  Get t0
----
Filter (#0 > (#1 + 42)) // { types: "(Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }

# Reduce
explain with=types
Reduce group_by=[#0] aggregates=[min(#1), max(#1)]
  Get t0
----
Reduce group_by=[#0] aggregates=[min(#1), max(#1)] // { types: "(Int64, Int16, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }

# TopK
explain with=types
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3
  Get t0
----
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3 // { types: "(Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }

# Negate
explain with=types
Negate
  Get t0
----
Negate // { types: "(Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }

# Threshold
explain with=types
Threshold
  Get t0
----
Threshold // { types: "(Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }

# ArrangeBy
explain with=types
ArrangeBy keys=[[#1], [#0, #1]]
  Get t0
----
ArrangeBy keys=[[#1], [#0, #1]] // { types: "(Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }

# Union (anonymous last)
explain with=types
Union
  Get t0
  Constant <empty> // { types: "(bigint, smallint)" }
----
Union // { types: "(Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }
  Constant <empty> // { types: "(Int64, Int16)" }

# Union (anonymous first)
explain with=types
Union
  Constant <empty> // { types: "(bigint, smallint)" }
  Get t0
----
Union // { types: "(Int64, Int16)" }
  Constant <empty> // { types: "(Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }

# Join
explain with=types
Join on=(#1 = #2)
  Get t0
  Get t0
----
Join on=(#1 = #2) // { types: "(Int64, Int16, Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }
  Get t0 // { types: "(Int64, Int16)" }
