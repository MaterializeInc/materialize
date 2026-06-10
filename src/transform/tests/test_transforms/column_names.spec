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

# Define t1 source
define
DefSource name=t1
  - c2: bigint
  - c3: bigint
----
Source defined as t1

# Constant
explain with=column_names
Constant // { types: "(bigint, bigint)" }
  - (1, 2)
  - (1, 3)
----
Constant // { column_names: "(#0, #1)" }
  - (1, 2)
  - (1, 3)

# Global Get
explain with=column_names
Get t0
----
Get t0 // { column_names: "(c0, c1)" }

# Local Get in a Let block
explain with=column_names
Return
  Get l0
With
  cte l0 =
    Get t0
----
With
  cte l0 =
    Get t0 // { column_names: "(c0, c1)" }
Return // { column_names: "(c0, c1)" }
  Get l0 // { column_names: "(c0, c1)" }

# Local Get in a LetRec block
explain with=column_names
Return
  Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint)" }
    Get t0
----
With Mutually Recursive
  cte l0 =
    Get t0 // { column_names: "(c0, c1)" }
Return // { column_names: "(c0, c1)" }
  Get l0 // { column_names: "(c0, c1)" }

# Project
explain with=column_names
Project (#1, #0, #1)
  Get t0
----
Project (#1, #0, #1) // { column_names: "(c1, c0, c1)" }
  Get t0 // { column_names: "(c0, c1)" }

# Map
explain with=column_names
Map (#1, #0 + #1, #0)
  Get t0
----
Map (#1, (#0 + #1), #0) // { column_names: "(c0, c1, c1, #3, c0)" }
  Get t0 // { column_names: "(c0, c1)" }

# FlatMap
explain with=column_names
FlatMap generate_series(#0, #1, 1)
  Get t0
----
FlatMap generate_series(#0, #1, 1) // { column_names: "(c0, c1, #2)" }
  Get t0 // { column_names: "(c0, c1)" }

# Filter
explain with=column_names
Filter (#0 > #1 + 42)
  Get t0
----
Filter (#0 > (#1 + 42)) // { column_names: "(c0, c1)" }
  Get t0 // { column_names: "(c0, c1)" }

# Reduce
explain with=column_names
Reduce group_by=[#0] aggregates=[min(#1), max(#1)]
  Get t0
----
Reduce group_by=[#0] aggregates=[min(#1), max(#1)] // { column_names: "(c0, min_c1, max_c1)" }
  Get t0 // { column_names: "(c0, c1)" }

# TopK
explain with=column_names
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3
  Get t0
----
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=3 // { column_names: "(c0, c1)" }
  Get t0 // { column_names: "(c0, c1)" }

# Negate
explain with=column_names
Negate
  Get t0
----
Negate // { column_names: "(c0, c1)" }
  Get t0 // { column_names: "(c0, c1)" }

# Threshold
explain with=column_names
Threshold
  Get t0
----
Threshold // { column_names: "(c0, c1)" }
  Get t0 // { column_names: "(c0, c1)" }

# ArrangeBy
explain with=column_names
ArrangeBy keys=[[#1], [#0, #1]]
  Get t0
----
ArrangeBy keys=[[#1], [#0, #1]] // { column_names: "(c0, c1)" }
  Get t0 // { column_names: "(c0, c1)" }

# Union (anonymous last)
explain with=column_names
Union
  Get t0
  Constant <empty> // { types: "(bigint, bigint)" }
----
Union // { column_names: "(c0, c1)" }
  Get t0 // { column_names: "(c0, c1)" }
  Constant <empty> // { column_names: "(#0, #1)" }

# Union (anonymous first)
explain with=column_names
Union
  Constant <empty> // { types: "(bigint, bigint)" }
  Get t0
----
Union // { column_names: "(c0, c1)" }
  Constant <empty> // { column_names: "(#0, #1)" }
  Get t0 // { column_names: "(c0, c1)" }

# Join
explain with=column_names
Join on=(#1 = #2)
  Get t0
  Get t1
----
Join on=(#1 = #2) // { column_names: "(c0, c1, c2, c3)" }
  Get t0 // { column_names: "(c0, c1)" }
  Get t1 // { column_names: "(c2, c3)" }
