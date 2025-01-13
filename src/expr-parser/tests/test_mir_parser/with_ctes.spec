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

# One CTE
roundtrip
With
  cte l0 =
    Constant <empty> // { types: "(boolean, boolean)" }
Return
  Filter #0 AND #1
    Get l0
----
roundtrip OK

# Many CTEs
roundtrip
With
  cte l0 =
    Constant <empty> // { types: "(boolean, boolean?)" }
  cte l1 =
    Filter #0 AND #1
      Constant <empty> // { types: "(boolean?, boolean)" }
  cte l2 =
    Filter (NOT(#0) OR NOT(#1))
      Constant <empty> // { types: "(boolean?, boolean)" }
Return
  Union
    Get l0
    Get l2
    Get l1
----
roundtrip OK

# With mutually recursive (simple).
roundtrip
With Mutually Recursive
  cte l0 = // { types: "(boolean?, boolean)" }
    Get l1
  cte l1 = // { types: "(boolean, boolean?)" }
    Get l0
Return
  Filter #0 AND #1
    Get l0
----
roundtrip OK

# With mutually recursive (nested).
roundtrip
With Mutually Recursive
  cte l1 = // { types: "(bigint, bigint)" }
    With Mutually Recursive
      cte l0 = // { types: "(bigint, bigint)" }
        Union
          Get l0
          Filter (#0 > 42)
            Get t0
    Return
      Union
        Get l1
        Filter (#0 > 1)
          Get l0
Return
  Map ("return_outer")
    Get l1
----
roundtrip OK
