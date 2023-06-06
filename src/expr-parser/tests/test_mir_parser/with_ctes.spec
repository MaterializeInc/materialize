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

# One CTE
roundtrip
Return
  Filter #0 AND #1
    Get l0
With
  cte l0 =
    Constant <empty> // { types: "(boolean, boolean)" }
----
roundtrip OK

# Many CTEs
roundtrip
Return
  Union
    Get l0
    Get l2
    Get l1
With
  cte l2 =
    Filter (NOT(#0) OR NOT(#1))
      Constant <empty> // { types: "(boolean?, boolean)" }
  cte l1 =
    Filter #0 AND #1
      Constant <empty> // { types: "(boolean?, boolean)" }
  cte l0 =
    Constant <empty> // { types: "(boolean, boolean?)" }
----
roundtrip OK

# With mutually recursive (simple).
roundtrip
Return
  Filter #0 AND #1
    Get l0
With Mutually Recursive
  cte l0 = // { types: "(boolean?, boolean)" }
    Get l1
  cte l1 = // { types: "(boolean, boolean?)" }
    Get l0
----
roundtrip OK

# With mutually recursive (nested).
roundtrip
Return
  Map ("return_inner")
    Get l1
With Mutually Recursive
  cte l1 = // { types: "(bigint, bigint)" }
    Return
      Union
        Get l1
        Filter (#0 > 1)
          Get l0
    With Mutually Recursive
      cte l0 = // { types: "(bigint, bigint)" }
        Union
          Get l0
          Filter (#0 > 42)
            Get t0
----
roundtrip OK
