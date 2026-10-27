# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that the test runner can properly construct sources with keys
# and report on key information in plans

define
DefSource name=x
  - c0: integer?
  - c1: bigint?
  - c2: integer?
----
Source defined as t0

define
DefSource name=y
  - c0: bigint?
  - c1: integer?
  - c2: integer?
----
Source defined as t1

explain with=(types, keys)
Union
  Get x
  Get x
  Get x
----
Union // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }

explain with=(types, keys)
Union
  Get x
  Project (#1, #0, #2)
    Get y
----
Union // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Project (#1, #0, #2) // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
    Get y // { types: "(r_int64?, r_int32?, r_int32?)", keys: "()" }

explain with=(types, keys)
Union
  Project (#1, #0, #2)
    Get y
  Get x
----
Union // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Project (#1, #0, #2) // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
    Get y // { types: "(r_int64?, r_int32?, r_int32?)", keys: "()" }
  Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }

explain with=(types, keys)
With
  cte l0 =
    Project (#1, #0, #2)
      Get y
Return
  Union
    Get x
    Get l0
----
With
  cte l0 =
    Project (#1, #0, #2) // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
      Get y // { types: "(r_int64?, r_int32?, r_int32?)", keys: "()" }
Return // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Union // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
    Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
    Get l0 // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }

explain with=(types, keys)
With
  cte l0 =
    Project (#1, #0, #2)
      Get y
Return
  Union
    Get l0
    Get l0
----
With
  cte l0 =
    Project (#1, #0, #2) // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
      Get y // { types: "(r_int64?, r_int32?, r_int32?)", keys: "()" }
Return // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Union // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
    Get l0 // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
    Get l0 // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }

explain with=(types, keys)
CrossJoin
  Get x
  Get y
----
CrossJoin // { types: "(r_int32?, r_int64?, r_int32?, r_int64?, r_int32?, r_int32?)", keys: "()" }
  Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
  Get y // { types: "(r_int64?, r_int32?, r_int32?)", keys: "()" }

# Key information propagation through Filters

explain with=(types, keys)
Filter (#0 = #1)
  Distinct project=[#0, #2]
    Get x
----
Filter (#0 = #1) // { types: "(r_int32, r_int32)", keys: "([0], [1])" }
  Distinct project=[#0, #2] // { types: "(r_int32?, r_int32?)", keys: "([0, 1])" }
    Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }

define
DefSource name=with_keys keys=[[#0, #1], [#1, #2]]
  - c0: integer?
  - c1: integer?
  - c2: integer?
----
Source defined as t2

explain with=(types, keys)
Filter (#0 = #2)
  Get with_keys
----
Filter (#0 = #2) // { types: "(r_int32, r_int32?, r_int32)", keys: "([0, 1], [1, 2])" }
  Get with_keys // { types: "(r_int32?, r_int32?, r_int32?)", keys: "([0, 1], [1, 2])" }

define
DefSource name=with_keys2 keys=[[#0, #1], [#2, #3]]
  - c0: integer?
  - c1: integer?
  - c2: integer?
  - c3: integer?
----
Source defined as t3

explain with=(types, keys)
Filter (#0 = #2)
  Get with_keys2
----
Filter (#0 = #2) // { types: "(r_int32, r_int32?, r_int32, r_int32?)", keys: "([0, 1], [0, 3], [1, 2], [2, 3])" }
  Get with_keys2 // { types: "(r_int32?, r_int32?, r_int32?, r_int32?)", keys: "([0, 1], [2, 3])" }

# Regression test for materialize#14146. The keys at the end should be [#0]

explain with=(types, keys)
Filter (#0 = #0)
  Reduce group_by=[#0] aggregates=[count(*)]
    Get x
----
Filter (#0 = #0) // { types: "(r_int32, r_int64)", keys: "([0])" }
  Reduce group_by=[#0] aggregates=[count(*)] // { types: "(r_int32?, r_int64)", keys: "([0])" }
    Get x // { types: "(r_int32?, r_int64?, r_int32?)", keys: "()" }
