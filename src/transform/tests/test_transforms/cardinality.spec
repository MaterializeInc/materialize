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
  - c0: bigint
  - c1: bigint
  - c2: bigint
----
Source defined as t0


# A join on columns that are not unique on either side multiplies the input
# estimates.
explain with=cardinality stats=(t0-1000)
Join on=(#1 = #4)
  Get t0
  Filter #0 = #2
    Get t0
----
Join on=(#1 = #4) // { cardinality: "100000" }
  Get t0 // { cardinality: "1000" }
  Filter (#0 = #2) // { cardinality: "100" }
    Get t0 // { cardinality: "1000" }

# A join on a unique key column of one side is bounded by the other side.
explain with=cardinality stats=(t0-1000)
Join on=(#1 = #3)
  Get t0
  Filter #0 = #2
    Get t0
----
Join on=(#1 = #3) // { cardinality: "1000" }
  Get t0 // { cardinality: "1000" }
  Filter (#0 = #2) // { cardinality: "100" }
    Get t0 // { cardinality: "1000" }

# A join on a column *equated* to a unique key column gets the same bound: the
# rhs filter makes #5 equivalent to the key column #3, and the estimator
# widens unique columns through the equivalence class.
explain with=cardinality stats=(t0-1000)
Join on=(#1 = #5)
  Get t0
  Filter #0 = #2
    Get t0
----
Join on=(#1 = #5) // { cardinality: "1000" }
  Get t0 // { cardinality: "1000" }
  Filter (#0 = #2) // { cardinality: "100" }
    Get t0 // { cardinality: "1000" }
