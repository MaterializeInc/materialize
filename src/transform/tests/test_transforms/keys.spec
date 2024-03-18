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
DefSource name=t0 keys=[[#0], [#1]]
  - c0: bigint
  - c1: bigint
  - c2: bigint
----
Source defined as t0

# Define t1 source
define
DefSource name=t1 keys=[[#0, #1, #2, #3, #4]]
  - c0: bigint
  - c1: bigint
  - c2: bigint
  - c3: bigint
  - c4: bigint
----
Source defined as t1


## Supported patterns
## ------------------


# Map
explain with=keys
Map (4145)
  Get t0
----
Map (4145) // { keys: "([0], [1])" }
  Get t0 // { keys: "([0], [1])" }

# Project with equalities between key components
explain with=keys
Filter #4 = #3 AND #1 = #0
  Get t1
----
Filter (#4 = #3) AND (#1 = #0) // { keys: "([0, 2, 3], [0, 2, 4], [1, 2, 3], [1, 2, 4])" }
  Get t1 // { keys: "([0, 1, 2, 3, 4])" }

# Project with equalities between key components and
# a literal constraint on a key component
explain with=keys
Filter #0 = #1 AND #3 = 5
  Get t1
----
Filter (#0 = #1) AND (#3 = 5) // { keys: "([0, 2, 4], [1, 2, 4])" }
  Get t1 // { keys: "([0, 1, 2, 3, 4])" }


## Incomplete patterns
## -------------------

# Join which equates lhs and rhs keys
# Loses keys from the rhs (should be: ([0], [1], [3], [4]))
explain with=keys
Join on=(#0 = #3)
  Get t0
  Get t0
----
Join on=(#0 = #3) // { keys: "([0], [1])" }
  Get t0 // { keys: "([0], [1])" }
  Get t0 // { keys: "([0], [1])" }

# Join which equates lhs and rhs keys and has other predicates
# Loses keys from the rhs (should be: ([0], [1], [3], [4]))
explain with=keys
Join on=(#0 = #3 AND #2 = #5)
  Get t0
  Get t0
----
Join on=(#0 = #3 AND #2 = #5) // { keys: "([0], [1])" }
  Get t0 // { keys: "([0], [1])" }
  Get t0 // { keys: "([0], [1])" }


## Unsupported patterns
## --------------------


# Map with a monotone scalar expression
# Should be: ([0], [1], [3])
explain with=keys
Map (#0 + 7)
  Get t0
----
Map ((#0 + 7)) // { keys: "([0], [1])" }
  Get t0 // { keys: "([0], [1])" }

# Join has missing keys
# Should be: ([0], [1], [3])
explain with=keys
Join on=(#0 = #3 AND #1 = #4)
  Get t0
  Get t0
----
Join on=(#0 = #3 AND #1 = #4) // { keys: "([0], [1])" }
  Get t0 // { keys: "([0], [1])" }
  Get t0 // { keys: "([0], [1])" }
