# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Define a source with a set of columns.
define
DefSource name=x
  - c0: integer?
  - c1: bigint?
----
Source defined as t0

# Build builds the IR exactly as written, and performs no optimizations on it.
apply pipeline=identity
Get x
----
Get x

# Can build nested expressions.
apply pipeline=identity
Filter #0
  Get x
----
Filter #0
  Get x

apply pipeline=identity
Filter #0 AND #1
  Map (true)
    Get x
----
Filter #0 AND #1
  Map (true)
    Get x

# If the `apply` flag is passed to build with the name of a transform, that
# transform will be applied (once).
apply pipeline=predicate_pushdown
Filter #0 AND #1
  Map (true)
    Get x
----
Map (true)
  Filter #0 AND #1
    Get x

# If `opt` is used instead of `build`, the full optimizer is run on the IR.
apply pipeline=optimize
Project (#3)
  Map (#0, #1)
    Get x
----
Project (#1)
  Get x

apply pipeline=identity
Join on=(#0 = #2 AND #1 = #3)
  Get x
  Get x
----
Join on=(#0 = #2 AND #1 = #3)
  Get x
  Get x

apply pipeline=identity
Negate
  Constant // { types: "(bigint?)" }
    - (1)
----
Negate
  Constant
    - (1)
