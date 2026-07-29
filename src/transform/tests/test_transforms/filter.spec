# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

define
DefSource name=x
  - c0: bigint?
  - c1: bigint?
----
Source defined as t0

# Redundant IS NOT NULL predicate

apply pipeline=fusion
Filter not(((#0) IS NULL)) AND (#0 = 1)
  Get x
----
Filter (#0 = 1)
  Get x

apply pipeline=fusion
Filter not(((#1) IS NULL)) AND (#0 = #1)
  Get x
----
Filter (#0 = #1)
  Get x

apply pipeline=fusion
Filter (#0 = 1)
  Filter not(((#0) IS NULL))
    Get x
----
Filter (#0 = 1)
  Get x

# Impossible condition detection

apply pipeline=fusion
Filter (#0 = 1)
  Filter ((#0) IS NULL)
    Get x
----
Filter false
  Get x

apply pipeline=(fusion,fold_constants)
Filter (#0 = 1)
  Filter ((#0) IS NULL)
    Get x
----
Constant <empty>

apply pipeline=fusion
Filter (#0 = #1)
  Filter ((#1) IS NULL)
    Get x
----
Filter false
  Get x

apply pipeline=fusion
Filter not(((#0) IS NULL))
  Filter ((#0) IS NULL)
    Get x
----
Filter false
  Get x
