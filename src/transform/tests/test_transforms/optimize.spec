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

define
DefSource name=t0 keys=[[#0]]
  - c0: bigint
  - c1: bigint?
----
Source defined as t0

# The `optimize` pipeline applies the full optimizer.
apply pipeline=optimize
Filter #0 = 1 AND #1 = 2
  Get t0
----
Filter (#0 = 1) AND (#1 = 2)
  Get t0

# A `pipeline` list applies its transforms in sequence.
apply pipeline=(fusion_join,fold_constants)
CrossJoin
  CrossJoin
    Get t0
    Get t0
  Constant // { types: "(bigint)" }
    - (1)
----
CrossJoin
  Get t0
  Get t0
  Constant
    - (1)

# Flag-gated transform behavior is enabled via directive args.
apply pipeline=will_distinct enable_will_distinct_propagation=true
Distinct project=[#0]
  Project (#0)
    Get t0
----
Distinct project=[#0]
  Project (#0)
    Get t0
