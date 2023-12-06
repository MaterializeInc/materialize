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


# Positive tests
# --------------

# inner_limit - outer_offset < outer_limit
# resulting limit is outer_limit
apply pipeline=fusion_top_k
TopK limit=4 offset=5
  TopK limit=7 offset=3
    Get t0
----
TopK limit=2 offset=8
  Get t0

# inner_limit - outer_offset > outer_limit
# resulting limit is outer_limit
apply pipeline=fusion_top_k
TopK limit=4 offset=2
  TopK limit=7 offset=3
    Get t0
----
TopK limit=4 offset=5
  Get t0

# Reduce to `Constant <empty>` when the resulting limit is zero.
apply pipeline=fusion_top_k
TopK group_by=[#1] limit=7 offset=7
  TopK group_by=[#1] limit=6 offset=3
    Get t0
----
Constant <empty>

# Corner case: outer offset=i64::MAX
apply pipeline=fusion_top_k
TopK group_by=[#0] limit=7 offset=9223372036854775807
  TopK group_by=[#0] limit=9223372036854775807 offset=10
    Get t0
----
Constant <empty>

# Corner case: outer offset=i64::MAX - 1
apply pipeline=fusion_top_k
TopK group_by=[#0] limit=7 offset=9223372036854775806
  TopK group_by=[#0] limit=9223372036854775807 offset=10
    Get t0
----
TopK group_by=[#0] limit=1 offset=9223372036854775816
  Get t0


# Negative tests
# --------------

# Skipped: different group_by clauses
apply pipeline=fusion_top_k
TopK group_by=[#0] order_by=[#1 asc] limit=7
  TopK group_by=[#1] order_by=[#1 asc] limit=5
    Get t0
----
TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=7
  TopK group_by=[#1] order_by=[#1 asc nulls_last] limit=5
    Get t0

# Skipped: different order_by clauses
apply pipeline=fusion_top_k
TopK group_by=[#0] order_by=[#1 asc nulls_first] limit=7
  TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=5
    Get t0
----
TopK group_by=[#0] order_by=[#1 asc nulls_first] limit=7
  TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=5
    Get t0

# Skipped: inner limit is an expression
apply pipeline=fusion_top_k
TopK group_by=[#0] order_by=[#1 asc] limit=7
  TopK group_by=[#0] order_by=[#1 asc] limit=(#0+5)
    Get t0
----
TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=7
  TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=(#0 + 5)
    Get t0

# Skipped: outer limit is an expression
apply pipeline=fusion_top_k
TopK group_by=[#0] order_by=[#1 asc] limit=(#0 + 5)
  TopK group_by=[#0] order_by=[#1 asc] limit=5
    Get t0
----
TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=(#0 + 5)
  TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=5
    Get t0

# Skipped: inner limit is negative
apply pipeline=fusion_top_k
TopK group_by=[#0] order_by=[#1 asc] limit=7
  TopK group_by=[#0] order_by=[#1 asc] limit=-1
    Get t0
----
TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=7
  TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=-1
    Get t0

# Skipped: outer limit is negative
apply pipeline=fusion_top_k
TopK group_by=[#0] order_by=[#1 asc] limit=-1
  TopK group_by=[#0] order_by=[#1 asc] limit=5
    Get t0
----
TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=-1
  TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=5
    Get t0

# Skipped: outer offset cannot be cast to i64
apply pipeline=fusion_top_k
TopK group_by=[#0] order_by=[#1 asc] limit=7 offset=9223372036854775808
  TopK group_by=[#0] order_by=[#1 asc] limit=5
    Get t0
----
TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=7 offset=9223372036854775808
  TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=5
    Get t0
