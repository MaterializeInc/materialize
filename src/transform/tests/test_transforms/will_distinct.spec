# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Focused regression tests for the WillDistinct transform's generalized
# propagation, gated behind `enable_will_distinct_propagation`. Each test plants
# an inner `Distinct` (a Reduce with no aggregates) that is shadowed by an outer
# `Distinct` through exactly one intermediate operator. When propagation reaches
# the inner Distinct it is rewritten away into a Map+Project, so the presence or
# absence of an inner `Reduce`/`Distinct` node in the output tells us whether the
# arm fired.

define
DefSource name=x keys=[[#0]]
  - c0: bigint?
  - c1: bigint?
----
Source defined as t0

# ---------------------------------------------------------------------------
# TopK, limit 1, offset 0: masks magnitudes like a Distinct, so the inner
# Distinct is redundant and is removed when the flag is on.
# ---------------------------------------------------------------------------
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0, #1]
  TopK order_by=[#0 asc nulls_first] limit=1
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0, #1]
  TopK order_by=[#0 asc nulls_first] limit=1
    Project (#2, #3)
      Map (#0, #1)
        Get x

# NEGATIVE (locks in the #1 fix): TopK limit 1 OFFSET 5. Which row survives
# depends on the cumulative multiplicities of the skipped rows, so the inner
# Distinct must NOT be removed even with the flag on.
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0, #1]
  TopK order_by=[#0 asc nulls_first] limit=1 offset=5
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0, #1]
  TopK order_by=[#0 asc nulls_first] limit=1 offset=5
    Distinct project=[#0, #1]
      Get x

# CONTROL: with the flag OFF, the generalized TopK arm is inert and the inner
# Distinct survives even for limit 1 offset 0 (reproduces historic behavior).
apply pipeline=WillDistinct
Distinct project=[#0, #1]
  TopK order_by=[#0 asc nulls_first] limit=1
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0, #1]
  TopK order_by=[#0 asc nulls_first] limit=1
    Distinct project=[#0, #1]
      Get x

# ---------------------------------------------------------------------------
# Sign-preserving unary operators: inner Distinct removed when flag is on.
# ---------------------------------------------------------------------------

# Map
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0, #1, #2]
  Map (add_int64(#0, #1))
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0..=#2]
  Map ((#0 + #1))
    Project (#2, #3)
      Map (#0, #1)
        Get x

# Filter
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0, #1]
  Filter (#0 > #1)
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0, #1]
  Filter (#0 > #1)
    Project (#2, #3)
      Map (#0, #1)
        Get x

# Negate
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0, #1]
  Negate
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0, #1]
  Negate
    Project (#2, #3)
      Map (#0, #1)
        Get x

# Threshold
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0, #1]
  Threshold
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0, #1]
  Threshold
    Project (#2, #3)
      Map (#0, #1)
        Get x

# FlatMap: retains input columns, so distinct input rows never merge; the
# table-function count is non-negative, so the inner Distinct is removed.
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0, #1]
  FlatMap generate_series(#0, #1, 1)
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0, #1]
  FlatMap generate_series(#0, #1, 1)
    Project (#2, #3)
      Map (#0, #1)
        Get x

# NEGATIVE: TopK whose limit is a non-literal expression (a column reference) is
# not recognized as a magnitude mask, so the inner Distinct is preserved.
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0, #1]
  TopK order_by=[#0 asc nulls_first] limit=#0
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0, #1]
  TopK order_by=[#0 asc nulls_first] limit=#0
    Distinct project=[#0, #1]
      Get x

# ---------------------------------------------------------------------------
# Project requires a non-negative input. Over a plain (non-negative) Distinct
# the inner Distinct is removed; under a Negate (negative) it is preserved
# because two rows could collapse to one with cancelling signs.
# ---------------------------------------------------------------------------

# Non-negative Project: removed.
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0]
  Project (#0)
    Distinct project=[#0, #1]
      Get x
----
Distinct project=[#0]
  Project (#0)
    Project (#2, #3)
      Map (#0, #1)
        Get x

# Project over a Negate (negative input): preserved.
apply pipeline=WillDistinct enable_will_distinct_propagation=true
Distinct project=[#0]
  Project (#0)
    Negate
      Distinct project=[#0, #1]
        Get x
----
Distinct project=[#0]
  Project (#0)
    Negate
      Distinct project=[#0, #1]
        Get x
