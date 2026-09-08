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
  - c0: integer?
  - c1: bigint?
----
Source defined as t0

# Regression test for materialize#5520
apply pipeline=NonNullRequirements
Filter (#0 = #3)
  FlatMap generate_series_i32(#1)
    Map (null::integer)
      Get x
----
Filter (#0 = #3)
  FlatMap generate_series(#1)
    Map (null)
      Get x
