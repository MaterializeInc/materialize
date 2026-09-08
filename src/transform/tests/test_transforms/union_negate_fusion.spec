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

apply pipeline=UnionFusion
Union
  Get x
  Union
    Get x
    Union
      Get x
      Get x
----
Union
  Get x
  Get x
  Get x
  Get x

apply pipeline=UnionNegateFusion
Union
  Get x
  Union
    Get x
    Negate
      Union
        Get x
        Get x
----
Union
  Get x
  Get x
  Negate
    Get x
  Negate
    Get x
