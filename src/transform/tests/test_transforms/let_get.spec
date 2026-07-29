# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# test that predicates in the body of a let get pushed down to values
apply pipeline=predicate_pushdown
With
  cte l0 =
    Constant // { types: "(bigint?, bigint?, bigint?)" }
      - (1, 2, 3)
      - (4, 5, 6)
Return
  Filter (#0 = 5::integer)
    Get l0
----
With
  cte l0 =
    Filter (#0 = 5)
      Constant
        - (1, 2, 3)
        - (4, 5, 6)
Return
  Filter
    Get l0
