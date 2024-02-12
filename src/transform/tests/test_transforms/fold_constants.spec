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

# Map
apply pipeline=fold_constants
Map (#1 + 5)
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (1, 3)
----
Constant
  - (1, 2, 7)
  - (1, 3, 8)

# TODO: add more cases

# TopK with a literal limit
apply pipeline=fold_constants
TopK group_by=[#0] order_by=[#1 asc] limit=3 offset=1
  Constant // { types: "(bigint, bigint)" }
    - (1, 4)
    - (2, 4)
    - (1, 5)
    - (1, 3)
    - (1, 3)
    - (2, 6)
    - (2, 2)
    - (1, 1)
    - (1, 6)
    - (2, 5)
    - (2, 1)
    - (2, 3)
    - (1, 2)
----
Constant
  - (1, 2)
  - ((1, 3) x 2)
  - (2, 2)
  - (2, 3)
  - (2, 4)

# Negative tests
# --------------

# Skipped: TopK with a non-literal limit expression
apply pipeline=fold_constants
TopK group_by=[#0] order_by=[#1 asc] limit=#1 offset=1
  Constant // { types: "(bigint, bigint)" }
    - (1, 1)
    - (1, 2)
    - (1, 3)
----
TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=#1 offset=1
  Constant
    - (1, 1)
    - (1, 2)
    - (1, 3)

# Skipped: TopK with a negative limit expression
apply pipeline=fold_constants
TopK group_by=[#0] order_by=[#1 asc] limit=-1 offset=1
  Constant // { types: "(bigint, bigint)" }
    - (1, 1)
    - (1, 2)
    - (1, 3)
----
TopK group_by=[#0] order_by=[#1 asc nulls_last] limit=-1 offset=1
  Constant
    - (1, 1)
    - (1, 2)
    - (1, 3)
