# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

apply pipeline=optimize
TopK limit=3 offset=2
  Constant // { types: "(integer?)" }
    - (5)
    - (4)
    - (2)
    - (3)
    - (2)
    - (1)
----
Constant
  - (2)
  - (3)
  - (4)

apply pipeline=optimize
TopK limit=10
  Constant // { types: "(integer?)" }
    - (5)
    - (4)
    - (2)
    - (3)
    - (2)
    - (1)
----
Constant
  - (1)
  - ((2) x 2)
  - (3)
  - (4)
  - (5)

apply pipeline=optimize
TopK group_by=[#0] order_by=[#1 asc nulls_first] limit=2 offset=1
  Constant // { types: "(text?, integer?)" }
    - ("a", 2)
    - ("b", 1)
    - ("a", 3)
    - ("b", 3)
    - ("a", 2)
    - ("a", 3)
    - ("a", 4)
    - ("b", 3)
    - ("b", 3)
----
Constant
  - ("a", 2)
  - ("a", 3)
  - (("b", 3) x 2)

apply pipeline=optimize
TopK group_by=[#0] order_by=[#1 desc nulls_first] limit=2
  Constant // { types: "(text?, integer?)" }
    - ("a", 2)
    - ("b", 1)
    - ("a", 3)
    - ("b", 3)
    - ("a", 2)
    - ("a", 3)
    - ("a", 4)
    - ("b", 3)
    - ("b", 3)
----
Constant
  - ("a", 3)
  - ("a", 4)
  - (("b", 3) x 2)
