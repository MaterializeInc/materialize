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

# both have limit and offset

apply pipeline=fusion
TopK limit=1 offset=1
  TopK limit=3 offset=2
    Get x
----
TopK limit=1 offset=3
  Get x

apply pipeline=fusion
TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=1 offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x
----
TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=1 offset=3
  Get x

# outer limit is greater than inner limit plus outer offset

apply pipeline=fusion
TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=10
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x
----
TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
  Get x

apply pipeline=fusion
TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=10 offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x
----
TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=2 offset=3
  Get x

# outer offset is equal to inner limit

apply pipeline=fusion
TopK limit=1 offset=3
  TopK limit=3 offset=2
    Get x
----
Constant <empty>

# outer offset is greater than the inner offset

apply pipeline=fusion
TopK offset=4
  TopK limit=3
    Get x
----
Constant <empty>

# inner has no limit, but both have offset

apply pipeline=fusion
TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=10 offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] offset=2
    Get x
----
TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=10 offset=3
  Get x

# both have no limit, but offset

apply pipeline=fusion
TopK group_by=[#0] order_by=[#0 asc nulls_first] offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] offset=2
    Get x
----
TopK group_by=[#0] order_by=[#0 asc nulls_first] offset=3
  Get x

# outer has no limit, but both have offset

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first] offset=1
  TopK order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x
----
TopK order_by=[#0 asc nulls_first] limit=2 offset=3
  Get x

# outer has no limit and no offset

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first]
  TopK order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x
----
TopK order_by=[#0 asc nulls_first] limit=3 offset=2
  Get x

# inner has no limit and no offset

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first] limit=3 offset=2
  TopK order_by=[#0 asc nulls_first]
    Get x
----
TopK order_by=[#0 asc nulls_first] limit=3 offset=2
  Get x

# inner has no limit and no offset, and outer has only limit

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first] limit=3
  TopK order_by=[#0 asc nulls_first]
    Get x
----
TopK order_by=[#0 asc nulls_first] limit=3
  Get x

# inner has no limit and no offset, and outer has only offset

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first] offset=1
  TopK order_by=[#0 asc nulls_first]
    Get x
----
TopK order_by=[#0 asc nulls_first] offset=1
  Get x

# both have no limit and no offset

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first]
  TopK order_by=[#0 asc nulls_first]
    Get x
----
TopK order_by=[#0 asc nulls_first]
  Get x

# both have limit 0 and no offset

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first] limit=0
  TopK order_by=[#0 asc nulls_first] limit=0
    Get x
----
Constant <empty>

# outer has limit 0

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first] limit=0
  TopK order_by=[#0 asc nulls_first]
    Get x
----
Constant <empty>

# inner has limit 0

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first]
  TopK order_by=[#0 asc nulls_first] limit=0
    Get x
----
Constant <empty>

apply pipeline=identity
TopK limit=1 offset=1
  TopK limit=3 offset=2
    Constant // { types: "(integer?)" }
      - (5)
      - (4)
      - (2)
      - (3)
      - (2)
      - (1)
----
TopK limit=1 offset=1
  TopK limit=3 offset=2
    Constant
      - (5)
      - (4)
      - (2)
      - (3)
      - (2)
      - (1)

apply pipeline=fusion
TopK limit=1 offset=1
  TopK limit=3 offset=2
    Constant // { types: "(integer?)" }
      - (5)
      - (4)
      - (2)
      - (3)
      - (2)
      - (1)
----
TopK limit=1 offset=3
  Constant
    - (5)
    - (4)
    - (2)
    - (3)
    - (2)
    - (1)

apply pipeline=optimize
TopK limit=1 offset=1
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
  - (3)


apply pipeline=fusion
TopK limit=1 offset=3
  TopK limit=3 offset=2
    Constant // { types: "(integer?)" }
      - (5)
      - (4)
      - (2)
      - (3)
      - (2)
      - (1)
----
Constant <empty>

apply pipeline=fusion
TopK order_by=[#0 asc nulls_first] limit=1
  TopK order_by=[#1 asc nulls_first] limit=3 offset=2
    Constant // { types: "(integer?, integer?)" }
      - (5, 4)
      - (3, 2)
      - (1, 0)
----
TopK order_by=[#0 asc nulls_first] limit=1
  TopK order_by=[#1 asc nulls_first] limit=3 offset=2
    Constant
      - (5, 4)
      - (3, 2)
      - (1, 0)

apply pipeline=fusion
TopK order_by=[#1 asc nulls_first] limit=1
  TopK order_by=[#1 asc nulls_first] limit=3 offset=2
    Constant // { types: "(integer?, integer?)" }
      - (5, 4)
      - (3, 2)
      - (1, 0)
----
TopK order_by=[#1 asc nulls_first] limit=1 offset=2
  Constant
    - (5, 4)
    - (3, 2)
    - (1, 0)

apply pipeline=fusion
TopK group_by=[#1] order_by=[#1 asc nulls_first] limit=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Constant // { types: "(integer?, integer?)" }
      - (5, 4)
      - (3, 2)
      - (1, 0)
----
TopK group_by=[#1] order_by=[#1 asc nulls_first] limit=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Constant
      - (5, 4)
      - (3, 2)
      - (1, 0)

apply pipeline=fusion
TopK group_by=[#0] order_by=[#1 asc nulls_first] limit=1
  TopK group_by=[#0] limit=3 offset=2
    Constant // { types: "(integer?, integer?)" }
      - (5, 4)
      - (3, 2)
      - (1, 0)
----
TopK group_by=[#0] order_by=[#1 asc nulls_first] limit=1
  TopK group_by=[#0] limit=3 offset=2
    Constant
      - (5, 4)
      - (3, 2)
      - (1, 0)

# Fusionable TopK operators with grouping key

apply pipeline=fusion
TopK group_by=[#0] limit=1
  TopK group_by=[#0] limit=3 offset=1
    Constant // { types: "(integer?, integer?)" }
      - (5, 4)
      - (3, 2)
      - (1, 0)
      - (1, 1)
----
TopK group_by=[#0] limit=1 offset=1
  Constant
    - (5, 4)
    - (3, 2)
    - (1, 0)
    - (1, 1)

apply pipeline=fold_constants
TopK group_by=[#0] limit=1 offset=1
  Constant // { types: "(integer?, integer?)" }
    - (5, 4)
    - (3, 2)
    - (1, 0)
    - (1, 1)
----
Constant
  - (1, 1)

apply pipeline=fold_constants
TopK group_by=[#0] limit=1
  TopK group_by=[#0] limit=3 offset=1
    Constant // { types: "(integer?, integer?)" }
      - (5, 4)
      - (3, 2)
      - (1, 0)
      - (1, 1)
----
Constant
  - (1, 1)

# Both nulls_last

apply pipeline=fusion
TopK group_by=[#0] order_by=[#0 asc nulls_last] limit=1 offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_last] limit=3 offset=2
    Get x
----
TopK group_by=[#0] order_by=[#0 asc nulls_last] limit=1 offset=3
  Get x

# Cannot be fused, because nulls_last differs

apply pipeline=fusion
TopK group_by=[#0] order_by=[#0 asc nulls_last] limit=1 offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x
----
TopK group_by=[#0] order_by=[#0 asc nulls_last] limit=1 offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x

# Cannot be fused, because asc-desc differs

apply pipeline=fusion
TopK group_by=[#0] order_by=[#0 desc nulls_first] limit=1 offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x
----
TopK group_by=[#0] order_by=[#0 desc nulls_first] limit=1 offset=1
  TopK group_by=[#0] order_by=[#0 asc nulls_first] limit=3 offset=2
    Get x
