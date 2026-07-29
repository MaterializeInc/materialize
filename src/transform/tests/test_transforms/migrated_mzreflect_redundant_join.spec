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

define
DefSource name=y keys=[[#0]]
  - c0: bigint?
  - c1: bigint?
----
Source defined as t1

apply pipeline=RedundantJoin
Join on=(#0 = #1)
  Distinct project=[#0]
    Get x
  Get x
----
Project (#2, #0, #1)
  Map (#0)
    CrossJoin
      Get x

apply pipeline=RedundantJoin
Join on=(#0 = #2)
  Get x
  Distinct project=[#0]
    Get x
----
Project (#0..=#2)
  Map (#0)
    CrossJoin
      Get x

# self-join on primary key

apply pipeline=RedundantJoin
Join on=(#0 = #2)
  Get y
  Get y
----
Project (#0..=#3)
  Map (#0, #1)
    CrossJoin
      Get y

# Expressions that can be built from the other projection.

apply pipeline=RedundantJoin
Join on=(#0 = #6)
  Map (#0, add_int64(#0, 1), ((#0) IS NULL), record_create["f1"](#0), case when (#0 = 0) then 1 else 2 end)
    Distinct project=[#0]
      Get x
  Get x
----
Project (#2..=#7, #0, #1)
  Map (#0, #0, (#0 + 1), (#0) IS NULL, row(#0), case when (#0 = 0) then 1 else 2 end)
    CrossJoin
      Get x

apply pipeline=RedundantJoin
Join on=(#0 = #4)
  Map (((#0) IS NULL))
    Map (add_int64(#0, 1))
      Map (#0)
        Distinct project=[#0]
          Get x
  Get x
----
Project (#2..=#5, #0, #1)
  Map (#0, #0, (#0 + 1), (#0) IS NULL)
    CrossJoin
      Get x

apply pipeline=RedundantJoin
Join on=(#3 = #4)
  Project (#3, #2, #1, #0)
    Map (((#0) IS NULL))
      Map (add_int64(#0, 1))
        Map (#0)
          Distinct project=[#0]
            Get x
  Get x
----
Project (#2..=#5, #0, #1)
  Map ((#0) IS NULL, (#0 + 1), #0, #0)
    CrossJoin
      Get x

apply pipeline=RedundantJoin
Join on=(#0 = #1)
  Project (#2)
    Map (add_int64(#0, 1))
      Get x
  Distinct project=[add_int64(#0, 1)]
    Get x
----
Project (#0, #1)
  Map (#0)
    CrossJoin
      Project (#2)
        Map ((#0 + 1))
          Get x

apply pipeline=RedundantJoin
Join on=(#0 = #1)
  Union
    Project (#2)
      Map (add_int64(#0, 1))
        Get x
    Project (#2)
      Map (add_int64(#0, 1))
        Get x
  Distinct project=[add_int64(#0, 1)]
    Get x
----
Project (#0, #1)
  Map (#0)
    CrossJoin
      Union
        Project (#2)
          Map ((#0 + 1))
            Get x
        Project (#2)
          Map ((#0 + 1))
            Get x

# different dereferenced projection in union branches

apply pipeline=RedundantJoin
Join on=(#0 = #1)
  Union
    Project (#2)
      Map (add_int64(#0, 1))
        Get x
    Project (#2)
      Map (add_int64(#0, 2))
        Get x
  Distinct project=[add_int64(#0, 1)]
    Get x
----
Join on=(#0 = #1)
  Union
    Project (#2)
      Map ((#0 + 1))
        Get x
    Project (#2)
      Map ((#0 + 2))
        Get x
  Distinct project=[(#0 + 1)]
    Get x

# We can't remove the join unless the literal is lifted

apply pipeline=RedundantJoin
Join on=(#0 = #2)
  Map (1)
    Distinct project=[#0]
      Get x
  Get x
----
Join on=(#0 = #2)
  Map (1)
    Distinct project=[#0]
      Get x
  Get x

apply pipeline=(LiteralLifting,RedundantJoin)
Join on=(#0 = #2)
  Map (1)
    Distinct project=[#0]
      Get x
  Get x
----
Project (#0, #3, #1, #2)
  Map (1)
    Project (#2, #0, #1)
      Map (#0)
        CrossJoin
          Get x
