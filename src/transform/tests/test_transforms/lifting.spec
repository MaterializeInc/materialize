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

define
DefSource name=y
  - c0: bigint?
  - c1: bigint?
----
Source defined as t1

# Discard literals that are not projected.
apply pipeline=literal_lifting
Project (#5, #3)
  Map (1, 2, 3, 4)
    Get x
----
Project (#3, #2)
  Map (2, 4)
    Get x

apply pipeline=identity
Project (#3, #3)
  Map (1, 2, 3)
    Get x
----
Project (#3, #3)
  Map (1, 2, 3)
    Get x

apply pipeline=literal_lifting
Project (#3, #3)
  Map (1, 2, 3)
    Get x
----
Project (#2, #2)
  Map (2)
    Get x

apply pipeline=literal_lifting
Project (#3, #4, #3)
  Map (1, 2, 3)
    Get x
----
Project (#2, #3, #2)
  Map (2, 3)
    Get x

# Merge nested Map operators within a Project
apply pipeline=literal_lifting
Project (#2, #3)
  Map (2)
    Map (1)
      Get x
----
Project (#2, #3)
  Map (1, 2)
    Get x

# Map: Permute columns to put literals at the end
apply pipeline=literal_lifting
Project (#3, #6)
  Map (3, #2, 4)
    Map (1, #0, 2)
      Get x
----
Project (#3, #6)
  Project (#0..=#3, #5, #6, #4)
    Map (#2, 2, 3)
      Project (#0, #1, #3, #2)
        Map (#0, 1)
          Get x


apply pipeline=(literal_lifting,projection_lifting,literal_lifting)
Project (#3, #6)
  Map (3, #2, 4)
    Map (1, #0, 2)
      Get x
----
Project (#2, #4)
  Project (#0..=#2, #4, #3)
    Map (#0, 1, 1)
      Get x

apply pipeline=optimize
Project (#3, #6)
  Map (3, #2, 4)
    Map (1, #0, 2)
      Get x
----
Project (#0, #2)
  Map (1)
    Get x

# Extract common values in all rows in Constant operator
apply pipeline=identity
Constant // { types: "(bigint?, bigint?, bigint?)" }
  - (1, 2, 3)
  - (1, 4, 3)
----
Constant
  - (1, 2, 3)
  - (1, 4, 3)

apply pipeline=literal_lifting
Constant // { types: "(bigint?, bigint?, bigint?)" }
  - (1, 2, 3)
  - (1, 4, 3)
----
Map (3)
  Project (#1, #0)
    Map (1)
      Constant
        - (2)
        - (4)

apply pipeline=literal_lifting
Union
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (2, 4, 3)
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (3, 2, 3)
    - (4, 4, 3)
----
Map (3)
  Union
    Constant
      - (1, 2)
      - (2, 4)
    Constant
      - (3, 2)
      - (4, 4)

apply pipeline=literal_lifting
Union
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (1, 4, 3)
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (1, 4, 3)
----
Map (3)
  Union
    Project (#1, #0)
      Map (1)
        Constant
          - (2)
          - (4)
    Project (#1, #0)
      Map (1)
        Constant
          - (2)
          - (4)

apply pipeline=(literal_lifting,projection_lifting,literal_lifting)
Union
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (1, 4, 3)
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (1, 4, 3)
----
Project (#1, #0, #2)
  Map (1, 3)
    Union
      Constant
        - (2)
        - (4)
      Constant
        - (2)
        - (4)

apply pipeline=literal_lifting
Union
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (1, 4, 3)
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (2, 2, 3)
    - (2, 4, 3)
----
Map (3)
  Union
    Project (#1, #0)
      Map (1)
        Constant
          - (2)
          - (4)
    Project (#1, #0)
      Map (2)
        Constant
          - (2)
          - (4)

apply pipeline=(literal_lifting,projection_lifting,literal_lifting)
Union
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (1, 4, 3)
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (2, 2, 3)
    - (2, 4, 3)
----
Project (#1, #0, #2)
  Map (3)
    Union
      Map (1)
        Constant
          - (2)
          - (4)
      Map (2)
        Constant
          - (2)
          - (4)

apply pipeline=literal_lifting
Union
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (2, 2, 3)
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (4, 3, 3)
    - (4, 5, 3)
----
Map (3)
  Union
    Map (2)
      Constant
        - (1)
        - (2)
    Project (#1, #0)
      Map (4)
        Constant
          - (3)
          - (5)

apply pipeline=(literal_lifting,projection_lifting,literal_lifting)
Union
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (1, 2, 3)
    - (2, 2, 3)
  Constant // { types: "(bigint?, bigint?, bigint?)" }
    - (4, 3, 3)
    - (4, 5, 3)
----
Map (3)
  Union
    Map (2)
      Constant
        - (1)
        - (2)
    Project (#1, #0)
      Map (4)
        Constant
          - (3)
          - (5)

# Union: literals in the suffix in all branches are lifted...
apply pipeline=literal_lifting
Union
  Project (#0, #3, #2)
    Map (2, 1)
      Get x
  Project (#0, #2, #3)
    Map (1, 2)
      Get x
----
Union
  Project (#0, #3, #2)
    Map (2, 1)
      Get x
  Project (#0, #2, #3)
    Map (1, 2)
      Get x

# .. but other common literals are not lifted by LiteralLifting...
apply pipeline=literal_lifting
Union
  Project (#2, #0)
    Map (1)
      Get x
  Project (#2, #0)
    Map (1)
      Get x
----
Union
  Project (#2, #0)
    Map (1)
      Get x
  Project (#2, #0)
    Map (1)
      Get x

# ... however, they eventually get lifted as a result of the following transformations
apply pipeline=(projection_lifting,literal_lifting)
Union
  Project (#2, #0)
    Map (1)
      Get x
  Project (#2, #0)
    Map (1)
      Get x
----
Project (#2, #0)
  Map (1)
    Union
      Get x
      Get x

apply pipeline=optimize
Union
  Project (#2, #0)
    Map (1)
      Get y
  Project (#2, #1)
    Map (1)
      Get y
----
With
  cte l0 =
    Map (1)
      Get y
Return
  Union
    Project (#2, #0)
      Get l0
    Project (#2, #1)
      Get l0

apply pipeline=literal_lifting
Constant // { types: "(bigint?, bigint?, bigint?)", keys: "([1, 2])" }
  - (1, 2, 3)
  - (1, 4, 3)
----
Map (3)
  Project (#1, #0)
    Map (1)
      Constant
        - (2)
        - (4)
