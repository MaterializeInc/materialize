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

# Define x source
define
DefSource name=x
  - c0: bigint
  - c1: bigint
----
Source defined as t0

# Define y source
define
DefSource name=y keys=[[#0]]
  - c0: bigint
  - c1: bigint
----
Source defined as t1


# Basic cases
# -----------

# Simple positive test
# ```sql
# (select * from x) except all (select * from x where a < 7)
# ```
apply pipeline=threshold_elision
Threshold
  Union
    Get x
    Negate
      Filter #0 < 7
        Get x
----
Union
  Get x
  Negate
    Filter (#0 < 7)
      Get x


# Simple negative test: EXCEPT ALL
# ```sql
# (select * from x) except all (select * from y where a < 7)
# ```
apply pipeline=threshold_elision
Threshold
  Union
    Get x
    Negate
      Filter #0 < 7
        Get y
----
Threshold
  Union
    Get x
    Negate
      Filter (#0 < 7)
        Get y


# Simple positive test: EXCEPT
# ```sql
# (select * from x) except (select * from x where a < 7)
# ```
apply pipeline=threshold_elision
Threshold
  Union
    Distinct project=[#0, #1]
      Get x
    Negate
      Distinct project=[#0, #1]
        Filter #0 < 7
          Get x
----
Union
  Distinct project=[#0, #1]
    Get x
  Negate
    Distinct project=[#0, #1]
      Filter (#0 < 7)
        Get x


# Simple negative test: EXCEPT
# ```sql
# (select * from x) except (select * from y where a > 7)
# ```
apply pipeline=threshold_elision
Threshold
  Union
    Distinct project=[#0, #1]
      Get x
    Negate
      Distinct project=[#0, #1]
        Filter #0 < 7
          Get y
----
Threshold
  Union
    Distinct project=[#0, #1]
      Get x
    Negate
      Distinct project=[#0, #1]
        Filter (#0 < 7)
          Get y


# Positive test: EXCEPT where the lhs has a Negate
# ```sql
# with r as (select * from x except select * from x where a < 7)
# select * from r except all select * from r where a > 9;
# ```
apply pipeline=threshold_elision
Return
  Threshold
    Union
      Get l0
      Negate
        Filter #0 > 9
          Get l0
With
  cte l0 =
    Threshold
      Union
        Get x
        Negate
          Filter #0 < 7
            Get x
----
With
  cte l0 =
    Union
      Get x
      Negate
        Filter (#0 < 7)
          Get x
Return
  Union
    Get l0
    Negate
      Filter (#0 > 9)
        Get l0
