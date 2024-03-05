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
DefSource name=y
  - c1: bigint
  - c1: bigint
----
Source defined as t1


# Positive tests
# --------------

# Simple test: two nested joins.
apply pipeline=fusion_join
Join on=(#3 = #4)
  Join on=(#1 = #2)
    Get x
    Get x
  Get x
----
Join on=(#1 = #2 AND #3 = #4)
  Get x
  Get x
  Get x

# Simple test: two nested joins separated by a filter.
apply pipeline=fusion_join
Join on=(#0 = #2)
  Get x
  Filter (#1 > 42)
    Join on=(#0 = #2)
      Get x
      Get y
----
Join on=(#0 = #2 = #4)
  Get x
  Filter (#1 > 42)
    Get x
  Get y

# Simple test: a join with an that has a filter.
apply pipeline=fusion_join
Join on=(#0 = #2)
  Get x
  Filter (#1 > 42)
    Get y
----
Join on=(#0 = #2)
  Get x
  Filter (#1 > 42)
    Get y

# Check that filters around non-join operators are handled properly.
apply pipeline=fusion_join
Join on=(#0 = #2 = #6)
  Get x
  Filter (#1 > 42)
    Join on=(#0 = #2)
      Get x
      Get y
  Filter (#1 < 17)
    Get y
----
Join on=(#0 = #2 = #4 = #6)
  Get x
  Filter (#1 > 42)
    Get x
  Get y
  Filter (#1 < 17)
    Get y

# Full-blown MFP between joins
apply pipeline=fusion_join
Join on=((#0 + 4) = (#2 + 3) = #6)
  Get x
  Project (#0, #1, #2, #3)
    Filter (#1 + #4 > 17 AND #1 + #4 < 42)
      Map(#1 * #3)
        Join on=((#0 + 4) = (#2 + 3))
          Get x
          Get y
  Project (#1, #0)
    Filter (#1 < 17)
      Get y
----
Project (#0..=#7)
  Filter (#8 < 42) AND (#8 > 17)
    Map ((#3 + (#3 * #5)))
      Join on=(#6 = (#0 + 4) = (#2 + 3) AND (#2 + 4) = (#4 + 3))
        Get x
        Get x
        Get y
        Project (#1, #0)
          Filter (#1 < 17)
            Get y

# Full-blown MFP between joins: `Map (#1 * #3)` is referenced by a
# Join predicate.
apply pipeline=fusion_join
Join on=((#0 + 4) = (#2 + 3) = #6)
  Get x
  Project (#4, #0, #1, #2)
    Filter (#1 + #4 > 17 AND #1 + #4 < 42)
      Map (#1 * #3)
        Join on=((#0 + 4) = (#2 + 3))
          Get x
          Get y
  Project (#1, #0)
    Filter (#1 < 17)
      Get y
----
Project (#0, #1, #8, #2..=#4, #6, #7)
  Filter (#9 < 42) AND (#9 > 17)
    Map ((#3 * #5), (#3 + #8))
      Join on=(#6 = (#0 + 4) = ((#3 * #5) + 3) AND (#2 + 4) = (#4 + 3))
        Get x
        Get x
        Get y
        Project (#1, #0)
          Filter (#1 < 17)
            Get y
