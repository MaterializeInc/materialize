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


# Regression test for database-issues#7467.
#
# The Join has a condition that is a local predicate
# and was lost prior to database-issues#7467.
apply pipeline=reduction_pushdown
Distinct project=[#1]
  Join on=((#1 + #1) = #0)
    Get x
    Get y
----
Project (#0)
  CrossJoin
    Distinct project=[#1]
      Filter (((#1 + #1) = #0) OR (((#1 + #1)) IS NULL AND (#0) IS NULL))
        Get x
    Distinct project=[]
      Get y
