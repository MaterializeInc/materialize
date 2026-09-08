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
  - c2: bigint?
----
Source defined as t0

apply pipeline=optimize
Join on=(#0 = #3)
  Get x
  Get x
----
With
  cte l0 =
    ArrangeBy keys=[[#0]]
      Get x
Return
  Project (#0..=#2, #0, #4, #5)
    Join on=(#0 = #3) type=differential
      implementation
        %0:l0[#0]K » %1:l0[#0]K
      Get l0
      Get l0

# tests single-input predicates properly get pushed out of join equivalences
# using different combinations of literals and non-literals with different multiplicities

apply pipeline=optimize
Join on=(#0 = #1 = #2)
  Get x
----
Project (#0..=#2)
  Filter ((#4 AND #5) OR ((#0) IS NULL AND ((#3 AND #5) OR ((#1) IS NULL AND (#3 OR #4)))))
    Map ((#2) IS NULL, (#0 = #2), (#0 = #1))
      Get x

apply pipeline=optimize
Join on=(#0 = #2 = #1 = #2)
  Get x
----
Project (#0..=#2)
  Filter ((#4 AND #5) OR ((#0) IS NULL AND ((#3 AND #5) OR ((#1) IS NULL AND (#3 OR #4)))))
    Map ((#2) IS NULL, (#0 = #2), (#0 = #1))
      Get x

apply pipeline=optimize
Join on=(#0 = #1 = 5)
  Get x
----
Filter (#0 = 5) AND (#1 = 5)
  Get x

apply pipeline=optimize
Join on=(5 = #0 = #1)
  Get x
----
Filter (#0 = 5) AND (#1 = 5)
  Get x

apply pipeline=optimize
Join on=(5 = #0 = 5 = #3)
  Get x
  Get x
----
With
  cte l0 =
    ArrangeBy keys=[[]]
      Filter (#0 = 5)
        Get x
Return
  CrossJoin type=differential
    implementation
      %0:l0[×]ef » %1:l0[×]ef
    Get l0
    Get l0

apply pipeline=optimize
Join on=(5 = 9 = #0 = #3)
  Get x
  Get x
----
Constant <empty>

# test that JoinImplementation properly lifts MapFilterProjects
# The choice of MFP in the test is arbitrary; to ensure that MFP lifting is
# being correctly tested, make sure that:
# * the optimized result has at least one Map, one Filter, and one Project
#   after the join.
# * the project reorders columns in some way.
# * the filter has at least one predicate that refers to a mapped column.

apply pipeline=optimize
Join on=(#2 = #3)
  Project (#2, #1, #0)
    Filter (#1 < #2)
      Map (neg_int64(#1))
        Reduce group_by=[#0] aggregates=[sum_int64(#1)]
          Get x
  Get x
----
Project (#5, #1, #0, #0, #3, #4)
  Filter (#1 < #5)
    Map (-(#1))
      Join on=(#0 = #2) type=differential
        implementation
          %0[#0]UKAf » %1:x[#0]Kf
        ArrangeBy keys=[[#0]]
          Reduce group_by=[#0] aggregates=[sum(#1)]
            Project (#0, #1)
              Get x
        ArrangeBy keys=[[#0]]
          Get x

## MFPs don't get lifted if join is not using a pre-existing arrangement on that input.

apply pipeline=optimize
Join on=(#1 = #3)
  Project (#1, #3, #0)
    Filter (#1 < #0)
      Map (neg_int64(#1))
        Get x
  Reduce group_by=[#0] aggregates=[sum_int64(#1)]
    Get x
----
Project (#1, #2, #0, #2, #4)
  Join on=(#2 = #3) type=differential
    implementation
      %1[#0]UKA » %0:x[#2]Kf
    ArrangeBy keys=[[#2]]
      Project (#0, #1, #3)
        Filter (#1 < #0)
          Map (-(#1))
            Get x
    ArrangeBy keys=[[#0]]
      Reduce group_by=[#0] aggregates=[sum(#1)]
        Project (#0, #1)
          Get x

## join equivalence references column created by map being lifted.

apply pipeline=optimize
Join on=(#2 = #3 AND #1 = #6)
  Project (#1, #2, #0)
    Filter (#1 < #2)
      Map (neg_int64(#1))
        Reduce group_by=[#0] aggregates=[sum_int64(#1)]
          Get x
  Get x
  Reduce group_by=[#1] aggregates=[max_int64(#2)]
    Get x
----
Project (#1, #2, #0, #0, #4, #5, #2, #7)
  Join on=(#0 = #3 AND #2 = #6) type=differential
    implementation
      %2[#0]UKA » %0[#2]Kf » %1:x[#0]Kf
    ArrangeBy keys=[[#2]]
      Filter (#1 < #2)
        Map (-(#1))
          Reduce group_by=[#0] aggregates=[sum(#1)]
            Project (#0, #1)
              Get x
    ArrangeBy keys=[[#0]]
      Get x
    ArrangeBy keys=[[#0]]
      Reduce group_by=[#0] aggregates=[max(#1)]
        Project (#1, #2)
          Get x
