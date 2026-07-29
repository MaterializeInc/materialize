# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reduction pushdown tests

define
DefSource name=w
  - c0: smallint?
  - c1: smallint?
----
Source defined as t0

define
DefSource name=x
  - c0: smallint?
  - c1: text?
----
Source defined as t1

define
DefSource name=y keys=[[#1]]
  - c0: smallint?
  - c1: text?
----
Source defined as t2

define
DefSource name=z
  - c0: smallint?
  - c1: text?
----
Source defined as t3

# Distinct Pushdown tests

apply pipeline=reduction_pushdown
Distinct project=[#1]
  Join on=(#1 = #3)
    Get x
    Get y
----
Project (#0)
  Join on=(#0 = #1)
    Distinct project=[#1]
      Get x
    Distinct project=[#1]
      Get y

## distinct(<multiple columns from same input>)

apply pipeline=reduction_pushdown
Distinct project=[#0, #1]
  Join on=(#1 = #3)
    Get x
    Get y
----
Project (#0, #1)
  Join on=(#1 = #2)
    Distinct project=[#0, #1]
      Get x
    Distinct project=[#1]
      Get y

## distinct(<multiple columns from differing inputs>)

apply pipeline=reduction_pushdown
Distinct project=[#0, #1, #2]
  Join on=(#1 = #3)
    Get x
    Get y
----
Project (#0, #1, #3)
  Join on=(#1 = #2)
    Distinct project=[#0, #1]
      Get x
    Distinct project=[#1, #0]
      Get y

## Expressions in join equivalence classes

apply pipeline=reduction_pushdown
Distinct project=[#3]
  Join on=(substr(#1, 5) = #3)
    Get x
    Get y
----
Project (#1)
  Join on=(#1 = #0)
    Distinct project=[substr(#1, 5)]
      Get x
    Distinct project=[#1]
      Get y

apply pipeline=reduction_pushdown
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = #3)
    Get x
    Get y
----
Project (#0)
  Join on=(#1 = #0)
    Distinct project=[substr(#1, 5)]
      Get x
    Distinct project=[#1]
      Get y

### Negative test: Do not do reduction pushdown
### if there are multi-component expressions in the join equivalence

apply pipeline=reduction_pushdown
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = text_concat_binary(#1, #3))
    Get x
    Get y
----
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = (#1 || #3))
    Get x
    Get y

apply pipeline=reduction_pushdown
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = #3 AND text_concat_binary(#1, #3) = "hello")
    Get x
    Get y
----
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = #3 AND (#1 || #3) = "hello")
    Get x
    Get y

### Negative test: multi-input expression in group by key

apply pipeline=reduction_pushdown
Distinct project=[text_concat_binary(#1, #3)]
  Join on=(text_concat_binary(#1, #3) = "hello")
    Get x
    Get y
----
Distinct project=[(#1 || #3)]
  Join on=((#1 || #3) = "hello")
    Get x
    Get y

## Distinct pushdown across more than two inputs
## Make sure no cross joins happen.

apply pipeline=reduction_pushdown
Distinct project=[#1]
  Join on=(#1 = #3 = #5)
    Get x
    Get y
    Get y
----
Project (#0)
  Join on=(#0 = #1 = #2)
    Distinct project=[#1]
      Get x
    Distinct project=[#1]
      Get y
    Distinct project=[#1]
      Get y

apply pipeline=reduction_pushdown
Distinct project=[#1, #5]
  Join on=(#1 = #3 AND #2 = #4)
    Get x
    Get y
    Get z
----
Project (#0, #2)
  Join on=(#0 = #1)
    Distinct project=[#1]
      Get x
    Distinct project=[#1, #3]
      Join on=(#0 = #2)
        Get y
        Get z

### Similar to the above, but the join graph is now y-x-z instead of x-y-z

apply pipeline=reduction_pushdown
Distinct project=[#3, #5]
  Join on=(#0 = #2 AND #1 = #5)
    Get x
    Get y
    Get z
----
Project (#0, #2)
  Join on=(#1 = #2)
    Distinct project=[#3, #1]
      Join on=(#0 = #2)
        Get x
        Get y
    Distinct project=[#1]
      Get z

### Push down reductions on join(x, y) and join(z, w)

apply pipeline=reduction_pushdown
Distinct project=[#3, #5]
  Join on=(#0 = #2 AND #3 = #5 AND #4 = #6)
    Get x
    Get y
    Get z
    Get w
----
Project (#0, #3)
  Join on=(#0 = #2 AND #1 = #3)
    Distinct project=[#3, #3]
      Join on=(#0 = #2)
        Get x
        Get y
    Distinct project=[#1, #1]
      Join on=(#0 = #2)
        Get z
        Get w

# TODO(mgree): is this changed join order okay?
apply pipeline=optimize
Distinct project=[#3, #5]
  Join on=(#0 = #2 AND #3 = #5 AND #4 = #6)
    Get x
    Get y
    Get z
    Get w
----
Project (#0, #0)
  Join on=(#0 = #1) type=differential
    implementation
      %0[#0]UKA » %1[#0]UKA
    ArrangeBy keys=[[#0]]
      Distinct project=[#0]
        Project (#2)
          Join on=(#0 = #1) type=differential
            implementation
              %0:x[#0]K » %1:y[#0]K
            ArrangeBy keys=[[#0]]
              Project (#0)
                Get x
            ArrangeBy keys=[[#0]]
              Get y
    ArrangeBy keys=[[#0]]
      Distinct project=[#0]
        Project (#1)
          Join on=(#0 = #2) type=differential
            implementation
              %0:z[#0]K » %1:w[#0]K
            ArrangeBy keys=[[#0]]
              Get z
            ArrangeBy keys=[[#0]]
              Project (#0)
                Get w

### TODO: support this case where a reduction pushdown can happen by breaking
### it into components x-y and z.

apply pipeline=reduction_pushdown
Distinct project=[text_concat_binary(#1, #3)]
  Join on=(text_concat_binary(#1, #3) = "hello" AND #1 = #5)
    Get x
    Get y
    Get z
----
Distinct project=[(#1 || #3)]
  Join on=((#1 || #3) = "hello" AND #1 = #5)
    Get x
    Get y
    Get z

## Cross join tests

apply pipeline=reduction_pushdown
Distinct project=[#5]
  Join on=(#3 = #5)
    Get x
    Get y
    Get z
----
Project (#1)
  Join on=(#0 = #1)
    Distinct project=[]
      Get x
    Distinct project=[#1]
      Get y
    Distinct project=[#1]
      Get z

apply pipeline=reduction_pushdown
Distinct project=[#0]
  Join on=(#3 = #5)
    Get x
    Get y
    Get z
----
Project (#0)
  CrossJoin
    Distinct project=[#0]
      Get x
    Distinct project=[]
      Join on=(#1 = #3)
        Get y
        Get z

# Pushdown agg(distinct <single-input-expression>)

apply pipeline=reduction_pushdown
Reduce group_by=[#1] aggregates=[sum_int16(distinct #0)]
  Join on=(#1 = #3)
    Get x
    Get y
----
Project (#0, #1)
  Join on=(#0 = #2)
    Reduce group_by=[#1] aggregates=[sum(distinct #0)]
      Get x
    Distinct project=[#1]
      Get y

apply pipeline=reduction_pushdown
Reduce group_by=[#3] aggregates=[sum_int16(distinct #2)]
  Join on=(#1 = #3)
    Get x
    Get y
    Get z
----
Project (#1, #2)
  Join on=(#0 = #1)
    Distinct project=[#1]
      Get x
    Reduce group_by=[#1] aggregates=[sum(distinct #0)]
      Get y
    Distinct project=[]
      Get z

apply pipeline=reduction_pushdown
Reduce group_by=[#3] aggregates=[sum_int16(distinct neg_int16(#0)), sum_int16(distinct #2)]
  Join on=(#1 = #3 = #5)
    Get x
    Get y
    Get z
----
Project (#2, #1, #3)
  Join on=(#0 = #2 = #4)
    Reduce group_by=[#1] aggregates=[sum(distinct -(#0))]
      Get x
    Reduce group_by=[#1] aggregates=[sum(distinct #0)]
      Get y
    Distinct project=[#1]
      Get z

# Pushdown agg(distinct <single-component multi-input expression>)

apply pipeline=reduction_pushdown
Reduce group_by=[#6] aggregates=[sum_int16(distinct add_int16(#0, #2)), sum_int16(distinct mul_int16(#2, #4))]
  Join on=(#1 = #3 = #5 AND #4 = #6)
    Get x
    Get y
    Get z
    Get w
----
Project (#3, #1, #2)
  Join on=(#0 = #3)
    Reduce group_by=[#4] aggregates=[sum(distinct (#0 + #2)), sum(distinct (#2 * #4))]
      Join on=(#1 = #3 = #5)
        Get x
        Get y
        Get z
    Distinct project=[#0]
      Get w

# Empty group by key tests

apply pipeline=reduction_pushdown
Reduce aggregates=[sum_int16(distinct #0)]
  CrossJoin
    Get x
    Get y
----
Project (#0)
  CrossJoin
    Reduce aggregates=[sum(distinct #0)]
      Get x
    Distinct project=[]
      Get y

apply pipeline=reduction_pushdown
Reduce aggregates=[sum_int16(distinct #0)]
  Join on=(#1 = #3)
    Get x
    Get y
----
Reduce aggregates=[sum(distinct #0)]
  Join on=(#1 = #3)
    Get x
    Get y
