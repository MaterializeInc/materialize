# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for partial reduction pushdown to constant inputs.
#
# TODO: Implement support for partial reduction pushdown.
# The general idea was discussed in
# https://github.com/MaterializeInc/materialize/issues/10119, but we
# decided that we need to spend more time on formalizing the proposed
# approach, to ensure it is actually correct. Until we have done so,
# the tests here only exercise the non-partial reduction pushdown
# optimization and are mostly equivalent to the tests in
# `reduction-pushdown`.

define
DefSource name=w
  - c0: smallint?
  - c1: integer?
----
Source defined as t0

define
DefSource name=x
  - c0: integer?
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

apply pipeline=ReductionPushdown
Distinct project=[#1]
  Join on=(#1 = #3)
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
----
Project (#0)
  Join on=(#0 = #1)
    Distinct project=[#1]
      Get x
    Distinct project=[#1]
      Constant <empty>

## distinct(<multiple columns from same input>)

apply pipeline=ReductionPushdown
Distinct project=[#0, #1]
  Join on=(#1 = #3)
    Constant <empty> // { types: "(integer?, text?)" }
    Get y
----
Project (#0, #1)
  Join on=(#1 = #2)
    Distinct project=[#0, #1]
      Constant <empty>
    Distinct project=[#1]
      Get y

## distinct(<multiple columns from differing inputs>)

apply pipeline=ReductionPushdown
Distinct project=[#0, #1, #2]
  Join on=(#1 = #3)
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
----
Project (#0, #1, #3)
  Join on=(#1 = #2)
    Distinct project=[#0, #1]
      Get x
    Distinct project=[#1, #0]
      Constant <empty>

## Negative test: Perform a full reduction pushdown
## if all inputs are constant

apply pipeline=ReductionPushdown
Distinct project=[#1]
  Join on=(#1 = #3)
    Constant <empty> // { types: "(integer?, text?)" }
    Constant <empty> // { types: "(integer?, text?)" }
----
Project (#0)
  Join on=(#0 = #1)
    Distinct project=[#1]
      Constant <empty>
    Distinct project=[#1]
      Constant <empty>

## Expressions in join equivalence classes

apply pipeline=ReductionPushdown
Distinct project=[#3]
  Join on=(substr(#1, 5) = #3)
    Constant <empty> // { types: "(integer?, text?)" }
    Get y
----
Project (#1)
  Join on=(#1 = #0)
    Distinct project=[substr(#1, 5)]
      Constant <empty>
    Distinct project=[#1]
      Get y

apply pipeline=ReductionPushdown
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = #3)
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
----
Project (#0)
  Join on=(#1 = #0)
    Distinct project=[substr(#1, 5)]
      Get x
    Distinct project=[#1]
      Constant <empty>

### Negative test: Do not do reduction pushdown
### if there are multi-component expressions in the join equivalence

apply pipeline=ReductionPushdown
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = text_concat_binary(#1, #3))
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
----
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = (#1 || #3))
    Get x
    Constant <empty>

apply pipeline=ReductionPushdown
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = #3 AND text_concat_binary(#1, #3) = "hello")
    Constant <empty> // { types: "(integer?, text?)" }
    Get y
----
Distinct project=[substr(#1, 5)]
  Join on=(substr(#1, 5) = #3 AND (#1 || #3) = "hello")
    Constant <empty>
    Get y

### Negative test: multi-input expression in group by key

apply pipeline=ReductionPushdown
Distinct project=[text_concat_binary(#1, #3)]
  Join on=(text_concat_binary(#1, #3) = "hello")
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
----
Distinct project=[(#1 || #3)]
  Join on=((#1 || #3) = "hello")
    Get x
    Constant <empty>

## Distinct pushdown across more than two inputs
## Make sure no cross joins happen.

apply pipeline=ReductionPushdown
Distinct project=[#1]
  Join on=(#1 = #3 = #5)
    Get x
    Get y
    Constant <empty> // { types: "(integer?, text?)" }
----
Project (#0)
  Join on=(#0 = #1 = #2)
    Distinct project=[#1]
      Get x
    Distinct project=[#1]
      Get y
    Distinct project=[#1]
      Constant <empty>

apply pipeline=ReductionPushdown
Distinct project=[#1, #5]
  Join on=(#1 = #3 AND #2 = #4)
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
    Constant <empty> // { types: "(text?, integer?)" }
----
Project (#0, #2)
  Join on=(#0 = #1)
    Distinct project=[#1]
      Get x
    Distinct project=[#1, #3]
      Join on=(#0 = #2)
        Constant <empty>
        Constant <empty>

### Negative test: Perform a full pushdown
### if each sub-join is non-constant

apply pipeline=ReductionPushdown
Distinct project=[#3, #5]
  Join on=(#0 = #2 AND #1 = #5)
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
    Get z
----
Project (#0, #2)
  Join on=(#1 = #2)
    Distinct project=[#3, #1]
      Join on=(#0 = #2)
        Get x
        Constant <empty>
    Distinct project=[#1]
      Get z

## Cross join tests

apply pipeline=ReductionPushdown
Distinct project=[#5]
  Join on=(#3 = #5)
    Constant <empty> // { types: "(integer?, text?)" }
    Get y
    Get z
----
Project (#1)
  Join on=(#0 = #1)
    Distinct project=[]
      Constant <empty>
    Distinct project=[#1]
      Get y
    Distinct project=[#1]
      Get z

apply pipeline=ReductionPushdown
Distinct project=[#0]
  Join on=(#3 = #5)
    Constant <empty> // { types: "(integer?, text?)" }
    Get y
    Get z
----
Project (#0)
  CrossJoin
    Distinct project=[#0]
      Constant <empty>
    Distinct project=[]
      Join on=(#1 = #3)
        Get y
        Get z

# Pushdown agg(distinct <single-input-expression>)

apply pipeline=ReductionPushdown
Reduce group_by=[#1] aggregates=[sum_int32(distinct #0)]
  Join on=(#1 = #3)
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
----
Project (#0, #1)
  Join on=(#0 = #2)
    Reduce group_by=[#1] aggregates=[sum(distinct #0)]
      Get x
    Distinct project=[#1]
      Constant <empty>

apply pipeline=ReductionPushdown
Reduce group_by=[#3] aggregates=[sum_int16(distinct #2)]
  Join on=(#1 = #3)
    Get x
    Constant <empty> // { types: "(integer?, text?)" }
    Get z
----
Project (#1, #2)
  Join on=(#0 = #1)
    Distinct project=[#1]
      Get x
    Reduce group_by=[#1] aggregates=[sum(distinct #0)]
      Constant <empty>
    Distinct project=[]
      Get z

apply pipeline=ReductionPushdown
Reduce group_by=[#3] aggregates=[sum_int32(distinct neg_int32(#0)), sum_int16(distinct #2)]
  Join on=(#1 = #3 = #5)
    Constant <empty> // { types: "(integer?, text?)" }
    Constant <empty> // { types: "(integer?, text?)" }
    Get z
----
Project (#2, #1, #3)
  Join on=(#0 = #2 = #4)
    Reduce group_by=[#1] aggregates=[sum(distinct -(#0))]
      Constant <empty>
    Reduce group_by=[#1] aggregates=[sum(distinct #0)]
      Constant <empty>
    Distinct project=[#1]
      Get z

# Pushdown agg(distinct <single-component multi-input expression>)

apply pipeline=ReductionPushdown
Reduce group_by=[#6] aggregates=[sum_int32(distinct add_int32(#0, cast_int16_to_int32(#2))), sum_int16(distinct mul_int16(#2, #4))]
  Join on=(#1 = #3 = #5 AND #4 = #6)
    Constant <empty> // { types: "(integer?, text?)" }
    Constant <empty> // { types: "(integer?, text?)" }
    Constant <empty> // { types: "(integer?, text?)" }
    Get w
----
Project (#3, #1, #2)
  Join on=(#0 = #3)
    Reduce group_by=[#4] aggregates=[sum(distinct (#0 + smallint_to_integer(#2))), sum(distinct (#2 * #4))]
      Join on=(#1 = #3 = #5)
        Constant <empty>
        Constant <empty>
        Constant <empty>
    Distinct project=[#0]
      Get w
