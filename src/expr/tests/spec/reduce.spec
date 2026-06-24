# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IsNull around a single UnaryFunc.

reduce
types (integer?)
((((#0) IS NULL)) IS NULL)
----
false

reduce
types (integer?)
((cast_int32_to_numeric[39](#0)) IS NULL)
----
(#0) IS NULL

reduce
types (jsonb?)
((cast_jsonb_to_int32(#0)) IS NULL)
----
(#0) IS NULL

# IsNull around a single BinaryFunc.

reduce
types (integer, integer)
((add_int32(#1, #0)) IS NULL)
----
false

reduce
types (boolean, boolean)
(((#1 AND #0)) IS NULL)
----
false

reduce
types (jsonb, bigint)
((jsonb_get_int64(#1, #0)) IS NULL)
----
((#1 -> #0)) IS NULL

# IsNull around multiple functions

reduce
types (jsonb?)
((((cast_jsonb_to_int32(#0)) IS NULL)) IS NULL)
----
false

reduce
types (integer?)
((not(((#0) IS NULL))) IS NULL)
----
false

reduce
types (integer?)
((cast_numeric_to_int64(cast_int32_to_numeric[39](#0))) IS NULL)
----
(#0) IS NULL

reduce
types (integer?)
((cast_int64_to_float64(cast_numeric_to_int64(cast_int32_to_numeric[39](#0)))) IS NULL)
----
(#0) IS NULL

reduce
types (integer?, integer?, integer?)
((add_int32(#1, mul_int32(#2, #0))) IS NULL)
----
((#0) IS NULL OR (#1) IS NULL OR (#2) IS NULL)

# Null-propagating UnaryFunc around BinaryFunc
reduce
types (integer, integer)
((cast_int32_to_numeric[39](add_int32(#1, #0))) IS NULL)
----
false

# Non-null-propagating UnaryFunc around BinaryFunc
reduce
types (integer, integer)
((((add_int32(#1, #0)) IS NULL)) IS NULL)
----
false

# Null-propagating BinaryFunc around UnaryFuncs
reduce
types (text?, boolean?)
(((((#1) IS NULL) < cast_string_to_bool(#0))) IS NULL)
----
(#0) IS NULL

# Non-null-propagating BinaryFunc around UnaryFuncs
reduce
types (text?, boolean?)
(((((#1) IS NULL) OR cast_string_to_bool(#0))) IS NULL)
----
(((#1) IS NULL OR text_to_boolean(#0))) IS NULL

# outer is_null needs to be resolved in a second round.

reduce
types (text?, boolean?)
(((((((#1) IS NULL)) IS NULL) OR cast_string_to_bool(#0))) IS NULL)
----
(#0) IS NULL

# Constant folding

reduce
types (integer?, integer?)
((add_int32(#1, mul_int32(null::boolean, #0))) IS NULL)
----
true

reduce
types (text?, boolean?)
(((((((#1) IS NULL)) IS NULL) AND cast_string_to_bool(#0))) IS NULL)
----
false

# Not/demorgans propagation

reduce
types (integer, integer)
not(((add_int32(#1, #0)) IS NULL))
----
true

reduce
types (integer, integer, integer)
not(((#1 > #0) AND (#1 < #2)))
----
((#1 <= #0) OR (#1 >= #2))

reduce
types (boolean?, integer, integer)
not((not(#0) AND (#1 < not(#2))))
----
(#0 OR (#1 >= NOT(#2)))

reduce
types (boolean?, boolean?, boolean?)
not((not(#0) AND (#1 OR not(#2))))
----
(#0 OR (#2 AND NOT(#1)))

# undistribute_and_or

reduce
types (boolean?, boolean?, boolean?)
((not(#1) AND #0) OR (not(#1) AND #2))
----
(NOT(#1) AND (#0 OR #2))

reduce
types (boolean?, boolean?, boolean?)
not(((#1 AND #0) OR (#1 AND #2)))
----
(NOT(#1) OR (NOT(#0) AND NOT(#2)))

reduce
types (boolean?, boolean?, boolean?)
not(((#1 OR #0) AND (#1 OR #2)))
----
(NOT(#1) AND (NOT(#0) OR NOT(#2)))

reduce
types (bigint?, integer?, integer?)
(((add_int32(#1, cast_int64_to_int32(#0))) IS NULL) AND ((add_int32(#2, #0)) IS NULL))
----
((#0) IS NULL OR ((#1) IS NULL AND (#2) IS NULL))

# undistribute_and_or -- If there are multiple overlapping undistribution opportunities, and one of them leads to an
# absorption, then we should pick that one.
# Here, we could undistribute either #1 or #2, but #2 leads to an absorption.

reduce
types (integer?, integer?, integer?)
((#0 AND #1) OR (#1 AND #2) OR and(#2))
----
(#2 OR (#0 AND #1))

# flatten_associative + undistribute_and_or
# ((#0 OR #1) OR (#2 > 2)) AND (#0 OR ((#2 < 3) OR #1))
reduce
types (boolean?, boolean?, integer?)
(or((#0 OR #1), (#2 > 2)) AND or(#0, ((#2 < 3) OR #1)))
----
(#0 OR #1 OR ((#2 < 3) AND (#2 > 2)))

# Test that flatten_associative works on
# functions other than `and` and `or`.
reduce
types (integer?, integer?, integer?, integer?, integer?, integer?, integer?, integer?, integer?)
greatest(greatest(#0, #1, #2), least(#3, #4, #5), greatest(#6, #7, #8))
----
greatest(#0, #1, #2, least(#3, #4, #5), #6, #7, #8)

# Right-deep tree
reduce
types (integer?, integer?, integer?, integer?, integer?)
coalesce(#0, coalesce(#1, coalesce(#2, coalesce(#3, #4))))
----
coalesce(#0, #1, #2, #3, #4)

# Left-deep tree
reduce
types (integer?, integer?, integer?, integer?, integer?)
coalesce(coalesce(coalesce(coalesce(#0, #1), #2), #3), #4)
----
coalesce(#0, #1, #2, #3, #4)

# undistribute_and_or -- more than 2 args at the top level.
# (#0 OR #1 OR (#2 > 2)) AND (#0 OR (#2 < 3) OR #1) AND (#1 OR (#5 < 7) OR #0)
reduce
types (boolean?, boolean?, integer?, integer?, integer?, integer?)
((#0 OR #1 OR (#2 > 2)) AND (#0 OR (#2 < 3) OR #1) AND (#1 OR (#5 < 7) OR #0))
----
(#0 OR #1 OR ((#2 < 3) AND (#5 < 7) AND (#2 > 2)))

# undistribute_and_or -- only a subset of the top-level args have a non-empty intersection.
# This test comes from TPC-H Q19, after distribute_and_over_or does 2 steps. Undistribute_and_or has to do many steps
# to undo that.
# In the output, `(#20 OR #45)` is `l_shipmode IN ('AIR', 'AIR REG')`, and the 4-arg ORs are the INs on p_container.
reduce
types (boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?, boolean?)
((#20 AND #21 AND #22 AND #23 AND #24 AND #25) OR (#20 AND #21 AND #26 AND #23 AND #24 AND #25) OR (#20 AND #21 AND #27 AND #23 AND #24 AND #25) OR (#20 AND #21 AND #28 AND #23 AND #24 AND #25) OR (#20 AND #29 AND #30 AND #31 AND #32 AND #33) OR (#20 AND #29 AND #34 AND #31 AND #32 AND #33) OR (#20 AND #29 AND #35 AND #31 AND #32 AND #33) OR (#20 AND #29 AND #36 AND #31 AND #32 AND #33) OR (#20 AND #37 AND #38 AND #39 AND #40 AND #41) OR (#20 AND #37 AND #42 AND #39 AND #40 AND #41) OR (#20 AND #37 AND #43 AND #39 AND #40 AND #41) OR (#20 AND #37 AND #44 AND #39 AND #40 AND #41) OR (#45 AND #21 AND #22 AND #23 AND #24 AND #25) OR (#45 AND #21 AND #26 AND #23 AND #24 AND #25) OR (#45 AND #21 AND #27 AND #23 AND #24 AND #25) OR (#45 AND #21 AND #28 AND #23 AND #24 AND #25) OR (#45 AND #29 AND #30 AND #31 AND #32 AND #33) OR (#45 AND #29 AND #34 AND #31 AND #32 AND #33) OR (#45 AND #29 AND #35 AND #31 AND #32 AND #33) OR (#45 AND #29 AND #36 AND #31 AND #32 AND #33) OR (#45 AND #37 AND #38 AND #39 AND #40 AND #41) OR (#45 AND #37 AND #42 AND #39 AND #40 AND #41) OR (#45 AND #37 AND #43 AND #39 AND #40 AND #41) OR (#45 AND #37 AND #44 AND #39 AND #40 AND #41))
----
((#20 OR #45) AND ((#21 AND #23 AND #24 AND #25 AND (#22 OR #26 OR #27 OR #28)) OR (#29 AND #31 AND #32 AND #33 AND (#30 OR #34 OR #35 OR #36)) OR (#37 AND #39 AND #40 AND #41 AND (#38 OR #42 OR #43 OR #44))))

## a | (a & b)

reduce
types (boolean?, boolean?, integer?)
(#0 OR (#0 AND (#2 < 3)))
----
#0

## a & (a | b)

reduce
types (boolean?, boolean?, integer?)
((#0 OR #1) AND or(#0, ((#2 < 3) OR #1)))
----
(#0 OR #1)

## Record get/create optimizations

reduce
types (integer?, integer?)
record_get[0](record_create["f1", "f2"](#0, #1))
----
#0

reduce
types (integer?, integer?)
record_get[0](coalesce(record_create["f1", "f2"](#0, #1), record_create["f1", "f2"](null::integer, null::integer)))
----
#0

## list_index(list_create, literal), e.g., list[f1, f2][2] --> f2
## See rest of the tests for this in list.slt

reduce
types (integer?, integer?, integer?)
list_index(list_create[integer](#1, #2), 2)
----
#2

## Case/If optimizations

reduce
types (integer?, integer?)
case when (#0 > #1) then false else true end
----
((#0) IS NULL OR (#1) IS NULL OR (#0 <= #1))

reduce
types (integer?, integer?)
case when (#0 > #1) then false else null::boolean end
----
(null AND ((#0) IS NULL OR (#1) IS NULL OR (#0 <= #1)))

reduce
types (integer?, integer?)
case when (#0 > #1) then false else false end
----
false

# non-literal expression in the THEN clause
reduce
types (integer?, integer?)
case when (#0 > #1) then (#0 = 1::integer) else false end
----
case when (#0 > #1) then (#0 = 1) else false end

reduce
types (integer?, integer?)
case when (#0 > #1) then null::boolean else false end
----
(null AND (#0) IS NOT NULL AND (#1) IS NOT NULL AND (#0 > #1))

# non-literal expression in the THEN clause
reduce
types (integer?, integer?)
case when (#0 > #1) then true else false end
----
((#0) IS NOT NULL AND (#1) IS NOT NULL AND (#0 > #1))

reduce
types (integer?, integer?)
case when (#0 > #1) then true else null::boolean end
----
(null OR ((#0) IS NOT NULL AND (#1) IS NOT NULL AND (#0 > #1)))

reduce
types (integer?, integer?)
case when (#0 > #1) then (#0 = 1::integer) else true end
----
case when (#0 > #1) then (#0 = 1) else true end

reduce
types (integer?, integer?)
case when (#0 > #1) then null::boolean else true end
----
(null OR (#0) IS NULL OR (#1) IS NULL OR (#0 <= #1))

reduce
types (integer?, integer?)
case when (#0 > #1) then true else true end
----
true

reduce
types (integer?, integer?)
case when (#0 > #1) then null::boolean else null::boolean end
----
null

reduce
types (integer?, integer?)
case when (#0 > #1) then null::integer else null::integer end
----
null

reduce
types (integer?, integer?)
case when (#0 > #1) then 1::integer else 2::integer end
----
case when (#0 > #1) then 1 else 2 end

reduce
types ()
case when null::boolean then true else false end
----
false

reduce
types ()
case when null::boolean then false else true end
----
true

reduce
types (boolean?)
case when null::boolean then #0 else false end
----
false

reduce
types (boolean?)
case when null::boolean then #0 else true end
----
true

reduce
types (boolean?)
case when null::boolean then false else #0 end
----
#0

reduce
types (boolean?)
case when null::boolean then true else #0 end
----
#0

reduce
types ()
case when true then true else false end
----
true

reduce
types ()
case when true then false else true end
----
false

reduce
types (boolean?)
case when true then #0 else false end
----
#0

reduce
types (boolean?)
case when true then #0 else true end
----
#0

reduce
types (boolean?)
case when true then false else #0 end
----
false

reduce
types (boolean?)
case when true then true else #0 end
----
true

reduce
types ()
case when false then true else false end
----
false

reduce
types ()
case when false then false else true end
----
true

reduce
types (boolean?)
case when false then #0 else false end
----
false

reduce
types (boolean?)
case when false then #0 else true end
----
true

reduce
types (boolean?)
case when false then false else #0 end
----
#0

reduce
types (boolean?)
case when false then true else #0 end
----
#0

### Regression test for materialize#9995.
### The inner if statement can be replaced by its `condition`, but we must
### ensure that we keep the type of the `then` and `els` clauses.
### The type of the error should be int32 instead of bool.

reduce
types (bigint?)
case when (#0 = 1) then 1 else case when (#0 = div_int64(1, 0)) then 1 else 1 end end
----
case when (#0 = 1) then 1 else error("division by zero") end

## undistribute_and_or works despite multiple copies of the same expression in
## the intersection

canonicalize
types (double precision?, double precision?, double precision?, double precision?)
((not(((#1) IS NULL)) AND (#0 < #2)) OR not(or(((#1) IS NULL), (((#1) IS NULL) OR ((#3) IS NULL)))))
----
(#1) IS NOT NULL
((#3) IS NOT NULL OR (#0 < #2))

canonicalize
types (double precision?, double precision?, double precision?, double precision?)
(not(((add_float64(add_float64(#3, #1), #1)) IS NULL)) OR (not(((#1) IS NULL)) AND (#0 < #2)))
----
(#1) IS NOT NULL
((#3) IS NOT NULL OR (#0 < #2))

# expressions in equivalence classes only become simpler.

canonicalize-join
types (integer?)
(#0 AND add_int32(#0, #0)), add_int32(#0, #0), add_int32(#0, add_int32(#0, #0))
----
[(#0 + #0) (#0 + (#0 + #0)) (#0 AND (#0 + #0))]

canonicalize-join
types (integer?, integer?, integer?, integer?)
#0, #3
#1, add_int32(add_int32(#2, #2), #1), add_int32(add_int32(#2, #2), add_int32(add_int32(#0, #0), #0)), add_int32(add_int32(#3, #3), #3)
----
[#0 #3]
[#1 ((#0 + #0) + #0) ((#2 + #2) + #1)]

canonicalize-join
types (integer?, integer?, integer?, integer?, integer?, integer?)
#0, #3
#1, add_int32(add_int32(#2, #2), #1), add_int32(mul_int32(#4, #5), add_int32(add_int32(#0, #0), #0)), add_int32(add_int32(#3, #3), #3)
add_int32(#2, #2), mul_int32(#4, #5)
----
[#0 #3]
[#1 ((#0 + #0) + #0) ((#2 + #2) + #1)]
[(#2 + #2) (#4 * #5)]

# replacing expressions with simpler equivalent ones can result in the
# collapsing of equivalence classes.

canonicalize-join
types (integer?, integer?, integer?, integer?, integer?, integer?)
#0, #3
#1, add_int32(#0, #0), add_int32(add_int32(#2, #2), #1), add_int32(mul_int32(#4, #5), add_int32(add_int32(#0, #0), #0)), add_int32(add_int32(#3, #3), #3)
add_int32(#3, #3), mul_int32(#4, #5), sub_int32(add_int32(#3, #3), mul_int32(#4, #5))
----
[#0 #3]
[#1 (#0 + #0) (#1 + #0) (#1 + #1) ((#2 + #2) + #1) (#1 - #1) (#4 * #5)]

canonicalize-join
types (integer?, integer?, integer?, integer?)
#0, #3, #3
add_int32(#0, #0), #1
add_int32(#3, #3), #2
----
[#0 #3]
[#1 #2 (#0 + #0)]

# replacing expressions with simpler equivalent ones can result in the
# removal of redundant equivalence classes.

canonicalize-join
types (integer?, integer?, integer?, integer?)
#0, #0, #3
add_int32(#0, #0), add_int32(#3, #3)
----
[#0 #3]

# test an equivalence class when the number of leaves are the same but the
# number of nonleaves are not.

canonicalize-join
types (integer?)
cast_int16_to_int32(#0), neg_int32(cast_int16_to_int32(#0)), neg_int32(neg_int32(cast_int16_to_int32(#0)))
----
[-(smallint_to_integer(#0)) smallint_to_integer(#0)]

# literals don't get overwritten with equivalent expressions

canonicalize-join
types (integer?, integer?)
#0, 4::integer, add_int32(#1, 4::integer)
----
[#0 4 (#1 + 4)]

# functions on literals don't cause cycling
canonicalize-join
types (integer?, integer?, integer?, integer?)
#0, 4::integer
neg_int32(#0), neg_int32(#1), neg_int32(4::integer)
add_int32(#1, neg_int32(#1)), #3
----
[#0 4]
[#3 (#1 + -4)]
[-4 -(#1)]

canonicalize-join
types (integer?, integer?, integer?, integer?)
#0, 4::integer
neg_int32(#0), neg_int32(#1)
neg_int32(#1), neg_int32(4::integer)
add_int32(#1, neg_int32(#1)), #3
----
[#0 4]
[#3 (#1 + -4)]
[-4 -(#1)]

# expressions in join equivalences get reduced after simpler equivalent
# expressions are substituted

## constant folding

canonicalize-join
types (bigint?)
0, add_int64(0, #0)
1234, add_int64(add_int64(0, #0), 0)
----
[#0 0 1234]

## consecutive nots cancel each other

canonicalize-join
types (boolean?, text?, text?)
not(#0), is_regexp_match_case_insensitive(#1, #2)
false, not(is_regexp_match_case_insensitive(#1, #2))
----
[#0 false]
[true (#1 ~* #2)]

canonicalize-join
types (boolean?, boolean?)
not(#0), (#0 OR #1)
false, not((#0 OR #1))
----
[#0 false]
[#1 true]

## demorgans

canonicalize-join
types (boolean?, boolean?, boolean?, boolean?, boolean?)
(#2 AND #3), coalesce(#0, #1, #2, false)
#0, (coalesce(#0, #1, #2, false) OR (#2 AND #4))
----
[#0 (#2 AND (#3 OR #4))]
[coalesce(#0, #1, #2, false) (#2 AND #3)]

## decompose is_null

canonicalize-join
types (integer?, integer?, integer?, integer?)
add_int32(#2, #3), coalesce(#0, #1, false)
#0, ((coalesce(#0, #1, false)) IS NULL)
----
[#0 ((#2) IS NULL OR (#3) IS NULL)]
[(#2 + #3) coalesce(#0, #1, false)]

# impossible condition detection during predicate canonicalization produces
# reliable output regardless of the order of the input predicates
canonicalize
types (bigint?)
(#0 = 10)
((#0) IS NULL)
----
false

canonicalize
types (bigint?, bigint?, bigint?)
((((#0) IS NULL) AND ((#1) IS NULL)) OR (#0 = #1))
((((#0) IS NULL) AND ((#2) IS NULL)) OR (#0 = #2))
not(((#0) IS NULL))
not(((#1) IS NULL))
not(((#0) IS NULL))
not(((#2) IS NULL))
----
(#0 = #1)
(#0 = #2)

canonicalize
types (bigint?, bigint?, bigint?)
((((#0) IS NULL) AND ((#1) IS NULL)) OR (#0 = #1))
((((#0) IS NULL) AND ((#2) IS NULL)) OR (#0 = #2))
not(((#0) IS NULL))
not(((#1) IS NULL))
not(((#1) IS NULL))
not(((#2) IS NULL))
----
(#0 = #1)
(#0 = #2)

canonicalize
types (bigint?, bigint?, bigint?)
((((#1) IS NULL) AND ((#2) IS NULL)) OR (#1 = #2))
(#0 = #1)
(#0 = #2)
----
(#0 = #1)
(#0 = #2)
(#1 = #2)

# check that predicates are sorted by their complexity
canonicalize
types (integer?, integer?)
(add_int32(#0, 1::integer) = #1)
(#0 < 2147483647::integer)
----
(#0 < 2147483647)
(#1 = (#0 + 1))

canonicalize
types (integer?, integer?, integer?)
(add_int32(#0, 1::integer) < #2)
(add_int32(#0, 1::integer) < 2147483647::integer)
(#0 = #1)
----
(#0 = #1)
((#0 + 1) < 2147483647)
((#0 + 1) < #2)

# Complementary-pair collapse: `p OR NOT(p)` --> true and `p AND NOT(p)` -->
# false, but only when `p` is non-nullable and infallible.

# Non-nullable, infallible `p`: collapses.
reduce
types (boolean)
(#0 OR not(#0))
----
true

reduce
types (boolean)
(#0 AND not(#0))
----
false

# Extra operands do not block the collapse: the forced zero dominates them.
reduce
types (boolean, boolean)
or(#0, not(#0), #1)
----
true

reduce
types (boolean, boolean)
and(#0, not(#0), #1)
----
false

# The dominance holds even when the extra operand is nullable or fallible: the
# forced zero short-circuits past both.
reduce
types (boolean, boolean?)
or(#0, not(#0), #1)
----
true

reduce
types (boolean, text)
or(#0, not(#0), cast_string_to_bool(#1))
----
true

# Nullable `p`: `NULL OR NOT(NULL)` is NULL, not true, so no collapse.
reduce
types (boolean?)
(#0 OR not(#0))
----
(#0 OR NOT(#0))

reduce
types (boolean?)
(#0 AND not(#0))
----
(#0 AND NOT(#0))

# Fallible `p`: if `p` errors, both `p` and `NOT(p)` error, so the call errors
# rather than evaluating to its zero. No collapse even though `p` is
# non-nullable.
reduce
types (text)
(cast_string_to_bool(#0) OR not(cast_string_to_bool(#0)))
----
(NOT(text_to_boolean(#0)) OR text_to_boolean(#0))

# The negation is matched in the canonical form `reduce` leaves it in, so a
# comparison pair collapses too: `reduce` rewrites `NOT(#0 = 5)` to `#0 != 5`.
reduce
types (integer)
or((#0 = 5::integer), not((#0 = 5::integer)))
----
true

reduce
types (integer)
and((#0 = 5::integer), not((#0 = 5::integer)))
----
false

# Ordered comparisons flip too, but only in the argument order they were
# written in: `reduce` canonically orders the arguments of `=` and `!=` only.
reduce
types (integer, integer)
or((#0 < #1), not((#0 < #1)))
----
true

reduce
types (integer, integer)
or((#0 < #1), (#1 <= #0))
----
((#0 < #1) OR (#1 <= #0))

# Nullable comparison: no collapse.
reduce
types (integer?)
or((#0 = 5::integer), not((#0 = 5::integer)))
----
((#0 = 5) OR (#0 != 5))

# Fallible comparison: no collapse.
reduce
types (integer)
or((div_int32(1::integer, #0) = 1::integer), not((div_int32(1::integer, #0) = 1::integer)))
----
((1 = (1 / #0)) OR (1 != (1 / #0)))

# De Morgan's law spreads the negation of a nested AND/OR over its children, so
# the complement is a negated child per child rather than a single operand.
reduce
types (boolean, boolean)
or((#0 AND #1), not((#0 AND #1)))
----
true

reduce
types (boolean, boolean)
and((#0 OR #1), not((#0 OR #1)))
----
false

# Nullable children: no collapse.
reduce
types (boolean?, boolean?)
or((#0 AND #1), not((#0 AND #1)))
----
(NOT(#0) OR NOT(#1) OR (#0 AND #1))

# Only part of the spread negation is present, which is not a tautology.
reduce
types (boolean, boolean)
or((#0 AND #1), not(#0))
----
(NOT(#0) OR (#0 AND #1))
