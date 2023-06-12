# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TODO(mgree): need to relax the parser to write more type errors
#
# missing relation type errors:
#  unbouned variables (parser panics)
#  bad type annotation on Get (parser panics)
#  project of non-existent column (parser panics)
#  union of incompatible types (parser panics)
#  strict join equivalence checking (typechecker is imprecise, we only log debug messages)
#
# missing scalar/tablefunc/aggregate type errors:
#   bad column (parser rules out)
#   mismatched then/else branches in an if (parser doesn't support as of 2023-06-09)
#   bad input type (not checkable---we don't know input types as of 2023-06-09)

# Source definitions
# ------------------

# Define t0 source
define
DefSource name=t0
  - bigint
  - bigint?
  - text
----
Source defined as t0

# Define t1 source
define
DefSource name=t1
  - text
  - bigint
  - boolean
----
Source defined as t1

############################################################################
# Case: Constant
############################################################################

typecheck
Constant // { types: "(bigint, bigint)" }
  - (1, 3)
----
(Int64, Int64)

typecheck
Constant // { types: "(bigint, bigint)" }
  - (1, 3)
  - ("oh", "no")
----
----
In the MIR term:
Constant
  - (1, 3)
  - ("oh", "no")


bad constant row
      got ("oh", "no")
expected row of type (Int64, Int64)
----
----

############################################################################
# Case: Get
############################################################################

# Present
typecheck
Get t0
----
(Int64, Int64?, String)


typecheck
Get t1
----
(String, Int64, Bool)

############################################################################
# Case: Let
############################################################################

typecheck
Return
  Get l0
With
  cte l0 =
    Get t0
----
(Int64, Int64?, String)

# no shadowing
typecheck
Return
  Get l0
With
  cte l0 =
    Return
      Get l0
    With
      cte l0 =
        Get t0
----
(Int64, Int64?, String)

# shadowing
typecheck
Return
  Get l0
With
  cte l0 =
    Get t0
  cte l0 =
    Get t1
----
----
In the MIR term:
Return
  Get l0
With
  cte l0 =
    Get t0


id l0 is shadowed
----
----

############################################################################
# Case: LetRec
############################################################################

# no shadowing
typecheck
Return
  Map ("return_inner")
    Get l1
With Mutually Recursive
  cte l1 = // { types: "(bigint, bigint?, text)" }
    Return
      Union
        Get l1
        Filter (#0 > 1)
          Get l0
    With Mutually Recursive
      cte l0 = // { types: "(bigint, bigint?, text)" }
        Union
          Get l0
          Filter (#0 > 42)
            Get t0
----
(Int64, Int64?, String, String)

typecheck
Return
  Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint?, text)" }
    Return
      Union
        Get l0
        Filter (#0 > 1)
          Get l0
    With Mutually Recursive
      cte l0 = // { types: "(bigint, bigint?, text)" }
        Union
          Get l0
          Filter (#0 > 42)
            Get t0
----
----
In the MIR term:
Return
  Union
    Get l0
    Filter (#0 > 1)
      Get l0
With Mutually Recursive
  cte l0 =
    Union
      Get l0
      Filter (#0 > 42)
        Get t0


id l0 is shadowed
----
----

# type annotations don't match
typecheck
Return
  Filter #0 AND #1
    Get l0
With Mutually Recursive
  cte l0 = // { types: "(boolean?, boolean)" }
    Constant // { types: "(bigint, bigint)" }
      - (1, 3)
  cte l1 = // { types: "(boolean, boolean?)" }
    Get l1
----
----
In the MIR term:
Return
  Filter #0 AND #1
    Get l0
With Mutually Recursive
  cte l0 =
    Constant
      - (1, 3)
  cte l1 =
    Get l1


mismatched column types: couldn't compute union of column types in let rec: Can't union types: Bool and Int64
      got Int64
expected Bool?
  Bool is a not a subtype of Int64
  Bool? is nullable but Int64 is not
----
----

############################################################################
# Case: Project
############################################################################

typecheck
Project (#0, #2)
  Get t0
----
(Int64, String)

typecheck
Project ()
  Get t0
----
()

# TODO(mgree): would be good to test this, but the parser dies in `col_with_input_cols` before we get to the typechecker, i.e., the other typechecker beats us to the punch
#typecheck
#Project (#0, #5, #1)
#  Get t0
#----
#error

############################################################################
# Case: Map
############################################################################

typecheck
Map (#0 + #1)
  Get t0
----
(Int64, Int64?, String, Int64?)

# ok constant
typecheck
Map (#0 + #1)
  Constant // { types: "(bigint, bigint)" }
    - (1, 3)
    - (2, 4)
----
(Int64, Int64, Int64)

# bad constant
typecheck
Map (#0 + #1)
  Constant // { types: "(bigint, bigint)" }
    - (1, 3)
    - (2, "uh oh")
----
----
In the MIR term:
Constant
  - (1, 3)
  - (2, "uh oh")


bad constant row
      got (2, "uh oh")
expected row of type (Int64, Int64)
----
----

############################################################################
# Case: FlatMap
############################################################################

typecheck
FlatMap generate_series(#0, #1 + 3, 5)
  Get t0
----
(Int64, Int64?, String, Int64)

############################################################################
# Case: Filter
############################################################################

# nullability narrowing
typecheck
Filter (#0 = #1)
  Get t0
----
(Int64, Int64, String)


# nullability narrowing
typecheck
Filter (#0 > #1)
  Get t0
----
(Int64, Int64, String)


# multiple predicates
typecheck
Filter (#0 = "hi" AND #1 > 2 AND #2)
  Get t1
----
(String, Int64, Bool)


# bad predicate
typecheck
Filter #2
  Get t0
----
----
In the MIR term:
Filter #2
  Get t0


mismatched column types: expected boolean condition
      got String
expected Bool?
  String is a not a subtype of Bool
----
----

############################################################################
# Case: Join
############################################################################

typecheck
Join on=(eq(#0, #3))
  Get t0
  Get t0
----
(Int64, Int64?, String, Int64, Int64?, String)

# TODO(mgree): should narrow to non-null with typechecker improvements
typecheck
Join on=(eq(#0, #1, #3, #4))
  Get t0
  Get t0
----
(Int64, Int64?, String, Int64, Int64?, String)

# TODO(mgree): should narrow to non-null with typechecker improvements
typecheck
Join on=(#0 = #4 AND #2 = #5 AND #1 = #3)
  Get t0
  Get t0
----
(Int64, Int64?, String, Int64, Int64?, String)

# TODO(mgree): panics in `col_with_input_cols` src/expr/src/relation/mod.rs:448:63
#typecheck
#Join on=(#1 = #6)
#  Get t0
#  Get t0
#----
#----
#----
#----

# TODO(mgree): panics in join_input_mapper.rs:236:22
#typecheck
#Join on=(#1 = (1 + #6))
#  Get t0
#  Get t0
#----
#----
#----
#----

############################################################################
# Case: Reduce
############################################################################

typecheck
Reduce aggregates=[max(#1), min(#1), count(*)] monotonic
  Constant // { types: "(text, bigint)" }
    - ("a", 2)
    - ("a", 4)
----
(Int64, Int64, Int64)

typecheck
Reduce group_by=[#0] aggregates=[max(#1), min(#1), sum(distinct #1)] monotonic exp_group_size=4
  Constant // { types: "(text, bigint)" }
    - ("a", 2)
    - ("a", 4)
----
(String, Int64, Int64, Numeric { max_scale: Some(NumericMaxScale(0)) })

# empty output type (no keys!)
typecheck
Distinct monotonic
  Constant // { types: "(text, bigint)" }
    - ("a", 2)
    - ("a", 4)
----
()

typecheck
Distinct group_by=[#0, #1] exp_group_size=4
  Constant // { types: "(text, bigint)" }
    - ("a", 2)
    - ("a", 4)
----
(String, Int64)

############################################################################
# Case: TopK
############################################################################

typecheck
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
----
(Int64, Int64)

typecheck
TopK group_by=[#1] limit=5 offset=2 monotonic
  Get t0
----
(Int64, Int64?, String)

typecheck
TopK group_by=[#1] limit=5 offset=2 monotonic exp_group_size=4
  Get t0
----
(Int64, Int64?, String)

typecheck
TopK group_by=[#3] limit=5 offset=2 monotonic
  Get t0
----
----
In the MIR term:
TopK group_by=[#3] limit=5 offset=2 monotonic
  Get t0


TopK group key component references invalid column 3 in columns: (Int64, Int64?, String)
----
----

typecheck
TopK group_by=[#0] order_by=[#3 asc nulls_first]
  Get t0
----
----
In the MIR term:
TopK group_by=[#0] order_by=[#3 asc nulls_first]
  Get t0


TopK ordering #3 asc nulls_first references invalid column 3
there are 3 columns: (Int64, Int64?, String)
----
----

typecheck
TopK group_by=[#0] order_by=[#1 asc nulls_first]
  Project (#0)
    Get t0
----
----
In the MIR term:
TopK group_by=[#0] order_by=[#1 asc nulls_first]
  Project (#0)
    Get t0


TopK ordering #1 asc nulls_first references invalid column 1
there is 1 column: (Int64)
----
----

############################################################################
# Case: Union
############################################################################

typecheck
Union
  Project (#0)
    Get t0
  Project (#1)
    Get t1
----
(Int64)

typecheck
Union
  Project (#0)
    Get t0
  Filter #0 IS NOT NULL
    Project (#1)
      Get t0
  Project (#1)
    Get t1
----
(Int64)

# appropriate union types (widening nullability)
typecheck
Union
  Project (#0)
    Get t0
  Project (#1)
    Get t0
  Project (#1)
    Get t1
----
(Int64?)

# TODO(mgree): another parser rejection
# union of incompatible types
#typecheck
#Union
#  Get t0
#  Get t1
#----
#----
#error
#----
#----

# another union of incompatible types
#typecheck
#Union
#  Project (#0)
#    Get t0
#  Project (#1)
#    Get t0
#  Project (#0)
#    Get t1
#----
#----
#error
#----
#----

############################################################################
# Case: Arrange
############################################################################

typecheck
ArrangeBy keys=[[#0], [#1]]
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
----
(Int64, Int64)

typecheck
ArrangeBy keys=[[#2]]
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
----
----
In the MIR term:
ArrangeBy keys=[[#2]]
  Constant
    - (1, 2)
    - (3, 4)


#2 references non-existent column 2
----
----
