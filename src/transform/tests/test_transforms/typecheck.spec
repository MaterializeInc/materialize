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

# TODO: Constant, Let, LetRec, Map, FlatMap, Filter, Join, Reduce, TopK, Negate, Threshold, Union, ArrangeBy

############################################################################
# Case: Get
############################################################################
# TODO(mgree): would be good to include unbound variables, but the expr-parser doesn't support that
# TODO(mgree): would be good to include a case where the annotated type is wrong, but the parser currently fills in the correct type

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
