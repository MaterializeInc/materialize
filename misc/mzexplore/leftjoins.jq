# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# leftjoins.jq — find unsimplified left joins

include "defs";

# PATTERN (in EXPLAIN syntax):
#
#      Union // { arity: 13 }
#        Map (null, null) // { arity: 13 }
#          Union // { arity: 11 }
#            Negate // { arity: 11 }
#              Project (#0..=#10) // { arity: 11 }
#                Get l9 // { arity: 13 }
#            Get l8 // { arity: 11 }
#        Get l9 // { arity: 13 }

def isleftjoin(expr):
      isunion(expr)
      # check antijoin
  and ismap(expr.Union.base) # TODO check for Nulls being tacked on there
  and isunion(expr.Union.base.Map.input)
  and (expr.Union.base.Map.input.Union.inputs | length == 1)
  and isnegate(expr.Union.base.Map.input.Union.base)
  and isproject(expr.Union.base.Map.input.Union.base.Negate.input)
      # check outer term
  and (expr.Union.inputs | length == 1)
      # check moral identity of negated inner term and outer term
;

[ ..
| select(isleftjoin(.))
| { "negated": .Union.base.Map.input.Union.base.Negate.input.Project.input,
    "outer": .Union.inputs[0],
    "inner": .Union.base.Map.input.Union.inputs[0] }
#| select(.outer == .negated)
#| { outer, inner }
] | length
