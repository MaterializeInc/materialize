# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# variadic_outer_joins.jq — finds and analyzes outer join stacks in HIR (RAW PLANs)

include "defs";

def is_left_or_inner_join:
      type == "object"
  and has("Join")
  and (.Join.kind | . == "LeftOuter" or . == "Inner")
;

def is_join_stack:
      is_left_or_inner_join
  and (.Join.left | is_left_or_inner_join)
  and (.Join.left.Join.left | is_left_or_inner_join)
;

def unstacked_children:
    if is_left_or_inner_join
    then [.Join.left | unstacked_children[] , .Join.right | unstacked_children[] ]
    else [.]
    end
;

def maximal_stacks:
  if is_join_stack
  then .,                                    # return this stack of joins...
       (unstacked_children | maximal_stacks) # ...and search the children that aren't themselves stacked
  else .[]? | maximal_stacks
  end
;

def stack_info:
  if is_left_or_inner_join
  then .Join.kind as $kind | .Join.left | stack_info | (getpath([$kind]) |= .+1 | .total |= .+1)
  else { "LeftOuter": 0, "Inner": 0, "total": 0 }
  end
;

def collect_predicates:
  if has("CallVariadic")
     and .CallVariadic.func == "And"
  then .CallVariadic.exprs
  else [.]
  end
;

def literal_true:
      has("Literal")
  and .Literal[0].data == 2 # null encoding?
  and .Literal[1].scalar_type == "Bool"
;

def is_column_ref:
  has("Column") and .Column.level == 0
;

def simple_eq:
      has("CallBinary")
  and .CallBinary.func == "Eq"
  and (.CallBinary.expr1 | is_column_ref)
  and (.CallBinary.expr2 | is_column_ref)
;

def is_column_ref_or_func_of_column_refs:
     is_column_ref
  or (has("CallUnary")    and (.CallUnary.expr | is_column_ref))
  or (has("CallBinary")   and ((.CallBinary.expr1 | is_column_ref) or (.CallBinary.expr2 | is_column_ref))) # what else do we want to allow?
  or (has("CallVariadic") and (.CallVariadic.exprs | any(is_column_ref)))
;

def complex_eq:
      has("CallBinary")
  and .CallBinary.func == "Eq"
  and (.CallBinary.expr1 | is_column_ref_or_func_of_column_refs)
  and (.CallBinary.expr2 | is_column_ref_or_func_of_column_refs)
;


def join_predicates:
  if is_left_or_inner_join
  then [(.Join.on | collect_predicates[]),
        (.Join.left | join_predicates[]),
        (.Join.right | join_predicates[])
       ]
  else []
  end
;

def compute_predicate_info:
    join_predicates
  | reduce .[] as $pred
      ({"literal_true": 0, "simple_eq": 0, "complex_eq": 0 };
       if $pred | literal_true
       then .literal_true += 1
       elif $pred | simple_eq
       then .simple_eq += 1
       elif $pred | complex_eq
       then .complex_eq += 1
       else .
       end)
;

maximal_stacks | values | { "file": input_filename, "size": . | stack_info, "predicates": . | compute_predicate_info }
