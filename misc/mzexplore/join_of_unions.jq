# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# join_of_unions.jq — finds and analyzes joins of unions in MIR plans

def is_join:
      type == "object"
  and has("Join")
;

def is_union:
      type == "object"
  and has("Union")
;

def is_arrangeby:
      type == "object"
  and has("ArrangeBy")
;

def is_union_or_arrangement_of_union:
      is_union
  or (is_arrangeby and .ArrangeBy.input | is_union)
;

def joins:
  if is_join
  then ., (.Join.inputs[] | joins)
  else .[]? | joins
  end
;

joins | values |
  { "file": input_filename
  , "inputs": .Join.inputs | length
  , "num_unions": .Join.inputs[] | [select(is_union_or_arrangement_of_union)] | length
  , "matched": .Join.inputs[] | any(is_union_or_arrangement_of_union)
  }
