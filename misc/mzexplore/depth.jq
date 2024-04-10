# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# depth.jq — calculating depth of an AST

include "defs";

def depth:
    [if is_mir_relexpr then 1 else 0 end +
     (if type == "object" or type == "array"
      then .[]? | depth
      else 0
      end)
    ] | max
;

depth
