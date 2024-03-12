# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# filters.jq — summarize all filter predicates (and sub-predicates)

include "defs";

[ ..
| .Filter?
| values
| .predicates[]
| ..
| select(iscall(.))
| walk(if iscolumn(.)
       then "COLUMN REFERENCE"
       elif isliteral(.)
       then "LITERAL " + typename(.Literal[1].scalar_type) +
            (if .Literal[1].nullable then "?" else "" end)
       else .
       end)
] | summarize(.)
