# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# defs.jq — utility functions for working with MIR ASTs

def typename(expr):
    if expr | type == "object"
    then expr.keys[0]
    else expr
    end
;

def iscall(expr):
    expr | type == "object" and (has("CallVariadic") or has("CallBinary") or has("CallUnary"))
;

def iscolumn(expr):
    expr | type == "object" and has("Column")
;

def isliteral(expr):
    expr | type == "object" and has("Literal")
;

def isunion(expr):
  expr | type == "object" and has("Union")
;

def ismap(expr):
  expr | type == "object" and has("Map")
;

def isnegate(expr):
  expr | type == "object" and has("Negate")
;

def isproject(expr):
  expr | type == "object" and has("Project")
;

def parts(expr):
    .CallUnary?.expr[]?, .CallBinary?.expr1[]?, .CallBinary?.expr2[]?, .CallVariadic?.exprs[]?
;

def subexprs(expr):
    if expr | type == "object"
    then
      if has("CallUnary")
      then [.CallUnary.expr]
      elif has("CallBinary")
      then [.CallBinary | .expr1, .expr2 ]
      elif has("CallVariadic")
      then .exprs[]
      else []
      end
    else []
    end
;

def summarize(expr):
    [ expr | group_by(.)[] | { "term": .[0], "occurrences": length } ] | sort_by(.occurrences) | reverse
;
