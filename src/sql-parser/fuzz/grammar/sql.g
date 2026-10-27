# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Grammar for Materialize SQL round-trip fuzzing.
#
# `build.rs` compiles this file into a rule table (`$OUT_DIR/grammar.rs`); the
# `grammar` fuzz target walks the table, mapping libFuzzer bytes to production
# choices, then checks that `parse -> print -> reparse` preserves the AST. The
# grammar is the single source of truth and is compiled at build time, so there
# is no generated file to keep in sync. Edit this file and rebuild.
#
# Format:
#   <rule> = <alt> | <alt> | ... ;
# An <alt> is a whitespace-separated sequence of items; an item is one of:
#   "literal"     a terminal, emitted verbatim (may contain | ; = inside quotes)
#   other_rule    a reference to another rule (recursively expanded)
#   @ident        a generated identifier (bare name or quoted collision)
#   @kw           a bare keyword used as an identifier (printer-quoting stress)
#   @int          a small integer literal
#   @str          a string literal (with lexing/escaping edge cases)
# `#` starts a comment line. The start symbol is the first rule (`statement`).
# Bias is expressed by repeating an alternative (each alt is chosen uniformly).

statement =
    query
  | query
  | query
  | "SELECT " select_list
  | query " UNION " set_rhs
  | query " UNION ALL " set_rhs
  | query " INTERSECT " set_rhs
  | query " EXCEPT " set_rhs
  | "INSERT INTO " table_name " " query
  | "CREATE VIEW " ident " AS " query
  | "CREATE MATERIALIZED VIEW " ident " AS " query
  | "EXPLAIN " query
  | "SUBSCRIBE (" query ")"
  | "DELETE FROM " table_name " WHERE " expr
  | "UPDATE " table_name " SET " ident " = " expr " WHERE " expr
  ;

set_rhs =
    query
  | "SELECT " select_list
  ;

query =
    "SELECT " distinct_opt select_list " FROM " from_item where_opt group_opt having_opt order_opt limit_opt
  ;

distinct_opt =
    ""
  | ""
  | "DISTINCT "
  ;

select_list =
    "*"
  | select_item
  | select_item ", " select_list
  ;

select_item =
    expr
  | expr " AS " ident
  ;

from_item =
    table_name
  | table_name
  | table_name " AS " ident
  | from_item " JOIN " from_item " ON " expr
  | from_item " LEFT JOIN " from_item " ON " expr
  | from_item " CROSS JOIN " from_item
  | "(" query ") AS " ident
  ;

table_name =
    ident
  | ident "." ident
  ;

where_opt =
    ""
  | ""
  | " WHERE " expr
  ;

group_opt =
    ""
  | ""
  | " GROUP BY " expr_list
  ;

having_opt =
    ""
  | " HAVING " expr
  ;

order_opt =
    ""
  | ""
  | " ORDER BY " order_list
  ;

order_list =
    expr order_dir
  | expr order_dir ", " order_list
  ;

order_dir =
    ""
  | " ASC"
  | " DESC"
  | " ASC NULLS LAST"
  | " DESC NULLS FIRST"
  ;

limit_opt =
    ""
  | ""
  | " LIMIT " @int
  | " LIMIT " @int " OFFSET " @int
  ;

expr_list =
    expr
  | expr ", " expr_list
  ;

expr =
    leaf
  | leaf
  | expr " " binop " " expr
  | unop expr
  | "(" expr ")"
  | expr "::" type
  | "CAST(" expr " AS " type ")"
  | expr " IS NULL"
  | expr " IS NOT NULL"
  | expr " IS DISTINCT FROM " expr
  | expr " BETWEEN " expr " AND " expr
  | expr " IN (" expr_list ")"
  | expr " IN (" query ")"
  | expr " NOT IN (" expr_list ")"
  | "EXISTS (" query ")"
  | expr " LIKE " expr
  | expr " LIKE " expr " ESCAPE " expr
  | expr " ILIKE " expr
  | expr quant_op " " quant " (" query ")"
  | expr quant_op " " quant " (ARRAY[" expr_list "])"
  | func_call
  | case_expr
  | "ARRAY[" expr_list "]"
  | "ARRAY(" query ")"
  | "LIST[" expr_list "]"
  | "ROW(" expr_list ")"
  | "(" query ")"
  # Postfix forms: `COLLATE`/`AT TIME ZONE` (loose, `PostfixCollateAt`) and
  # subscript/field-access (tight, `PostfixSubscriptCast`). These stress the
  # printer's receiver/operand parenthesization the most.
  | expr " COLLATE " collation
  | expr " AT TIME ZONE " expr
  | expr subscript
  | "(" expr ")." ident
  | "(" expr ").*"
  | "MAP[" map_entries "]"
  | "MAP(" query ")"
  | "NULLIF(" expr ", " expr ")"
  | special_func
  | typed_literal
  ;

leaf =
    ident
  | qualified
  | @int
  | @str
  | "true"
  | "false"
  | "null"
  | param
  ;

qualified =
    ident "." ident
  ;

param =
    "$" @int
  ;

quant =
    "ANY"
  | "ALL"
  | "SOME"
  ;

quant_op =
    " ="
  | " <>"
  | " <"
  | " >"
  | " <="
  | " >="
  ;

binop =
    "+" | "-" | "*" | "/" | "%" | "||" | "=" | "<>" | "!=" | "<" | ">" | "<=" | ">="
  | "AND" | "OR" | "->" | "->>" | "#>" | "#>>" | "@>" | "<@"
  ;

unop =
    "-" | "+" | "NOT " | "~"
  ;

type =
    "int4" | "int8" | "integer" | "text" | "boolean" | "double precision" | "numeric"
  | "numeric(10, 2)" | "date" | "timestamp" | "timestamptz" | "jsonb" | "uuid"
  | type "[]"
  | type " list"
  ;

func_call =
    func_name "(" ")"
  | func_name "(" expr_list ")"
  | func_name "(DISTINCT " expr_list ")"
  | func_name "(" expr_list ") OVER ()"
  | func_name "(" expr_list ") OVER (PARTITION BY " expr_list ")"
  | func_name "(" expr_list ") OVER (ORDER BY " order_list " " frame ")"
  | func_name "(" expr_list ") FILTER (WHERE " expr ")"
  | "count(*)"
  ;

frame =
    "ROWS BETWEEN " frame_bound " AND " frame_bound
  | "RANGE BETWEEN " frame_bound " AND " frame_bound
  | "GROUPS BETWEEN " frame_bound " AND " frame_bound
  | "ROWS " frame_bound
  ;

frame_bound =
    "UNBOUNDED PRECEDING"
  | "CURRENT ROW"
  | @int " PRECEDING"
  | @int " FOLLOWING"
  | "UNBOUNDED FOLLOWING"
  ;

collation =
    ident
  | ident "." ident
  ;

subscript =
    "[" expr "]"
  | "[" expr ":" expr "]"
  | "[" expr ":]"
  | "[:" expr "]"
  ;

map_entries =
    expr " => " expr
  | expr " => " expr ", " map_entries
  ;

special_func =
    "position(" expr " IN " expr ")"
  | "extract(" extract_field " FROM " expr ")"
  | "substring(" expr " FROM " expr ")"
  | "substring(" expr " FROM " expr " FOR " expr ")"
  | "trim(" expr ")"
  | "trim(BOTH " expr " FROM " expr ")"
  | "trim(LEADING " expr " FROM " expr ")"
  ;

extract_field =
    "'year'" | "'month'" | "'day'" | "'hour'" | "'epoch'" | "'dow'"
  ;

typed_literal =
    "DATE " @str
  | "TIMESTAMP " @str
  | "TIME " @str
  | "INTERVAL " @str
  ;

func_name =
    "count" | "sum" | "max" | "min" | "abs" | "lower" | "upper" | "coalesce"
  | "length" | "greatest" | "least" | "generate_series" | "f"
  ;

case_expr =
    "CASE WHEN " expr " THEN " expr " ELSE " expr " END"
  | "CASE WHEN " expr " THEN " expr " END"
  | "CASE " expr " WHEN " expr " THEN " expr " ELSE " expr " END"
  ;

# Identifiers, biased ~4:1 toward ordinary names over bare keywords (each
# alternative is equally likely); the bare-keyword case is the printer-quoting
# stressor (does the printer keep a keyword-as-identifier unambiguous?).
ident =
    @ident | @ident | @ident | @ident | @kw
  ;
