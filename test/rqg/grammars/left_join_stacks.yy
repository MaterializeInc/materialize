# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

explain:
  EXPLAIN query
;

query:
  select
;

select:
  SELECT
    select_table_alias.col_name AS c01,
    select_table_alias.col_name AS c02,
    select_table_alias.col_name AS c03,
    select_table_alias.col_name AS c04,
    select_table_alias.col_name AS c05,
    select_table_alias.col_name AS c06,
    select_table_alias.col_name AS c07,
    select_table_alias.col_name AS c08,
    select_table_alias.col_name AS c09,
    select_table_alias.col_name AS c10
  FROM
    star.ft
    left_join1
    left_join2
    left_join3
    left_join4
    left_join5
    left_join6
  ORDER BY
    c01, c02, c03, c04, c05, c06, c07, c08, c09, c10
;

select_table_alias:
    ft
  | d1
  | d2
  | d3
  | d4
  | d5
  | d6
;

col_name:
    k
  | v
;

table_name:
    star.d1
  | star.d2
  | star.d3
  | star.d1_pk
  | star.d2_pk
  | star.d3_pk
;

left_join1:
  # Join against ft:
    LEFT JOIN table_name AS d1 ON( ft.col_name = d1.col_name )
  | LEFT JOIN table_name AS d1 ON( ft.col_name = d1.col_name AND ft.col_name = d1.col_name )
;

left_join2:
  # Join against ft:
    LEFT JOIN table_name AS d2 ON( ft.col_name = d2.col_name )
  | LEFT JOIN table_name AS d2 ON( ft.col_name = d2.col_name AND ft.col_name = d2.col_name )
  # Join against d1:
  | LEFT JOIN table_name AS d2 ON( d1.col_name = d2.col_name )
  | LEFT JOIN table_name AS d2 ON( d1.col_name = d2.col_name AND d1.col_name = d2.col_name )
;

left_join3:
  # Join against ft:
    LEFT JOIN table_name AS d3 ON( ft.col_name = d3.col_name )
  | LEFT JOIN table_name AS d3 ON( ft.col_name = d3.col_name AND ft.col_name = d3.col_name )
  # Join against d1:
  | LEFT JOIN table_name AS d3 ON( d1.col_name = d3.col_name )
  | LEFT JOIN table_name AS d3 ON( d1.col_name = d3.col_name AND d1.col_name = d3.col_name )
  # Join against d2:
  | LEFT JOIN table_name AS d3 ON( d2.col_name = d3.col_name )
  | LEFT JOIN table_name AS d3 ON( d2.col_name = d3.col_name AND d2.col_name = d3.col_name )
;

left_join4:
  # Join against ft:
    LEFT JOIN table_name AS d4 ON( ft.col_name = d4.col_name )
  | LEFT JOIN table_name AS d4 ON( ft.col_name = d4.col_name AND ft.col_name = d4.col_name )
  # Join against d1:
  | LEFT JOIN table_name AS d4 ON( d1.col_name = d4.col_name )
  | LEFT JOIN table_name AS d4 ON( d1.col_name = d4.col_name AND d1.col_name = d4.col_name )
  # Join against d2:
  | LEFT JOIN table_name AS d4 ON( d2.col_name = d4.col_name )
  | LEFT JOIN table_name AS d4 ON( d2.col_name = d4.col_name AND d2.col_name = d4.col_name )
  # Join against d3:
  | LEFT JOIN table_name AS d4 ON( d3.col_name = d4.col_name )
  | LEFT JOIN table_name AS d4 ON( d3.col_name = d4.col_name AND d3.col_name = d4.col_name )
;

left_join5:
  # Join against ft:
    LEFT JOIN table_name AS d5 ON( ft.col_name = d5.col_name )
  | LEFT JOIN table_name AS d5 ON( ft.col_name = d5.col_name AND ft.col_name = d5.col_name )
  # Join against d1:
  | LEFT JOIN table_name AS d5 ON( d1.col_name = d5.col_name )
  | LEFT JOIN table_name AS d5 ON( d1.col_name = d5.col_name AND d1.col_name = d5.col_name )
  # Join against d2:
  | LEFT JOIN table_name AS d5 ON( d2.col_name = d5.col_name )
  | LEFT JOIN table_name AS d5 ON( d2.col_name = d5.col_name AND d2.col_name = d5.col_name )
  # Join against d3:
  | LEFT JOIN table_name AS d5 ON( d3.col_name = d5.col_name )
  | LEFT JOIN table_name AS d5 ON( d3.col_name = d5.col_name AND d3.col_name = d5.col_name )
  # Join against d4:
  | LEFT JOIN table_name AS d5 ON( d4.col_name = d5.col_name )
  | LEFT JOIN table_name AS d5 ON( d4.col_name = d5.col_name AND d4.col_name = d5.col_name )
;

left_join6:
  # Join against ft:
    LEFT JOIN table_name AS d6 ON( ft.col_name = d6.col_name )
  | LEFT JOIN table_name AS d6 ON( ft.col_name = d6.col_name AND ft.col_name = d6.col_name )
  # Join against d1:
  | LEFT JOIN table_name AS d6 ON( d1.col_name = d6.col_name )
  | LEFT JOIN table_name AS d6 ON( d1.col_name = d6.col_name AND d1.col_name = d6.col_name )
  # Join against d2:
  | LEFT JOIN table_name AS d6 ON( d2.col_name = d6.col_name )
  | LEFT JOIN table_name AS d6 ON( d2.col_name = d6.col_name AND d2.col_name = d6.col_name )
  # Join against d3:
  | LEFT JOIN table_name AS d6 ON( d3.col_name = d6.col_name )
  | LEFT JOIN table_name AS d6 ON( d3.col_name = d6.col_name AND d3.col_name = d6.col_name )
  # Join against d4:
  | LEFT JOIN table_name AS d6 ON( d4.col_name = d6.col_name )
  | LEFT JOIN table_name AS d6 ON( d4.col_name = d6.col_name AND d4.col_name = d6.col_name )
  # Join against d5:
  | LEFT JOIN table_name AS d6 ON( d5.col_name = d6.col_name )
  | LEFT JOIN table_name AS d6 ON( d5.col_name = d6.col_name AND d5.col_name = d6.col_name )
;
