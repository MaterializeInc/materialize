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
  SELECT
    ft.ft_col AS c01,
    ft.ft_col AS c02,
    dim_alias.dx_col AS c03,
    dim_alias.dx_col AS c04,
    dim_alias.dx_col AS c05,
    dim_alias.dx_col AS c06,
    dim_alias.dx_col AS c07,
    dim_alias.dx_col AS c08,
    dim_alias.dx_col AS c09,
    dim_alias.dx_col AS c10
  FROM
    star.ft as ft
    left_join1
    left_join2
    left_join3
    left_join4
    left_join5
    left_join6
  WHERE
    where_clause
  ORDER BY
    c01, c02, c03, c04, c05, c06, c07, c08, c09, c10
;

where_clause:
    true | true | true | true | true
  | ft.ft_col IS NOT NULL bool_op where_clause
  | ft.ft_col > 1 bool_op where_clause
  | dim_alias.dx_col < 7 bool_op where_clause
;

bool_op:
    AND | AND | AND | AND | AND
  | OR
;

dim_alias:
    d1
  | d2
  | d3
  | d4
  | d5
  | d6
;

ft_col:
    k
  | v
  | fk1
  | fk2
;

dx_col:
    v
  | pk1
  | pk2
;

fk_col:
    fk1
  | fk2
;

pk_col:
    k1
  | k2
;

dim_table:
    star.d1
  | star.d2
  | star.d3
  | star.d1_pk
  | star.d2_pk
  | star.d3_pk
;

left_join1:
  # Join against ft:
    LEFT JOIN dim_table AS d1 ON( ft.fk1 = d1.pk1 )
  | LEFT JOIN dim_table AS d1 ON( ft.fk1 = d1.pk1 AND ft.fk2 = d1.pk2 )
  | LEFT JOIN dim_table AS d1 ON( ft.fk1 = d1.pk2 AND ft.fk2 = d1.pk1 )
  | LEFT JOIN dim_table AS d1 ON( ft.fk1 = d1.pk1 AND ft.fk1 = d1.pk2 )
;

left_join2:
  # Join against ft:
    LEFT JOIN dim_table AS d2 ON( ft.fk1 = d2.pk1 )
  | LEFT JOIN dim_table AS d2 ON( ft.fk1 = d2.pk1 AND ft.fk2 = d2.pk2 )
  | LEFT JOIN dim_table AS d2 ON( ft.fk1 = d2.pk2 AND ft.fk2 = d2.pk1 )
  | LEFT JOIN dim_table AS d2 ON( ft.fk1 = d2.pk1 AND ft.fk1 = d2.pk2 )
  # Join against d1:
  | LEFT JOIN dim_table AS d2 ON( d1.pk1 = d2.pk1 )
  | LEFT JOIN dim_table AS d2 ON( d1.pk1 = d2.pk1 AND d1.pk2 = d2.pk2 )
  | LEFT JOIN dim_table AS d2 ON( d1.pk1 = d2.pk2 AND d1.pk2 = d2.pk1 )
  | LEFT JOIN dim_table AS d2 ON( d1.pk1 = d2.pk1 AND d1.pk1 = d2.pk2 )
;

left_join3:
  # Join against ft:
    LEFT JOIN dim_table AS d3 ON( ft.fk1 = d3.pk1 )
  | LEFT JOIN dim_table AS d3 ON( ft.fk1 = d3.pk1 AND ft.fk2 = d3.pk2 )
  | LEFT JOIN dim_table AS d3 ON( ft.fk1 = d3.pk2 AND ft.fk2 = d3.pk1 )
  | LEFT JOIN dim_table AS d3 ON( ft.fk1 = d3.pk1 AND ft.fk1 = d3.pk2 )
  | LEFT JOIN dim_table AS d3 ON( ft.fk1 = d3.pk1 AND ft.fk2 = d3.pk1 ) # VOJ lowering for this is excluded in materialize#26709
  # Join against d1:
  | LEFT JOIN dim_table AS d3 ON( d1.pk1 = d3.pk1 )
  | LEFT JOIN dim_table AS d3 ON( d1.pk1 = d3.pk1 AND d1.pk2 = d3.pk2 )
  | LEFT JOIN dim_table AS d3 ON( d1.pk1 = d3.pk2 AND d1.pk2 = d3.pk1 )
  | LEFT JOIN dim_table AS d3 ON( d1.pk1 = d3.pk1 AND d1.pk1 = d3.pk2 )
  # Join against d2:
  | LEFT JOIN dim_table AS d3 ON( d2.pk1 = d3.pk1 )
  | LEFT JOIN dim_table AS d3 ON( d2.pk1 = d3.pk1 AND d2.pk2 = d3.pk2 )
  | LEFT JOIN dim_table AS d3 ON( d2.pk1 = d3.pk2 AND d2.pk2 = d3.pk1 )
  | LEFT JOIN dim_table AS d3 ON( d2.pk1 = d3.pk1 AND d2.pk1 = d3.pk2 )
;

left_join4:
  # Join against ft:
    LEFT JOIN dim_table AS d4 ON( ft.fk1 = d4.pk1 )
  | LEFT JOIN dim_table AS d4 ON( ft.fk1 = d4.pk1 AND ft.fk2 = d4.pk2 )
  | LEFT JOIN dim_table AS d4 ON( ft.fk1 = d4.pk2 AND ft.fk2 = d4.pk1 )
  | LEFT JOIN dim_table AS d4 ON( ft.fk1 = d4.pk1 AND ft.fk1 = d4.pk2 )
  # Join against d1:
  | LEFT JOIN dim_table AS d4 ON( d1.pk1 = d4.pk1 )
  | LEFT JOIN dim_table AS d4 ON( d1.pk1 = d4.pk1 AND d1.pk2 = d4.pk2 )
  | LEFT JOIN dim_table AS d4 ON( d1.pk1 = d4.pk2 AND d1.pk2 = d4.pk1 )
  | LEFT JOIN dim_table AS d4 ON( d1.pk1 = d4.pk1 AND d1.pk1 = d4.pk2 )
  # Join against d2:
  | LEFT JOIN dim_table AS d4 ON( d2.pk1 = d4.pk1 )
  | LEFT JOIN dim_table AS d4 ON( d2.pk1 = d4.pk1 AND d2.pk2 = d4.pk2 )
  | LEFT JOIN dim_table AS d4 ON( d2.pk1 = d4.pk2 AND d2.pk2 = d4.pk1 )
  | LEFT JOIN dim_table AS d4 ON( d2.pk1 = d4.pk1 AND d2.pk1 = d4.pk2 )
  # Join against d3:
  | LEFT JOIN dim_table AS d4 ON( d3.pk1 = d4.pk1 )
  | LEFT JOIN dim_table AS d4 ON( d3.pk1 = d4.pk1 AND d3.pk2 = d4.pk2 )
  | LEFT JOIN dim_table AS d4 ON( d3.pk1 = d4.pk2 AND d3.pk2 = d4.pk1 )
  | LEFT JOIN dim_table AS d4 ON( d3.pk1 = d4.pk1 AND d3.pk1 = d4.pk2 )
;

left_join5:
  # Join against ft:
    LEFT JOIN dim_table AS d5 ON( ft.fk1 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( ft.fk1 = d5.pk1 AND ft.fk2 = d5.pk2 )
  | LEFT JOIN dim_table AS d5 ON( ft.fk1 = d5.pk2 AND ft.fk2 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( ft.fk1 = d5.pk1 AND ft.fk1 = d5.pk2 )
  # Join against d1:
  | LEFT JOIN dim_table AS d5 ON( d1.pk1 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( d1.pk1 = d5.pk1 AND d1.pk2 = d5.pk2 )
  | LEFT JOIN dim_table AS d5 ON( d1.pk1 = d5.pk2 AND d1.pk2 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( d1.pk1 = d5.pk1 AND d1.pk1 = d5.pk2 )
  # Join against d2:
  | LEFT JOIN dim_table AS d5 ON( d2.pk1 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( d2.pk1 = d5.pk1 AND d2.pk2 = d5.pk2 )
  | LEFT JOIN dim_table AS d5 ON( d2.pk1 = d5.pk2 AND d2.pk2 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( d2.pk1 = d5.pk1 AND d2.pk1 = d5.pk2 )
  # Join against d3:
  | LEFT JOIN dim_table AS d5 ON( d3.pk1 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( d3.pk1 = d5.pk1 AND d3.pk2 = d5.pk2 )
  | LEFT JOIN dim_table AS d5 ON( d3.pk1 = d5.pk2 AND d3.pk2 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( d3.pk1 = d5.pk1 AND d3.pk1 = d5.pk2 )
  # Join against d4:
  | LEFT JOIN dim_table AS d5 ON( d4.pk1 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( d4.pk1 = d5.pk1 AND d4.pk2 = d5.pk2 )
  | LEFT JOIN dim_table AS d5 ON( d4.pk1 = d5.pk2 AND d4.pk2 = d5.pk1 )
  | LEFT JOIN dim_table AS d5 ON( d4.pk1 = d5.pk1 AND d4.pk1 = d5.pk2 )
;

left_join6:
  # Join against ft:
    LEFT JOIN dim_table AS d6 ON( ft.fk1 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( ft.fk1 = d6.pk1 AND ft.fk2 = d6.pk2 )
  | LEFT JOIN dim_table AS d6 ON( ft.fk1 = d6.pk2 AND ft.fk2 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( ft.fk1 = d6.pk1 AND ft.fk1 = d6.pk2 )
  # Join against d1:
  | LEFT JOIN dim_table AS d6 ON( d1.pk1 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d1.pk1 = d6.pk1 AND d1.pk2 = d6.pk2 )
  | LEFT JOIN dim_table AS d6 ON( d1.pk1 = d6.pk2 AND d1.pk2 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d1.pk1 = d6.pk1 AND d1.pk1 = d6.pk2 )
  # Join against d2:
  | LEFT JOIN dim_table AS d6 ON( d2.pk1 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d2.pk1 = d6.pk1 AND d2.pk2 = d6.pk2 )
  | LEFT JOIN dim_table AS d6 ON( d2.pk1 = d6.pk2 AND d2.pk2 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d2.pk1 = d6.pk1 AND d2.pk1 = d6.pk2 )
  # Join against d3:
  | LEFT JOIN dim_table AS d6 ON( d3.pk1 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d3.pk1 = d6.pk1 AND d3.pk2 = d6.pk2 )
  | LEFT JOIN dim_table AS d6 ON( d3.pk1 = d6.pk2 AND d3.pk2 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d3.pk1 = d6.pk1 AND d3.pk1 = d6.pk2 )
  # Join against d4:
  | LEFT JOIN dim_table AS d6 ON( d4.pk1 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d4.pk1 = d6.pk1 AND d4.pk2 = d6.pk2 )
  | LEFT JOIN dim_table AS d6 ON( d4.pk1 = d6.pk2 AND d4.pk2 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d4.pk1 = d6.pk1 AND d4.pk1 = d6.pk2 )
  # Join against d5:
  | LEFT JOIN dim_table AS d6 ON( d5.pk1 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d5.pk1 = d6.pk1 AND d5.pk2 = d6.pk2 )
  | LEFT JOIN dim_table AS d6 ON( d5.pk1 = d6.pk2 AND d5.pk2 = d6.pk1 )
  | LEFT JOIN dim_table AS d6 ON( d5.pk1 = d6.pk1 AND d5.pk1 = d6.pk2 )
;
