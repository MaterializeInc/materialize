# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Check that decimal columns can properly be joined
#

#
# Different scales, same precision
#

> CREATE TABLE d5_2 (f1 DECIMAL(5,2));

> INSERT INTO d5_2 VALUES (12.34);

> CREATE TABLE d6_2 (f1 DECIMAL(6,2));

> INSERT INTO d6_2 VALUES (12.34);

> SELECT * FROM d5_2 JOIN d6_2 ON (d5_2.f1 = d6_2.f1);
12.34 12.34

#
# Same scale, different precisions
#

> CREATE TABLE d5_3(f1 DECIMAL(5,3));

> INSERT INTO d5_3 VALUES (12.340);

> SELECT * FROM d5_2 JOIN d5_3 ON (d5_2.f1 = d5_3.f1);
12.34 12.34


#
# Use of CAST in the join condition
#

> CREATE TABLE d5_3a (f1 DECIMAL(5,3));

> INSERT INTO d5_3a VALUES (12.341);

# cast to reduce precision
> SELECT * FROM d5_2 JOIN d5_3a ON (d5_2.f1 = d5_3a.f1::DECIMAL(5,2));
12.34 12.341

> SELECT * FROM d5_2 JOIN d5_3a ON (d5_2.f1 = ROUND(d5_3a.f1,2));
12.34 12.341

# cast to increase precision
> SELECT * FROM d5_2 JOIN d5_3 ON (d5_2.f1::DECIMAL(5,4) = d5_3.f1);
12.34 12.34

#
# Join between string and decimal
#

> CREATE TABLE s (f1 STRING);

> INSERT INTO s VALUES ('012.34'), ('12.34'), ('12.340');

> SELECT * FROM d5_2 JOIN s ON (d5_2.f1 = s.f1::decimal);
12.34 012.34
12.34 12.34
12.34 12.340

#
# Join between integer and decimal
#

> CREATE TABLE d5_2b (f1 DECIMAL(5,2));

> INSERT INTO d5_2b VALUES (12);

> CREATE TABLE i (f1 INTEGER);

> INSERT INTO i VALUES (12);

> SELECT * FROM d5_2b JOIN i ON d5_2b.f1 = i.f1;
12 12


#
# Join between float and decimal
#

> CREATE TABLE f (f1 DOUBLE);

> INSERT INTO f VALUES (12.34);

> SELECT * FROM d5_2 JOIN f ON d5_2.f1 = f.f1
12.34 12.34

#
# Join using an arithmetic expression
#

> CREATE TABLE d5_2c (f1 DECIMAL(5,2));

> INSERT INTO d5_2c VALUES (123.4);

> SELECT * FROM d5_2 JOIN d5_2c ON (d5_2.f1 = d5_2c.f1 / 10);
12.34 123.4

#
# More join semantics
#

> CREATE TABLE l (la numeric, lb text)
> CREATE TABLE r (ra numeric, rb text)
> INSERT INTO l VALUES (1.0, 'l1'), (2.0, 'l2'), (3.0, 'l3')
> INSERT INTO r VALUES (1.00, 'r1'), (3.00, 'r3'), (4.00, 'r4')

> SELECT * FROM l LEFT JOIN r ON l.la = r.ra
1  l1  1     r1
2  l2  <null>  <null>
3  l3  3     r3

> SELECT * FROM l RIGHT JOIN r ON l.la = r.ra
<null>  <null>  4  r4
1       l1      1  r1
3       l3      3  r3

# Test that projections work through join plans with scalars.
> SELECT l1.la, l2.lb, l3.lb
    FROM l as l1, l as l2, l as l3
    WHERE l1.la + 1 = l2.la AND l3.la = l1.la + l2.la
1  l2  l3

# Test that join plans with scalars work in subqueries
> SELECT l1.la, l1.lb
    FROM l as l1
    WHERE l1.la IN (
        SELECT l2.la + 1
        FROM l AS l2
        WHERE l2.la IN (
            SELECT l3.la + 1
            FROM l as l3
        )
    )
3  l3

# Test that multiplicities in outer joins are preserved.
# Fixed in #2724.
> CREATE TABLE l2 (la numeric, lb text)

> CREATE TABLE r2 (ra numeric, rb text)

> INSERT INTO l2 VALUES (1.0, 'l1'), (2.0, 'l2'),  (2.0, 'l2'), (3.0, 'l3')

> INSERT INTO r2 VALUES (1.00, 'r1'), (3.00, 'r3'), (4.00, 'r4'), (4.00, 'r4')

> SELECT * FROM l2 LEFT JOIN r2 ON l2.la = r2.ra
1  l1  1        r1
2  l2  <null>   <null>
2  l2  <null>   <null>
3  l3  3        r3

> SELECT * FROM l2 RIGHT JOIN r2 ON l2.la = r2.ra
<null>  <null>  4  r4
<null>  <null>  4  r4
1       l1      1  r1
3       l3      3  r3

> SELECT * FROM l WHERE EXISTS (SELECT * from l as l2 LEFT JOIN r ON l.la = r.ra)
1  l1
2  l2
3  l3

> SELECT * FROM r WHERE EXISTS (SELECT * from l RIGHT JOIN r as r2 ON l.la = r.ra)
1  r1
3  r3
4  r4

> SELECT * FROM l INNER JOIN r ON mod(l.la, 2) = mod(r.ra, 2)
1 l1 1 r1
1 l1 3 r3
2 l2 4 r4
3 l3 1 r1
3 l3 3 r3

# Test implicitly coercible USING columns
> CREATE TABLE small_l (la int, lb text)

> INSERT INTO small_l VALUES (1, 'l1'), (2, 'l2'), (3, 'l3')

> CREATE TABLE big_l (la numeric, lb text)

> INSERT INTO big_l VALUES (1.0, 'big_l1'), (3.0, 'bigl_3'), (5.0, 'bigl_5')

> SELECT la, small_l.lb, big_l.lb FROM small_l JOIN big_l USING (la)
1  l1  big_l1
3  l3  bigl_3

#
# numeric list joins
#

> CREATE TABLE numeric_list_l (la numeric list, lb text)
> CREATE TABLE numeric_list_r (ra numeric list, rb text)
> INSERT INTO numeric_list_l VALUES (LIST[1.0], 'l1'), (LIST[2.0], 'l2'), (LIST[3.0], 'l3')
> INSERT INTO numeric_list_r VALUES (LIST[1.00], 'r1'), (LIST[3.00], 'r3'), (LIST[4.00], 'r4')

> SELECT la::text, lb, ra::text, rb FROM numeric_list_l AS l LEFT JOIN numeric_list_r AS r ON l.la = r.ra
{1.0}  l1  {1.00}   r1
{2.0}  l2  <null>   <null>
{3.0}  l3  {3.00}   r3

> SELECT la::text, lb, ra::text, rb FROM numeric_list_l AS l RIGHT JOIN numeric_list_r AS r ON l.la = r.ra
<null>  <null>  {4.00}  r4
{1.0}     l1    {1.00}  r1
{3.0}     l3    {3.00}  r3

# Test that multiplicities in outer joins are preserved.
> CREATE TABLE numeric_list_l2 (la numeric list, lb text)
> CREATE TABLE numeric_list_r2 (ra numeric list, rb text)

> INSERT INTO numeric_list_l2 VALUES (LIST[1.0], 'l1'), (LIST[2.0], 'l2'),  (LIST[2.0], 'l2'), (LIST[3.0], 'l3')
> INSERT INTO numeric_list_r2 VALUES (LIST[1.00], 'r1'), (LIST[3.00], 'r3'), (LIST[4.00], 'r4'), (LIST[4.00], 'r4')

> SELECT la::text, lb, ra::text, rb FROM numeric_list_l2 AS l2 LEFT JOIN numeric_list_r2 AS r2 ON l2.la = r2.ra
{1.0}  l1  {1.00}  r1
{2.0}  l2  <null>  <null>
{2.0}  l2  <null>  <null>
{3.0}  l3  {3.00}  r3

> SELECT la::text, lb, ra::text, rb FROM numeric_list_l2 AS l2 RIGHT JOIN numeric_list_r2 AS r2 ON l2.la = r2.ra
<null> <null>  {4.00}  r4
<null> <null>  {4.00}  r4
{1.0}  l1      {1.00}  r1
{3.0}  l3      {3.00}  r3

> SELECT la::text, lb FROM numeric_list_l WHERE EXISTS (SELECT * from numeric_list_l AS l LEFT JOIN numeric_list_r AS r ON l.la = r.ra)
{1.0}  l1
{2.0}  l2
{3.0}  l3

> SELECT ra::text, rb FROM numeric_list_r WHERE EXISTS (SELECT * from numeric_list_l AS l RIGHT JOIN numeric_list_r AS r ON l.la = r.ra)
{1.00}  r1
{3.00}  r3
{4.00}  r4

# USING
> CREATE TABLE using_l (la numeric list, lb text)

> INSERT INTO using_l VALUES (LIST[1.0], 'using_l1'), (LIST[3.0], 'using_l3'), (LIST[5.0], 'using_l5')

> SELECT la::text, l.lb, using_l.lb FROM numeric_list_l AS l JOIN using_l USING (la)
{1}  l1  using_l1
{3}  l3  using_l3
