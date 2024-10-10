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
