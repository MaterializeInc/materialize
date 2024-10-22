# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE names (num bigint, name text)
> INSERT INTO names VALUES (1, 'one'), (2, 'two'), (3, 'three')
> CREATE TABLE mods (num bigint, mod text)
> INSERT INTO mods VALUES (0, 'even'), (1, 'odd'), (2, 'even')

> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM names JOIN mods USING (num);

> SELECT * FROM test1;
num name mod
------------
1 one odd
2 two even

> CREATE MATERIALIZED VIEW test2 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names JOIN mods ON names.num = mods.num;

> SELECT * FROM test2;
names_num names_name mods_num mods_mod
--------------------------------------
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test3 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names, mods WHERE names.num = mods.num;

> SELECT * FROM test3;
names_num names_name mods_num mods_mod
--------------------------------------
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test4 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even';

> SELECT * FROM test4;
names_num names_name mods_num mods_mod
--------------------------------------
2 two 2 even

> CREATE MATERIALIZED VIEW test5 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names LEFT JOIN mods ON names.num = mods.num;

> SELECT * FROM test5;
names_num names_name mods_num mods_mod
--------------------------------------
1 one 1 odd
2 two 2 even
3 three <null> <null>

> CREATE MATERIALIZED VIEW test6 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names RIGHT JOIN mods ON names.num = mods.num;

> SELECT * FROM test6;
names_num names_name mods_num mods_mod
--------------------------------------
<null> <null> 0 even
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test7 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even';

> SELECT * FROM test7;
names_num names_name mods_num mods_mod
--------------------------------------
2 two 2 even

> CREATE MATERIALIZED VIEW test8 AS
  SELECT mods.* FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even';

> SELECT * FROM test8;
num mod
-------
2 even

> CREATE MATERIALIZED VIEW test9 AS
  SELECT foo.mod, foo.num, bar.name FROM names as bar, mods as foo
  WHERE bar.num = foo.num AND foo.mod = 'even';

> SELECT * FROM test9;
mod num name
------------
even 2 two

> CREATE MATERIALIZED VIEW test10 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names, mods;

> SELECT * FROM test10;
names_num names_name mods_num mods_mod
--------------------------------------
1 one 0 even
1 one 1 odd
1 one 2 even
2 two 0 even
2 two 1 odd
2 two 2 even
3 three 0 even
3 three 1 odd
3 three 2 even

> CREATE MATERIALIZED VIEW test11 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names CROSS JOIN mods;

> SELECT * FROM test11;
names_num names_name mods_num mods_mod
--------------------------------------
1 one 0 even
1 one 1 odd
1 one 2 even
2 two 0 even
2 two 1 odd
2 two 2 even
3 three 0 even
3 three 1 odd
3 three 2 even

> CREATE MATERIALIZED VIEW test12 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names LEFT JOIN mods ON 1 = 0;

> SELECT * FROM test12;
names_num names_name mods_num mods_mod
--------------------------------------
1 one <null> <null>
2 two <null> <null>
3 three <null> <null>

> CREATE MATERIALIZED VIEW test13 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names RIGHT JOIN mods ON 1 = 0;

> SELECT * FROM test13;
names_num names_name mods_num mods_mod
--------------------------------------
<null> <null> 0 even
<null> <null> 1 odd
<null> <null> 2 even

> CREATE MATERIALIZED VIEW test14 (names_num, names_name, mods_num, mods_mod) AS
  SELECT * FROM names FULL OUTER JOIN mods ON 1 = 0;

> SELECT * FROM test14;
names_num names_name mods_num mods_mod
--------------------------------------
1 one <null> <null>
2 two <null> <null>
3 three <null> <null>
<null> <null> 0 even
<null> <null> 1 odd
<null> <null> 2 even
