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
> CREATE TABLE plurals (num text, noun text)
> INSERT INTO plurals VALUES ('one', 'sheep'), ('two', 'sheep'), ('one', 'mouse'), ('two', 'meeses')

> CREATE MATERIALIZED VIEW test1 (names_num, names_name, mods_num, mods_mod, nouns_num, nouns_noun) AS
  SELECT * FROM names, mods, plurals WHERE names.num = mods.num AND names.name = plurals.num;

> SELECT * FROM test1;
names_num names_name mods_num mods_mod nouns_num nouns_noun
-----------------------------------------------------------
1 one 1 odd one sheep
1 one 1 odd one mouse
2 two 2 even two sheep
2 two 2 even two meeses

> CREATE MATERIALIZED VIEW test2 (names_num, names_name, mods_num, mods_mod, plurals_num, plurals_noun) AS
  SELECT * FROM names, mods, plurals
  WHERE names.num = mods.num AND names.name = plurals.num AND plurals.num = 'one';

> SELECT * FROM test2;
names_num names_name mods_num mods_mod plurals_num plurals_noun
----------------------------------------------------------------
1 one 1 odd one sheep
1 one 1 odd one mouse

> CREATE MATERIALIZED VIEW test3 (names_num, names_name, mods_num, mods_mod, plurals_num, plurals_noun) AS
  SELECT * FROM names
  RIGHT JOIN mods ON names.num = mods.num
  LEFT JOIN plurals ON names.name = plurals.num;

> SELECT * FROM test3;
names_num names_name mods_num mods_mod plurals_num plurals_noun
---------------------------------------------------------------
<null> <null> 0 even <null> <null>
1 one 1 odd one sheep
1 one 1 odd one mouse
2 two 2 even two sheep
2 two 2 even two meeses

> CREATE MATERIALIZED VIEW test4 (names_num, names_name, mods_num, mods_mod, plurals_num, plurals_noun) AS
  SELECT * FROM names, mods, plurals as foo
  WHERE names.num = mods.num AND names.name = foo.num AND foo.num = 'one';

> SELECT * FROM test4;
names_num names_name mods_num mods_mod plurals_num plurals_noun
---------------------------------------------------------------
1 one 1 odd one sheep
1 one 1 odd one mouse
