# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

$ set names-schema={
    "type": "record",
    "name": "na",
    "fields": [
        {"name": "num", "type": "long"},
        {"name": "name", "type": "string"}
    ]
  }

$ kafka-ingest format=avro topic=names schema=${names-schema}
{"num": 1, "name": "one"}
{"num": 2, "name": "two"}
{"num": 3, "name": "three"}

> CREATE SOURCE names FROM 'kafka://${testdrive.kafka-addr}/names' USING SCHEMA '${names-schema}'

$ set mods-schema={
    "type": "record",
    "name": "na",
    "fields": [
        {"name": "num", "type": "long"},
        {"name": "mod", "type": "string"}
    ]
  }

$ kafka-ingest format=avro topic=mods schema=${mods-schema}
{"num": 0, "mod": "even"}
{"num": 1, "mod": "odd"}
{"num": 2, "mod": "even"}

> CREATE SOURCE mods FROM 'kafka://${testdrive.kafka-addr}/mods' USING SCHEMA '${mods-schema}'

> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM names JOIN mods USING (num);

> PEEK test1;
num name mod
------------
1 one odd
2 two even

> CREATE MATERIALIZED VIEW test2 AS
  SELECT * FROM names JOIN mods ON names.num = mods.num;

> PEEK test2;
num name num mod
----------------
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test3 AS
  SELECT * FROM names, mods WHERE names.num = mods.num;

> PEEK test3;
num name num mod
----------------
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test4 AS
  SELECT * FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even';

> PEEK test4;
num name num mod
----------------
2 two 2 even

> CREATE MATERIALIZED VIEW test5 AS
  SELECT * FROM names LEFT JOIN mods ON names.num = mods.num;

> PEEK test5;
num name num mod
----------------
1 one 1 odd
2 two 2 even
3 three <null> <null>

> CREATE MATERIALIZED VIEW test6 AS
  SELECT * FROM names RIGHT JOIN mods ON names.num = mods.num;

> PEEK test6;
num name num mod
----------------
<null> <null> 0 even
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test7 AS
  SELECT * FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even';

> PEEK test7;
num name num mod
----------------
2 two 2 even

> CREATE MATERIALIZED VIEW test8 AS
  SELECT mods.* FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even';

> PEEK test8;
num mod
-------
2 even

> CREATE MATERIALIZED VIEW test9 AS
  SELECT foo.mod, foo.num, bar.name FROM names as bar, mods as foo WHERE bar.num = foo.num AND foo.mod = 'even';

> PEEK test9;
mod num name
------------
even 2 two

> CREATE MATERIALIZED VIEW test10 AS
  SELECT * FROM names, mods;

> PEEK test10;
num name num mod
----------------
1 one 0 even
1 one 1 odd
1 one 2 even
2 two 0 even
2 two 1 odd
2 two 2 even
3 three 0 even
3 three 1 odd
3 three 2 even

> CREATE MATERIALIZED VIEW test11 AS
  SELECT * FROM names CROSS JOIN mods;

> PEEK test11;
num name num mod
----------------
1 one 0 even
1 one 1 odd
1 one 2 even
2 two 0 even
2 two 1 odd
2 two 2 even
3 three 0 even
3 three 1 odd
3 three 2 even
