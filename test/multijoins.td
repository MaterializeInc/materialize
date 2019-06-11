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

$ set plurals-schema={
    "type": "record",
    "name": "na",
    "fields": [
        {"name": "num", "type": "string"},
        {"name": "noun", "type": "string"}
    ]
  }

$ kafka-ingest format=avro topic=plurals schema=${plurals-schema}
{"num": "one", "noun": "sheep"}
{"num": "two", "noun": "sheep"}
{"num": "one", "noun": "mouse"}
{"num": "two", "noun": "meeses"}

> CREATE SOURCE plurals FROM 'kafka://${testdrive.kafka-addr}/plurals' USING SCHEMA '${plurals-schema}'

> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM names, mods, plurals WHERE names.num = mods.num AND names.name = plurals.num;

> PEEK test1;
num name num mod num plurals
----------------------------
1 one 1 odd one sheep
1 one 1 odd one mouse
2 two 2 even two sheep
2 two 2 even two meeses

> CREATE MATERIALIZED VIEW test2 AS
  SELECT * FROM names, mods, plurals WHERE names.num = mods.num AND names.name = plurals.num AND plurals.num = 'one';

> PEEK test2;
num name num mod num plurals
----------------------------
1 one 1 odd one sheep
1 one 1 odd one mouse

> CREATE MATERIALIZED VIEW test3 AS
  SELECT * FROM names RIGHT JOIN mods ON names.num = mods.num LEFT JOIN plurals ON names.name = plurals.num;

> PEEK test3;
num name num mod num plurals
----------------------------
<null> <null> 0 even <null> <null>
1 one 1 odd one sheep
1 one 1 odd one mouse
2 two 2 even two sheep
2 two 2 even two meeses

> CREATE MATERIALIZED VIEW test4 AS
  SELECT * FROM names, mods, plurals as foo WHERE names.num = mods.num AND names.name = foo.num AND foo.num = 'one';

> PEEK test4;
num name num mod num plurals
----------------------------
1 one 1 odd one sheep
1 one 1 odd one mouse
