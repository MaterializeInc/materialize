# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

$ set names-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                {
                    "name": "row",
                    "type": "record",
                    "fields": [
                        {"name": "num", "type": "long"},
                        {"name": "name", "type": "string"}
                    ]
                },
                "null"
            ]
        },
        { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=names schema=${names-schema} timestamp=1
{"before": null, "after": {"num": 1, "name": "one"}}
{"before": null, "after": {"num": 2, "name": "two"}}
{"before": null, "after": {"num": 3, "name": "three"}}

$ set mods-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                {
                    "name": "row",
                    "type": "record",
                    "fields": [
                        {"name": "num", "type": "long"},
                        {"name": "mod", "type": "string"}
                    ]
                },
                "null"
            ]
        },
        { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=mods schema=${mods-schema} timestamp=1
{"before": null, "after": {"num": 0, "mod": "even"}}
{"before": null, "after": {"num": 1, "mod": "odd"}}
{"before": null, "after": {"num": 2, "mod": "even"}}

$ set plurals-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                {
                    "name": "row",
                    "type": "record",
                    "fields": [
                        {"name": "num", "type": "string"},
                        {"name": "noun", "type": "string"}
                    ]
                },
                "null"
            ]
        },
        { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=plurals schema=${plurals-schema} timestamp=1
{"before": null, "after": {"num": "one", "noun": "sheep"}}
{"before": null, "after": {"num": "two", "noun": "sheep"}}
{"before": null, "after": {"num": "one", "noun": "mouse"}}
{"before": null, "after": {"num": "two", "noun": "meeses"}}

$ kafka-ingest format=avro topic=names schema=${names-schema} timestamp=2
{"before": null, "after": null}

$ kafka-ingest format=avro topic=mods schema=${mods-schema} timestamp=2
{"before": null, "after": null}

$ kafka-ingest format=avro topic=plurals schema=${plurals-schema} timestamp=2
{"before": null, "after": null}

> CREATE SOURCE names FROM 'kafka://${testdrive.kafka-addr}/testdrive-names-${testdrive.seed}'
  USING SCHEMA '${names-schema}'

> CREATE SOURCE mods FROM 'kafka://${testdrive.kafka-addr}/testdrive-mods-${testdrive.seed}'
  USING SCHEMA '${mods-schema}'

> CREATE SOURCE plurals FROM 'kafka://${testdrive.kafka-addr}/testdrive-plurals-${testdrive.seed}'
  USING SCHEMA '${plurals-schema}'

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
  SELECT * FROM names, mods, plurals
  WHERE names.num = mods.num AND names.name = plurals.num AND plurals.num = 'one';

> PEEK test2;
num name num mod num plurals
----------------------------
1 one 1 odd one sheep
1 one 1 odd one mouse

> CREATE MATERIALIZED VIEW test3 AS
  SELECT * FROM names
  RIGHT JOIN mods ON names.num = mods.num
  LEFT JOIN plurals ON names.name = plurals.num;

> PEEK test3;
num name num mod num plurals
----------------------------
<null> <null> 0 even <null> <null>
1 one 1 odd one sheep
1 one 1 odd one mouse
2 two 2 even two sheep
2 two 2 even two meeses

> CREATE MATERIALIZED VIEW test4 AS
  SELECT * FROM names, mods, plurals as foo
  WHERE names.num = mods.num AND names.name = foo.num AND foo.num = 'one';

> PEEK test4;
num name num mod num plurals
----------------------------
1 one 1 odd one sheep
1 one 1 odd one mouse
