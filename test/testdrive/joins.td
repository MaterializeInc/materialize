# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

$ kafka-ingest format=avro topic=names schema=${names-schema} timestamp=2
{"before": null, "after": null}

$ kafka-ingest format=avro topic=mods schema=${mods-schema} timestamp=2
{"before": null, "after": null}

> CREATE SOURCE names_source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-names-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${names-schema}'
  ENVELOPE DEBEZIUM

> CREATE SOURCE mods_source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-mods-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${mods-schema}'
  ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW names AS SELECT * FROM names_source

> CREATE MATERIALIZED VIEW mods AS SELECT * FROM mods_source

> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM names JOIN mods USING (num);

> SELECT * FROM test1;
num name mod
------------
1 one odd
2 two even

> CREATE MATERIALIZED VIEW test2 AS
  SELECT * FROM names JOIN mods ON names.num = mods.num;

> SELECT * FROM test2;
num name num mod
----------------
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test3 AS
  SELECT * FROM names, mods WHERE names.num = mods.num;

> SELECT * FROM test3;
num name num mod
----------------
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test4 AS
  SELECT * FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even';

> SELECT * FROM test4;
num name num mod
----------------
2 two 2 even

> CREATE MATERIALIZED VIEW test5 AS
  SELECT * FROM names LEFT JOIN mods ON names.num = mods.num;

> SELECT * FROM test5;
num name num mod
----------------
1 one 1 odd
2 two 2 even
3 three <null> <null>

> CREATE MATERIALIZED VIEW test6 AS
  SELECT * FROM names RIGHT JOIN mods ON names.num = mods.num;

> SELECT * FROM test6;
num name num mod
----------------
<null> <null> 0 even
1 one 1 odd
2 two 2 even

> CREATE MATERIALIZED VIEW test7 AS
  SELECT * FROM names, mods WHERE names.num = mods.num AND mods.mod = 'even';

> SELECT * FROM test7;
num name num mod
----------------
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

> CREATE MATERIALIZED VIEW test10 AS
  SELECT * FROM names, mods;

> SELECT * FROM test10;
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

> SELECT * FROM test11;
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

> CREATE MATERIALIZED VIEW test12 AS
  SELECT * FROM names LEFT JOIN mods ON 1 = 0;

> SELECT * FROM test12;
num name num mod
----------------
1 one <null> <null>
2 two <null> <null>
3 three <null> <null>

> CREATE MATERIALIZED VIEW test13 AS
  SELECT * FROM names RIGHT JOIN mods ON 1 = 0;

> SELECT * FROM test13;
num name num mod
----------------
<null> <null> 0 even
<null> <null> 1 odd
<null> <null> 2 even

> CREATE MATERIALIZED VIEW test14 AS
  SELECT * FROM names FULL OUTER JOIN mods ON 1 = 0;

> SELECT * FROM test14;
num name num mod
----------------
1 one <null> <null>
2 two <null> <null>
3 three <null> <null>
<null> <null> 0 even
<null> <null> 1 odd
<null> <null> 2 even
