# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# Test that a dependency chain with multiple links is properly maintained
# across creation and deletion.

$ set schema={
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
              {"name": "X", "type": "long"},
              {"name": "Y", "type": "string"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

> CREATE SOURCE s FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}' USING SCHEMA '${schema}'

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=33
{"before": null, "after": {"X": 1, "Y": "mouse"}}
{"before": null, "after": {"X": 2, "Y": "keyboard"}}
{"before": null, "after": {"X": 3, "Y": "monitor"}}
{"before": null, "after": {"X": 1, "Y": "keyboard"}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=34
{"before": null, "after": null}

> CREATE MATERIALIZED VIEW test1 AS SELECT 1;

> CREATE MATERIALIZED VIEW test2 AS SELECT * FROM test1;

> CREATE MATERIALIZED VIEW test3a AS SELECT * FROM test2;

> CREATE MATERIALIZED VIEW test3b AS SELECT * FROM test2;

! DROP VIEW test1;
cannot delete test1: still depended upon by catalog item 'test2'

! DROP VIEW test2;
cannot delete test2: still depended upon by catalog item 'test3a'

> DROP VIEW test3a;

! DROP VIEW test1;
cannot delete test1: still depended upon by catalog item 'test2'

! DROP VIEW test2;
cannot delete test2: still depended upon by catalog item 'test3b'

> DROP VIEW test3b;

! DROP VIEW test1;
cannot delete test1: still depended upon by catalog item 'test2'

> DROP VIEW test2;

> DROP VIEW test1;

# Test that CASCADE causes all dependent views to be dropped along with the
# named view.

> CREATE MATERIALIZED VIEW test1 AS SELECT 1;

> CREATE MATERIALIZED VIEW test2 AS SELECT * FROM test1;

> DROP VIEW test1 CASCADE;

# TODO(benesch): when we have a system stream that has the names of all
# available views, we can verify there that views are actually dropped,
# rather than verifying the drop by checking whether DROP VIEW fails.

! DROP VIEW test1;
catalog item 'test1' does not exist

! DROP VIEW test2;
catalog item 'test2' does not exist

# Test that DROP VIEW IF EXISTS succeeds even if the view does not exist.

> DROP VIEW IF EXISTS test1;

# Test that CASCADE causes all dependent views to be dropped along with the
# named source.

> CREATE VIEW test4 as SELECT * FROM s;

> DROP SOURCE s CASCADE;

! DROP VIEW test4;
catalog item 'test4' does not exist

# Test that sinks cannot be depended upon.

> CREATE MATERIALIZED VIEW v1 AS SELECT 1;

> CREATE SINK s1 FROM v1 INTO 'kafka://${testdrive.kafka-addr}/v'

! CREATE MATERIALIZED VIEW v2 AS SELECT * FROM s1;
catalog item s1 is a sink and cannot be depended upon

# Test that dependent indexes prevent view deletion when restrict is specified

> CREATE MATERIALIZED VIEW v2 AS SELECT 1 as a;

> CREATE INDEX i1 ON v2(a);

! DROP VIEW v2;
cannot delete v2: still depended upon by catalog item 'i1'

# Test that dependent indexes prevent source deletion when restrict is specified

> CREATE SOURCE s FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}' USING SCHEMA '${schema}'

> CREATE INDEX i2 ON s("X");

! DROP SOURCE s;
cannot delete s: still depended upon by catalog item 'i2'

# Test that indexes cannot be depended upon

! CREATE MATERIALIZED VIEW v3 as SELECT * FROM i1;
catalog item i1 is an index and cannot be depended upon

# Test cascade delete associated indexes
> DROP VIEW v2 CASCADE;

! DROP VIEW v2;
catalog item 'v2' does not exist

! DROP INDEX i1;
catalog item 'i1' does not exist

> DROP SOURCE s CASCADE;

! DROP SOURCE s;
catalog item 's' does not exist

! DROP INDEX i2;
catalog item 'i2' does not exist
